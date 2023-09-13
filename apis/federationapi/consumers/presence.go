package consumers

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/federationapi/queue"
	"github.com/withqb/coddy/apis/federationapi/storage"
	fedTypes "github.com/withqb/coddy/apis/federationapi/types"
	"github.com/withqb/coddy/apis/syncapi/types"
	roomserverAPI "github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// OutputReceiptConsumer consumes events that originate in the clientapi.
type OutputPresenceConsumer struct {
	ctx                     context.Context
	jetstream               nats.JetStreamContext
	durable                 string
	db                      storage.Database
	queues                  *queue.OutgoingQueues
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	topic                   string
	outboundPresenceEnabled bool
}

// NewOutputPresenceConsumer creates a new OutputPresenceConsumer. Call Start() to begin consuming events.
func NewOutputPresenceConsumer(
	process *process.ProcessContext,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) *OutputPresenceConsumer {
	return &OutputPresenceConsumer{
		ctx:                     process.Context(),
		jetstream:               js,
		queues:                  queues,
		db:                      store,
		isLocalServerName:       cfg.Matrix.IsLocalServerName,
		durable:                 cfg.Matrix.JetStream.Durable("FederationAPIPresenceConsumer"),
		topic:                   cfg.Matrix.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		outboundPresenceEnabled: cfg.Matrix.Presence.EnableOutbound,
		rsAPI:                   rsAPI,
	}
}

// Start consuming from the clientapi
func (t *OutputPresenceConsumer) Start() error {
	if !t.outboundPresenceEnabled {
		return nil
	}
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1, t.onMessage,
		nats.DeliverAll(), nats.ManualAck(), nats.HeadersOnly(),
	)
}

// onMessage is called in response to a message received on the presence
// events topic from the client api.
func (t *OutputPresenceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send presence events which originated from us
	userID := msg.Header.Get(jetstream.UserID)
	_, serverName, err := xtools.SplitID('@', userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("failed to extract domain from receipt sender")
		return true
	}
	if !t.isLocalServerName(serverName) {
		return true
	}

	parsedUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).WithField("user_id", userID).Error("invalid user ID")
		return true
	}

	roomIDs, err := t.rsAPI.QueryRoomsForUser(t.ctx, *parsedUserID, "join")
	if err != nil {
		log.WithError(err).Error("failed to calculate joined rooms for user")
		return true
	}

	roomIDStrs := make([]string, len(roomIDs))
	for i, roomID := range roomIDs {
		roomIDStrs[i] = roomID.String()
	}

	presence := msg.Header.Get("presence")

	ts, err := strconv.Atoi(msg.Header.Get("last_active_ts"))
	if err != nil {
		return true
	}

	// send this presence to all servers who share rooms with this user.
	joined, err := t.db.GetJoinedHostsForRooms(t.ctx, roomIDStrs, true, true)
	if err != nil {
		log.WithError(err).Error("failed to get joined hosts")
		return true
	}

	if len(joined) == 0 {
		return true
	}

	var statusMsg *string = nil
	if data, ok := msg.Header["status_msg"]; ok && len(data) > 0 {
		status := msg.Header.Get("status_msg")
		statusMsg = &status
	}

	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(ts)}

	content := fedTypes.Presence{
		Push: []fedTypes.PresenceContent{
			{
				CurrentlyActive: p.CurrentlyActive(),
				LastActiveAgo:   p.LastActiveAgo(),
				Presence:        presence,
				StatusMsg:       statusMsg,
				UserID:          userID,
			},
		},
	}

	edu := &xtools.EDU{
		Type:   spec.MPresence,
		Origin: string(serverName),
	}
	if edu.Content, err = json.Marshal(content); err != nil {
		log.WithError(err).Error("failed to marshal EDU JSON")
		return true
	}

	log.Tracef("sending presence EDU to %d servers", len(joined))
	if err = t.queues.SendEDU(edu, serverName, joined); err != nil {
		log.WithError(err).Error("failed to send EDU")
		return false
	}

	return true
}
