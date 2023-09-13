package consumers

import (
	"context"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/federationapi/queue"
	"github.com/withqb/coddy/services/federationapi/storage"
	syncTypes "github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// OutputSendToDeviceConsumer consumes events that originate in the clientapi.
type OutputSendToDeviceConsumer struct {
	ctx               context.Context
	jetstream         nats.JetStreamContext
	durable           string
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
	topic             string
}

// NewOutputSendToDeviceConsumer creates a new OutputSendToDeviceConsumer. Call Start() to begin consuming send-to-device events.
func NewOutputSendToDeviceConsumer(
	process *process.ProcessContext,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
) *OutputSendToDeviceConsumer {
	return &OutputSendToDeviceConsumer{
		ctx:               process.Context(),
		jetstream:         js,
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Coddy.IsLocalServerName,
		durable:           cfg.Coddy.JetStream.Durable("FederationAPIESendToDeviceConsumer"),
		topic:             cfg.Coddy.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
	}
}

// Start consuming from the client api
func (t *OutputSendToDeviceConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// send-to-device events topic from the client api.
func (t *OutputSendToDeviceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send send-to-device events which originated from us
	sender := msg.Header.Get("sender")
	_, originServerName, err := xtools.SplitID('@', sender)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).WithField("user_id", sender).Error("failed to extract domain from send-to-device sender")
		return true
	}
	if !t.isLocalServerName(originServerName) {
		return true
	}
	// Extract the send-to-device event from msg.
	var ote syncTypes.OutputSendToDeviceEvent
	if err = json.Unmarshal(msg.Data, &ote); err != nil {
		sentry.CaptureException(err)
		log.WithError(err).Errorf("output log: message parse failed (expected send-to-device)")
		return true
	}

	_, destServerName, err := xtools.SplitID('@', ote.UserID)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).WithField("user_id", ote.UserID).Error("failed to extract domain from send-to-device destination")
		return true
	}

	// The SyncAPI is already handling sendToDevice for the local server
	if t.isLocalServerName(destServerName) {
		return true
	}

	// Pack the EDU and marshal it
	edu := &xtools.EDU{
		Type:   spec.MDirectToDevice,
		Origin: string(originServerName),
	}
	tdm := xtools.ToDeviceMessage{
		Sender:    ote.Sender,
		Type:      ote.Type,
		MessageID: xutil.RandomString(32),
		Messages: map[string]map[string]json.RawMessage{
			ote.UserID: {
				ote.DeviceID: ote.Content,
			},
		},
	}
	if edu.Content, err = json.Marshal(tdm); err != nil {
		sentry.CaptureException(err)
		log.WithError(err).Error("failed to marshal EDU JSON")
		return true
	}

	log.Debugf("Sending send-to-device message into %q destination queue", destServerName)
	if err := t.queues.SendEDU(edu, originServerName, []spec.ServerName{destServerName}); err != nil {
		log.WithError(err).Error("failed to send EDU")
		return false
	}

	return true
}
