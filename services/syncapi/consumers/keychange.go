package consumers

import (
	"context"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/notifier"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/streams"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// OutputKeyChangeEventConsumer consumes events that originated in the key server.
type OutputKeyChangeEventConsumer struct {
	ctx       context.Context
	jetstream nats.JetStreamContext
	durable   string
	topic     string
	db        storage.Database
	notifier  *notifier.Notifier
	stream    streams.StreamProvider
	rsAPI     dataframeAPI.SyncDataframeAPI
}

// NewOutputKeyChangeEventConsumer creates a new OutputKeyChangeEventConsumer.
// Call Start() to begin consuming from the key server.
func NewOutputKeyChangeEventConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	topic string,
	js nats.JetStreamContext,
	rsAPI dataframeAPI.SyncDataframeAPI,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputKeyChangeEventConsumer {
	s := &OutputKeyChangeEventConsumer{
		ctx:       process.Context(),
		jetstream: js,
		durable:   cfg.Coddy.JetStream.Durable("SyncAPIKeyChangeConsumer"),
		topic:     topic,
		db:        store,
		rsAPI:     rsAPI,
		notifier:  notifier,
		stream:    stream,
	}

	return s
}

// Start consuming from the key server
func (s *OutputKeyChangeEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputKeyChangeEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m api.DeviceMessage
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		logrus.WithError(err).Errorf("failed to read device message from key change topic")
		return true
	}
	if m.DeviceKeys == nil && m.OutputCrossSigningKeyUpdate == nil {
		// This probably shouldn't happen but stops us from panicking if we come
		// across an update that doesn't satisfy either types.
		return true
	}
	switch m.Type {
	case api.TypeCrossSigningUpdate:
		return s.onCrossSigningMessage(m, m.DeviceChangeID)
	case api.TypeDeviceKeyUpdate:
		fallthrough
	default:
		return s.onDeviceKeyMessage(m, m.DeviceChangeID)
	}
}

func (s *OutputKeyChangeEventConsumer) onDeviceKeyMessage(m api.DeviceMessage, deviceChangeID int64) bool {
	if m.DeviceKeys == nil {
		return true
	}
	output := m.DeviceKeys
	// work out who we need to notify about the new key
	var queryRes dataframeAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(s.ctx, &dataframeAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}

func (s *OutputKeyChangeEventConsumer) onCrossSigningMessage(m api.DeviceMessage, deviceChangeID int64) bool {
	output := m.CrossSigningKeyUpdate
	// work out who we need to notify about the new key
	var queryRes dataframeAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(s.ctx, &dataframeAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}
