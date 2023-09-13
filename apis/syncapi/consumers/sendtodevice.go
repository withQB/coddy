package consumers

import (
	"context"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/syncapi/notifier"
	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/streams"
	"github.com/withqb/coddy/apis/syncapi/types"
	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// OutputSendToDeviceEventConsumer consumes events that originated in the EDU server.
type OutputSendToDeviceEventConsumer struct {
	ctx               context.Context
	jetstream         nats.JetStreamContext
	durable           string
	topic             string
	db                storage.Database
	userAPI           api.SyncKeyAPI
	isLocalServerName func(spec.ServerName) bool
	stream            streams.StreamProvider
	notifier          *notifier.Notifier
}

// NewOutputSendToDeviceEventConsumer creates a new OutputSendToDeviceEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputSendToDeviceEventConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	store storage.Database,
	userAPI api.SyncKeyAPI,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputSendToDeviceEventConsumer {
	return &OutputSendToDeviceEventConsumer{
		ctx:               process.Context(),
		jetstream:         js,
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
		durable:           cfg.Matrix.JetStream.Durable("SyncAPISendToDeviceConsumer"),
		db:                store,
		userAPI:           userAPI,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
		notifier:          notifier,
		stream:            stream,
	}
}

// Start consuming send-to-device events.
func (s *OutputSendToDeviceEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputSendToDeviceEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	userID := msg.Header.Get(jetstream.UserID)
	_, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).Errorf("send-to-device: failed to split user id, dropping message")
		return true
	}
	if !s.isLocalServerName(domain) {
		log.Tracef("ignoring send-to-device event with destination %s", domain)
		return true
	}

	var output types.OutputSendToDeviceEvent
	if err = json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("send-to-device: message parse failure")
		sentry.CaptureException(err)
		return true
	}

	logger := xutil.GetLogger(context.TODO()).WithFields(log.Fields{
		"sender":     output.Sender,
		"user_id":    output.UserID,
		"device_id":  output.DeviceID,
		"event_type": output.Type,
	})
	logger.Debugf("sync API received send-to-device event from the clientapi/federationsender")

	// Check we actually got the requesting device in our store, if we receive a frame key request
	if output.Type == "m.frame_key_request" {
		requestingDeviceID := gjson.GetBytes(output.SendToDeviceEvent.Content, "requesting_device_id").Str
		_, senderDomain, _ := xtools.SplitID('@', output.Sender)
		if requestingDeviceID != "" && !s.isLocalServerName(senderDomain) {
			// Mark the requesting device as stale, if we don't know about it.
			if err = s.userAPI.PerformMarkAsStaleIfNeeded(ctx, &api.PerformMarkAsStaleRequest{
				UserID: output.Sender, Domain: senderDomain, DeviceID: requestingDeviceID,
			}, &struct{}{}); err != nil {
				logger.WithError(err).Errorf("failed to mark as stale if needed")
				return false
			}
		}
	}

	streamPos, err := s.db.StoreNewSendForDeviceMessage(
		s.ctx, output.UserID, output.DeviceID, output.SendToDeviceEvent,
	)
	if err != nil {
		sentry.CaptureException(err)
		log.WithError(err).Errorf("send-to-device: failed to store message")
		return false
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewSendToDevice(
		output.UserID,
		[]string{output.DeviceID},
		types.StreamingToken{SendToDevicePosition: streamPos},
	)

	return true
}
