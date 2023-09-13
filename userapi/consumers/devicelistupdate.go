package consumers

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/userapi/internal"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// DeviceListUpdateConsumer consumes device list updates that came in over federation.
type DeviceListUpdateConsumer struct {
	ctx               context.Context
	jetstream         nats.JetStreamContext
	durable           string
	topic             string
	updater           *internal.DeviceListUpdater
	isLocalServerName func(spec.ServerName) bool
}

// NewDeviceListUpdateConsumer creates a new DeviceListConsumer. Call Start() to begin consuming from key servers.
func NewDeviceListUpdateConsumer(
	process *process.ProcessContext,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	updater *internal.DeviceListUpdater,
) *DeviceListUpdateConsumer {
	return &DeviceListUpdateConsumer{
		ctx:               process.Context(),
		jetstream:         js,
		durable:           cfg.Matrix.JetStream.Prefixed("KeyServerInputDeviceListConsumer"),
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.InputDeviceListUpdate),
		updater:           updater,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
	}
}

// Start consuming from key servers
func (t *DeviceListUpdateConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// key change events topic from the key server.
func (t *DeviceListUpdateConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m xtools.DeviceListUpdateEvent
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		logrus.WithError(err).Errorf("Failed to read from device list update input topic")
		return true
	}
	origin := spec.ServerName(msg.Header.Get("origin"))
	if _, serverName, err := xtools.SplitID('@', m.UserID); err != nil {
		return true
	} else if t.isLocalServerName(serverName) {
		return true
	} else if serverName != origin {
		return true
	}

	err := t.updater.Update(ctx, m)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"user_id":   m.UserID,
			"device_id": m.DeviceID,
			"stream_id": m.StreamID,
			"prev_id":   m.PrevID,
		}).WithError(err).Errorf("Failed to update device list")
		return false
	}
	return true
}
