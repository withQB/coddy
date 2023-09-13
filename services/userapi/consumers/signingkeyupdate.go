package consumers

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// SigningKeyUpdateConsumer consumes signing key updates that came in over federation.
type SigningKeyUpdateConsumer struct {
	ctx               context.Context
	jetstream         nats.JetStreamContext
	durable           string
	topic             string
	userAPI           api.UploadDeviceKeysAPI
	cfg               *config.UserAPI
	isLocalServerName func(spec.ServerName) bool
}

// NewSigningKeyUpdateConsumer creates a new SigningKeyUpdateConsumer. Call Start() to begin consuming from key servers.
func NewSigningKeyUpdateConsumer(
	process *process.ProcessContext,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	userAPI api.UploadDeviceKeysAPI,
) *SigningKeyUpdateConsumer {
	return &SigningKeyUpdateConsumer{
		ctx:               process.Context(),
		jetstream:         js,
		durable:           cfg.Coddy.JetStream.Prefixed("KeyServerSigningKeyConsumer"),
		topic:             cfg.Coddy.JetStream.Prefixed(jetstream.InputSigningKeyUpdate),
		userAPI:           userAPI,
		cfg:               cfg,
		isLocalServerName: cfg.Coddy.IsLocalServerName,
	}
}

// Start consuming from key servers
func (t *SigningKeyUpdateConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// signing key update events topic from the key server.
func (t *SigningKeyUpdateConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var updatePayload api.CrossSigningKeyUpdate
	if err := json.Unmarshal(msg.Data, &updatePayload); err != nil {
		logrus.WithError(err).Errorf("failed to read from signing key update input topic")
		return true
	}
	origin := spec.ServerName(msg.Header.Get("origin"))
	if _, serverName, err := xtools.SplitID('@', updatePayload.UserID); err != nil {
		logrus.WithError(err).Error("failed to split user id")
		return true
	} else if t.isLocalServerName(serverName) {
		logrus.Warn("dropping device key update from ourself")
		return true
	} else if serverName != origin {
		logrus.Warnf("dropping device key update, %s != %s", serverName, origin)
		return true
	}

	keys := fclient.CrossSigningKeys{}
	if updatePayload.MasterKey != nil {
		keys.MasterKey = *updatePayload.MasterKey
	}
	if updatePayload.SelfSigningKey != nil {
		keys.SelfSigningKey = *updatePayload.SelfSigningKey
	}
	uploadReq := &api.PerformUploadDeviceKeysRequest{
		CrossSigningKeys: keys,
		UserID:           updatePayload.UserID,
	}
	uploadRes := &api.PerformUploadDeviceKeysResponse{}
	t.userAPI.PerformUploadDeviceKeys(ctx, uploadReq, uploadRes)
	if uploadRes.Error != nil {
		logrus.WithError(uploadRes.Error).Error("failed to upload device keys")
		return true
	}

	return true
}
