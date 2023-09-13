package producers

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/userapi/api"
	"github.com/withqb/coddy/userapi/storage"
)

// KeyChange produces key change events for the sync API and federation sender to consume
type KeyChange struct {
	Topic     string
	JetStream JetStreamPublisher
	DB        storage.KeyChangeDatabase
}

// ProduceKeyChanges creates new change events for each key
func (p *KeyChange) ProduceKeyChanges(keys []api.DeviceMessage) error {
	userToDeviceCount := make(map[string]int)
	for _, key := range keys {
		id, err := p.DB.StoreKeyChange(context.Background(), key.UserID)
		if err != nil {
			return err
		}
		key.DeviceChangeID = id
		value, err := json.Marshal(key)
		if err != nil {
			return err
		}

		m := &nats.Msg{
			Subject: p.Topic,
			Header:  nats.Header{},
		}
		m.Header.Set(jetstream.UserID, key.UserID)
		m.Data = value

		_, err = p.JetStream.PublishMsg(m)
		if err != nil {
			return err
		}

		userToDeviceCount[key.UserID]++
	}
	for userID, count := range userToDeviceCount {
		logrus.WithFields(logrus.Fields{
			"user_id":         userID,
			"num_key_changes": count,
		}).Tracef("Produced to key change topic '%s'", p.Topic)
	}
	return nil
}

func (p *KeyChange) ProduceSigningKeyUpdate(key api.CrossSigningKeyUpdate) error {
	output := &api.DeviceMessage{
		Type: api.TypeCrossSigningUpdate,
		OutputCrossSigningKeyUpdate: &api.OutputCrossSigningKeyUpdate{
			CrossSigningKeyUpdate: key,
		},
	}

	id, err := p.DB.StoreKeyChange(context.Background(), key.UserID)
	if err != nil {
		return err
	}
	output.DeviceChangeID = id

	value, err := json.Marshal(output)
	if err != nil {
		return err
	}

	m := &nats.Msg{
		Subject: p.Topic,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, key.UserID)
	m.Data = value

	_, err = p.JetStream.PublishMsg(m)
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"user_id": key.UserID,
	}).Tracef("Produced to cross-signing update topic '%s'", p.Topic)
	return nil
}
