package producers

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/withqb/coddy/roomserver/acls"
	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/setup/jetstream"
)

var keyContentFields = map[string]string{
	"m.room.join_rules":         "join_rule",
	"m.room.history_visibility": "history_visibility",
	"m.room.member":             "membership",
}

type RoomEventProducer struct {
	Topic     string
	ACLs      *acls.ServerACLs
	JetStream nats.JetStreamContext
}

func (r *RoomEventProducer) ProduceRoomEvents(roomID string, updates []api.OutputEvent) error {
	var err error
	for _, update := range updates {
		msg := nats.NewMsg(r.Topic)
		msg.Header.Set(jetstream.RoomEventType, string(update.Type))
		msg.Header.Set(jetstream.RoomID, roomID)
		msg.Data, err = json.Marshal(update)
		if err != nil {
			return err
		}
		logger := log.WithFields(log.Fields{
			"room_id": roomID,
			"type":    update.Type,
		})
		if update.NewRoomEvent != nil {
			eventType := update.NewRoomEvent.Event.Type()
			logger = logger.WithFields(log.Fields{
				"event_type":     eventType,
				"event_id":       update.NewRoomEvent.Event.EventID(),
				"adds_state":     len(update.NewRoomEvent.AddsStateEventIDs),
				"removes_state":  len(update.NewRoomEvent.RemovesStateEventIDs),
				"send_as_server": update.NewRoomEvent.SendAsServer,
				"sender":         update.NewRoomEvent.Event.SenderID(),
			})
			if update.NewRoomEvent.Event.StateKey() != nil {
				logger = logger.WithField("state_key", *update.NewRoomEvent.Event.StateKey())
			}
			contentKey := keyContentFields[eventType]
			if contentKey != "" {
				value := gjson.GetBytes(update.NewRoomEvent.Event.Content(), contentKey)
				if value.Exists() {
					logger = logger.WithField("content_value", value.String())
				}
			}

			if eventType == "m.room.server_acl" && update.NewRoomEvent.Event.StateKeyEquals("") {
				ev := update.NewRoomEvent.Event.PDU
				defer r.ACLs.OnServerACLUpdate(ev)
			}
		}
		logger.Tracef("Producing to topic '%s'", r.Topic)
		if _, err := r.JetStream.PublishMsg(msg); err != nil {
			logger.WithError(err).Errorf("Failed to produce to topic '%s': %s", r.Topic, err)
			return err
		}
	}
	return nil
}