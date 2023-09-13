package producers

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/withqb/coddy/servers/dataframe/acls"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/jetstream"
)

var keyContentFields = map[string]string{
	"m.frame.join_rules":         "join_rule",
	"m.frame.history_visibility": "history_visibility",
	"m.frame.member":             "membership",
}

type FrameEventProducer struct {
	Topic     string
	ACLs      *acls.ServerACLs
	JetStream nats.JetStreamContext
}

func (r *FrameEventProducer) ProduceFrameEvents(frameID string, updates []api.OutputEvent) error {
	var err error
	for _, update := range updates {
		msg := nats.NewMsg(r.Topic)
		msg.Header.Set(jetstream.FrameEventType, string(update.Type))
		msg.Header.Set(jetstream.FrameID, frameID)
		msg.Data, err = json.Marshal(update)
		if err != nil {
			return err
		}
		logger := log.WithFields(log.Fields{
			"frame_id": frameID,
			"type":    update.Type,
		})
		if update.NewFrameEvent != nil {
			eventType := update.NewFrameEvent.Event.Type()
			logger = logger.WithFields(log.Fields{
				"event_type":     eventType,
				"event_id":       update.NewFrameEvent.Event.EventID(),
				"adds_state":     len(update.NewFrameEvent.AddsStateEventIDs),
				"removes_state":  len(update.NewFrameEvent.RemovesStateEventIDs),
				"send_as_server": update.NewFrameEvent.SendAsServer,
				"sender":         update.NewFrameEvent.Event.SenderID(),
			})
			if update.NewFrameEvent.Event.StateKey() != nil {
				logger = logger.WithField("state_key", *update.NewFrameEvent.Event.StateKey())
			}
			contentKey := keyContentFields[eventType]
			if contentKey != "" {
				value := gjson.GetBytes(update.NewFrameEvent.Event.Content(), contentKey)
				if value.Exists() {
					logger = logger.WithField("content_value", value.String())
				}
			}

			if eventType == "m.frame.server_acl" && update.NewFrameEvent.Event.StateKeyEquals("") {
				ev := update.NewFrameEvent.Event.PDU
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
