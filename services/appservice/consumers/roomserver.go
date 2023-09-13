// Copyright 2018 Vector Creations Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"

	log "github.com/sirupsen/logrus"
)

// ApplicationServiceTransaction is the transaction that is sent off to an
// application service.
type ApplicationServiceTransaction struct {
	Events []synctypes.ClientEvent `json:"events"`
}

// OutputFrameEventConsumer consumes events that originated in the frame server.
type OutputFrameEventConsumer struct {
	ctx       context.Context
	cfg       *config.AppServiceAPI
	jetstream nats.JetStreamContext
	topic     string
	rsAPI     api.AppserviceDataframeAPI
}

type appserviceState struct {
	*config.ApplicationService
	backoff int
}

// NewOutputFrameEventConsumer creates a new OutputFrameEventConsumer. Call
// Start() to begin consuming from frame servers.
func NewOutputFrameEventConsumer(
	process *process.ProcessContext,
	cfg *config.AppServiceAPI,
	js nats.JetStreamContext,
	rsAPI api.AppserviceDataframeAPI,
) *OutputFrameEventConsumer {
	return &OutputFrameEventConsumer{
		ctx:       process.Context(),
		cfg:       cfg,
		jetstream: js,
		topic:     cfg.Matrix.JetStream.Prefixed(jetstream.OutputFrameEvent),
		rsAPI:     rsAPI,
	}
}

// Start consuming from frame servers
func (s *OutputFrameEventConsumer) Start() error {
	for _, as := range s.cfg.Derived.ApplicationServices {
		appsvc := as
		state := &appserviceState{
			ApplicationService: &appsvc,
		}
		token := jetstream.Tokenise(as.ID)
		if err := jetstream.JetStreamConsumer(
			s.ctx, s.jetstream, s.topic,
			s.cfg.Matrix.JetStream.Durable("Appservice_"+token),
			50, // maximum number of events to send in a single transaction
			func(ctx context.Context, msgs []*nats.Msg) bool {
				return s.onMessage(ctx, state, msgs)
			},
			nats.DeliverNew(), nats.ManualAck(),
		); err != nil {
			return fmt.Errorf("failed to create %q consumer: %w", token, err)
		}
	}
	return nil
}

// onMessage is called when the appservice component receives a new event from
// the frame server output log.
func (s *OutputFrameEventConsumer) onMessage(
	ctx context.Context, state *appserviceState, msgs []*nats.Msg,
) bool {
	log.WithField("appservice", state.ID).Tracef("Appservice worker received %d message(s) from dataframe", len(msgs))
	events := make([]*types.HeaderedEvent, 0, len(msgs))
	for _, msg := range msgs {
		// Only handle events we care about
		receivedType := api.OutputType(msg.Header.Get(jetstream.FrameEventType))
		if receivedType != api.OutputTypeNewFrameEvent && receivedType != api.OutputTypeNewInviteEvent {
			continue
		}
		// Parse out the event JSON
		var output api.OutputEvent
		if err := json.Unmarshal(msg.Data, &output); err != nil {
			// If the message was invalid, log it and move on to the next message in the stream
			log.WithField("appservice", state.ID).WithError(err).Errorf("Appservice failed to parse message, ignoring")
			continue
		}
		switch output.Type {
		case api.OutputTypeNewFrameEvent:
			if output.NewFrameEvent == nil || !s.appserviceIsInterestedInEvent(ctx, output.NewFrameEvent.Event, state.ApplicationService) {
				continue
			}
			events = append(events, output.NewFrameEvent.Event)
			if len(output.NewFrameEvent.AddsStateEventIDs) > 0 {
				newEventID := output.NewFrameEvent.Event.EventID()
				eventsReq := &api.QueryEventsByIDRequest{
					FrameID:   output.NewFrameEvent.Event.FrameID(),
					EventIDs: make([]string, 0, len(output.NewFrameEvent.AddsStateEventIDs)),
				}
				eventsRes := &api.QueryEventsByIDResponse{}
				for _, eventID := range output.NewFrameEvent.AddsStateEventIDs {
					if eventID != newEventID {
						eventsReq.EventIDs = append(eventsReq.EventIDs, eventID)
					}
				}
				if len(eventsReq.EventIDs) > 0 {
					if err := s.rsAPI.QueryEventsByID(s.ctx, eventsReq, eventsRes); err != nil {
						log.WithError(err).Errorf("s.rsAPI.QueryEventsByID failed")
						return false
					}
					events = append(events, eventsRes.Events...)
				}
			}

		default:
			continue
		}
	}

	// If there are no events selected for sending then we should
	// ack the messages so that we don't get sent them again in the
	// future.
	if len(events) == 0 {
		return true
	}

	txnID := ""
	// Try to get the message metadata, if we're able to, use the timestamp as the txnID
	metadata, err := msgs[0].Metadata()
	if err == nil {
		txnID = strconv.Itoa(int(metadata.Timestamp.UnixNano()))
	}

	// Send event to any relevant application services. If we hit
	// an error here, return false, so that we negatively ack.
	log.WithField("appservice", state.ID).Debugf("Appservice worker sending %d events(s) from dataframe", len(events))
	return s.sendEvents(ctx, state, events, txnID) == nil
}

// sendEvents passes events to the appservice by using the transactions
// endpoint. It will block for the backoff period if necessary.
func (s *OutputFrameEventConsumer) sendEvents(
	ctx context.Context, state *appserviceState,
	events []*types.HeaderedEvent,
	txnID string,
) error {
	// Create the transaction body.
	transaction, err := json.Marshal(
		ApplicationServiceTransaction{
			Events: synctypes.ToClientEvents(xtools.ToPDUs(events), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return s.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
			}),
		},
	)
	if err != nil {
		return err
	}

	// If txnID is not defined, generate one from the events.
	if txnID == "" {
		txnID = fmt.Sprintf("%d_%d", events[0].PDU.OriginServerTS(), len(transaction))
	}

	// Send the transaction to the appservice.
	address := fmt.Sprintf("%s/transactions/%s?access_token=%s", state.RequestUrl(), txnID, url.QueryEscape(state.HSToken))
	req, err := http.NewRequestWithContext(ctx, "PUT", address, bytes.NewBuffer(transaction))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := state.HTTPClient.Do(req)
	if err != nil {
		return state.backoffAndPause(err)
	}

	// If the response was fine then we can clear any backoffs in place and
	// report that everything was OK. Otherwise, back off for a while.
	switch resp.StatusCode {
	case http.StatusOK:
		state.backoff = 0
	default:
		return state.backoffAndPause(fmt.Errorf("received HTTP status code %d from appservice url %s", resp.StatusCode, address))
	}
	return nil
}

// backoff pauses the calling goroutine for a 2^some backoff exponent seconds
func (s *appserviceState) backoffAndPause(err error) error {
	if s.backoff < 6 {
		s.backoff++
	}
	duration := time.Second * time.Duration(math.Pow(2, float64(s.backoff)))
	log.WithField("appservice", s.ID).WithError(err).Errorf("Unable to send transaction to appservice, backing off for %s", duration.String())
	time.Sleep(duration)
	return err
}

// appserviceIsInterestedInEvent returns a boolean depending on whether a given
// event falls within one of a given application service's namespaces.
//
// TDO: This should be cached, see https://github.com/withqb/dendrite/issues/1682
func (s *OutputFrameEventConsumer) appserviceIsInterestedInEvent(ctx context.Context, event *types.HeaderedEvent, appservice *config.ApplicationService) bool {
	user := ""
	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return false
	}
	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
	if err == nil {
		user = userID.String()
	}

	switch {
	case appservice.URL == "":
		return false
	case appservice.IsInterestedInUserID(user):
		return true
	case appservice.IsInterestedInFrameID(event.FrameID()):
		return true
	}

	if event.Type() == spec.MFrameMember && event.StateKey() != nil {
		if appservice.IsInterestedInUserID(*event.StateKey()) {
			return true
		}
	}

	// Check all known frame aliases of the frame the event came from
	queryReq := api.GetAliasesForFrameIDRequest{FrameID: event.FrameID()}
	var queryRes api.GetAliasesForFrameIDResponse
	if err := s.rsAPI.GetAliasesForFrameID(ctx, &queryReq, &queryRes); err == nil {
		for _, alias := range queryRes.Aliases {
			if appservice.IsInterestedInFrameAlias(alias) {
				return true
			}
		}
	} else {
		log.WithFields(log.Fields{
			"appservice": appservice.ID,
			"frame_id":    event.FrameID(),
		}).WithError(err).Errorf("Unable to get aliases for frame")
	}

	// Check if any of the members in the frame match the appservice
	return s.appserviceJoinedAtEvent(ctx, event, appservice)
}

// appserviceJoinedAtEvent returns a boolean depending on whether a given
// appservice has membership at the time a given event was created.
func (s *OutputFrameEventConsumer) appserviceJoinedAtEvent(ctx context.Context, event *types.HeaderedEvent, appservice *config.ApplicationService) bool {
	// TDO: This is only checking the current frame state, not the state at
	// the event in question. Pretty sure this is what Synapse does too, but
	// until we have a lighter way of checking the state before the event that
	// doesn't involve state res, then this is probably OK.
	membershipReq := &api.QueryMembershipsForFrameRequest{
		FrameID:     event.FrameID(),
		JoinedOnly: true,
	}
	membershipRes := &api.QueryMembershipsForFrameResponse{}

	// XXX: This could potentially race if the state for the event is not known yet
	// e.g. the event came over federation but we do not have the full state persisted.
	if err := s.rsAPI.QueryMembershipsForFrame(ctx, membershipReq, membershipRes); err == nil {
		for _, ev := range membershipRes.JoinEvents {
			switch {
			case ev.StateKey == nil:
				continue
			case ev.Type != spec.MFrameMember:
				continue
			}
			var membership xtools.MemberContent
			err = json.Unmarshal(ev.Content, &membership)
			switch {
			case err != nil:
				continue
			case membership.Membership == spec.Join:
				if appservice.IsInterestedInUserID(*ev.StateKey) {
					return true
				}
			}
		}
	} else {
		log.WithFields(log.Fields{
			"appservice": appservice.ID,
			"frame_id":    event.FrameID(),
		}).WithError(err).Errorf("Unable to get membership for frame")
	}
	return false
}
