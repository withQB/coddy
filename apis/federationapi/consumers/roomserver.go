package consumers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	syncAPITypes "github.com/withqb/coddy/apis/syncapi/types"
	"github.com/withqb/xtools/spec"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"

	"github.com/withqb/coddy/apis/federationapi/queue"
	"github.com/withqb/coddy/apis/federationapi/storage"
	"github.com/withqb/coddy/apis/federationapi/types"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// OutputFrameEventConsumer consumes events that originated in the frame server.
type OutputFrameEventConsumer struct {
	ctx           context.Context
	cfg           *config.FederationAPI
	rsAPI         api.FederationDataframeAPI
	jetstream     nats.JetStreamContext
	natsClient    *nats.Conn
	durable       string
	db            storage.Database
	queues        *queue.OutgoingQueues
	topic         string
	topicPresence string
}

// NewOutputFrameEventConsumer creates a new OutputFrameEventConsumer. Call Start() to begin consuming from frame servers.
func NewOutputFrameEventConsumer(
	process *process.ProcessContext,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	natsClient *nats.Conn,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI api.FederationDataframeAPI,
) *OutputFrameEventConsumer {
	return &OutputFrameEventConsumer{
		ctx:           process.Context(),
		cfg:           cfg,
		jetstream:     js,
		natsClient:    natsClient,
		db:            store,
		queues:        queues,
		rsAPI:         rsAPI,
		durable:       cfg.Matrix.JetStream.Durable("FederationAPIDataFrameConsumer"),
		topic:         cfg.Matrix.JetStream.Prefixed(jetstream.OutputFrameEvent),
		topicPresence: cfg.Matrix.JetStream.Prefixed(jetstream.RequestPresence),
	}
}

// Start consuming from frame servers
func (s *OutputFrameEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called when the federation server receives a new event from the frame server output log.
// It is unsafe to call this with messages for the same frame in multiple gorountines
// because updates it will likely fail with a types.EventIDMismatchError when it
// realises that it cannot update the frame state using the deltas.
func (s *OutputFrameEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	receivedType := api.OutputType(msg.Header.Get(jetstream.FrameEventType))

	// Only handle events we care about, avoids unneeded unmarshalling
	switch receivedType {
	case api.OutputTypeNewFrameEvent, api.OutputTypeNewInboundPeek, api.OutputTypePurgeFrame:
	default:
		return true
	}

	// Parse out the event JSON
	var output api.OutputEvent
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("dataframe output log: message parse failure")
		return true
	}

	switch output.Type {
	case api.OutputTypeNewFrameEvent:
		ev := output.NewFrameEvent.Event
		if err := s.processMessage(*output.NewFrameEvent, output.NewFrameEvent.RewritesState); err != nil {
			// panic rather than continue with an inconsistent database
			log.WithFields(log.Fields{
				"event_id":   ev.EventID(),
				"event":      string(ev.JSON()),
				"add":        output.NewFrameEvent.AddsStateEventIDs,
				"del":        output.NewFrameEvent.RemovesStateEventIDs,
				log.ErrorKey: err,
			}).Panicf("dataframe output log: write frame event failure")
		}

	case api.OutputTypeNewInboundPeek:
		if err := s.processInboundPeek(*output.NewInboundPeek); err != nil {
			log.WithFields(log.Fields{
				"event":      output.NewInboundPeek,
				log.ErrorKey: err,
			}).Panicf("dataframe output log: remote peek event failure")
			return false
		}

	case api.OutputTypePurgeFrame:
		log.WithField("frame_id", output.PurgeFrame.FrameID).Warn("Purging frame from federation API")
		if err := s.db.PurgeFrame(ctx, output.PurgeFrame.FrameID); err != nil {
			logrus.WithField("frame_id", output.PurgeFrame.FrameID).WithError(err).Error("failed to purge frame from federation API")
		} else {
			logrus.WithField("frame_id", output.PurgeFrame.FrameID).Warn("Frame purged from federation API")
		}

	default:
		log.WithField("type", output.Type).Debug(
			"dataframe output log: ignoring unknown output type",
		)
	}

	return true
}

// processInboundPeek starts tracking a new federated inbound peek (replacing the existing one if any)
// causing the federationapi to start sending messages to the peeking server
func (s *OutputFrameEventConsumer) processInboundPeek(orp api.OutputNewInboundPeek) error {

	// FXME: there's a race here - we should start /sending new peeked events
	// atomically after the orp.LatestEventID to ensure there are no gaps between
	// the peek beginning and the send stream beginning.
	//
	// We probably need to track orp.LatestEventID on the inbound peek, but it's
	// unclear how we then use that to prevent the race when we start the send
	// stream.
	//
	// This is making the tests flakey.

	return s.db.AddInboundPeek(s.ctx, orp.ServerName, orp.FrameID, orp.PeekID, orp.RenewalInterval)
}

// processMessage updates the list of currently joined hosts in the frame
// and then sends the event to the hosts that were joined before the event.
func (s *OutputFrameEventConsumer) processMessage(ore api.OutputNewFrameEvent, rewritesState bool) error {

	addsStateEvents, missingEventIDs := ore.NeededStateEventIDs()

	// Ask the dataframe and add in the rest of the results into the set.
	// Finally, work out if there are any more events missing.
	if len(missingEventIDs) > 0 {
		eventsReq := &api.QueryEventsByIDRequest{
			FrameID:   ore.Event.FrameID(),
			EventIDs: missingEventIDs,
		}
		eventsRes := &api.QueryEventsByIDResponse{}
		if err := s.rsAPI.QueryEventsByID(s.ctx, eventsReq, eventsRes); err != nil {
			return fmt.Errorf("s.rsAPI.QueryEventsByID: %w", err)
		}
		if len(eventsRes.Events) != len(missingEventIDs) {
			return fmt.Errorf("missing state events")
		}
		addsStateEvents = append(addsStateEvents, eventsRes.Events...)
	}

	evs := make([]xtools.PDU, len(addsStateEvents))
	for i := range evs {
		evs[i] = addsStateEvents[i].PDU
	}

	addsJoinedHosts, err := JoinedHostsFromEvents(s.ctx, evs, s.rsAPI)
	if err != nil {
		return err
	}
	// Update our copy of the current state.
	// We keep a copy of the current state because the state at each event is
	// expressed as a delta against the current state.
	// TODO(#290): handle EventIDMismatchError and recover the current state by
	// talking to the dataframe
	oldJoinedHosts, err := s.db.UpdateFrame(
		s.ctx,
		ore.Event.FrameID(),
		addsJoinedHosts,
		ore.RemovesStateEventIDs,
		rewritesState, // if we're re-writing state, nuke all joined hosts before adding
	)
	if err != nil {
		return err
	}

	// If we added new hosts, inform them about our known presence events for this frame
	if s.cfg.Matrix.Presence.EnableOutbound && len(addsJoinedHosts) > 0 && ore.Event.Type() == spec.MFrameMember && ore.Event.StateKey() != nil {
		membership, _ := ore.Event.Membership()
		if membership == spec.Join {
			s.sendPresence(ore.Event.FrameID(), addsJoinedHosts)
		}
	}

	if oldJoinedHosts == nil {
		// This means that there is nothing to update as this is a duplicate
		// message.
		// This can happen if dendrite crashed between reading the message and
		// persisting the stream position.
		return nil
	}

	if ore.SendAsServer == api.DoNotSendToOtherServers {
		// Ignore event that we don't need to send anywhere.
		return nil
	}

	// Work out which hosts were joined at the event itself.
	joinedHostsAtEvent, err := s.joinedHostsAtEvent(ore, oldJoinedHosts)
	if err != nil {
		return err
	}

	// TDO: do housekeeping to evict unrenewed peeking hosts

	// TDO: implement query to let the fedapi check whether a given peek is live or not

	// Send the event.
	return s.queues.SendEvent(
		ore.Event, spec.ServerName(ore.SendAsServer), joinedHostsAtEvent,
	)
}

func (s *OutputFrameEventConsumer) sendPresence(frameID string, addedJoined []types.JoinedHost) {
	joined := make([]spec.ServerName, 0, len(addedJoined))
	for _, added := range addedJoined {
		joined = append(joined, added.ServerName)
	}

	// get our locally joined users
	var queryRes api.QueryMembershipsForFrameResponse
	err := s.rsAPI.QueryMembershipsForFrame(s.ctx, &api.QueryMembershipsForFrameRequest{
		JoinedOnly: true,
		LocalOnly:  true,
		FrameID:     frameID,
	}, &queryRes)
	if err != nil {
		log.WithError(err).Error("failed to calculate joined frames for user")
		return
	}

	// send every presence we know about to the remote server
	content := types.Presence{}
	for _, ev := range queryRes.JoinEvents {
		msg := nats.NewMsg(s.topicPresence)
		msg.Header.Set(jetstream.UserID, ev.Sender)

		var presence *nats.Msg
		presence, err = s.natsClient.RequestMsg(msg, time.Second*10)
		if err != nil {
			log.WithError(err).Errorf("unable to get presence")
			continue
		}

		statusMsg := presence.Header.Get("status_msg")
		e := presence.Header.Get("error")
		if e != "" {
			continue
		}
		var lastActive int
		lastActive, err = strconv.Atoi(presence.Header.Get("last_active_ts"))
		if err != nil {
			continue
		}

		p := syncAPITypes.PresenceInternal{LastActiveTS: spec.Timestamp(lastActive)}

		content.Push = append(content.Push, types.PresenceContent{
			CurrentlyActive: p.CurrentlyActive(),
			LastActiveAgo:   p.LastActiveAgo(),
			Presence:        presence.Header.Get("presence"),
			StatusMsg:       &statusMsg,
			UserID:          ev.Sender,
		})
	}

	if len(content.Push) == 0 {
		return
	}

	edu := &xtools.EDU{
		Type:   spec.MPresence,
		Origin: string(s.cfg.Matrix.ServerName),
	}
	if edu.Content, err = json.Marshal(content); err != nil {
		log.WithError(err).Error("failed to marshal EDU JSON")
		return
	}
	if err := s.queues.SendEDU(edu, s.cfg.Matrix.ServerName, joined); err != nil {
		log.WithError(err).Error("failed to send EDU")
	}
}

// joinedHostsAtEvent works out a list of coddy servers that were joined to
// the frame at the event (including peeking ones)
// It is important to use the state at the event for sending messages because:
//
//  1. We shouldn't send messages to servers that weren't in the frame.
//  2. If a server is kicked from the frames it should still be told about the
//     kick event.
//
// Usually the list can be calculated locally, but sometimes it will need fetch
// events from the frame server.
// Returns an error if there was a problem talking to the frame server.
func (s *OutputFrameEventConsumer) joinedHostsAtEvent(
	ore api.OutputNewFrameEvent, oldJoinedHosts []types.JoinedHost,
) ([]spec.ServerName, error) {
	// Combine the delta into a single delta so that the adds and removes can
	// cancel each other out. This should reduce the number of times we need
	// to fetch a state event from the frame server.
	combinedAdds, combinedRemoves := combineDeltas(
		ore.AddsStateEventIDs, ore.RemovesStateEventIDs,
		ore.StateBeforeAddsEventIDs, ore.StateBeforeRemovesEventIDs,
	)
	combinedAddsEvents, err := s.lookupStateEvents(combinedAdds, ore.Event.PDU)
	if err != nil {
		return nil, err
	}

	combinedAddsJoinedHosts, err := JoinedHostsFromEvents(s.ctx, combinedAddsEvents, s.rsAPI)
	if err != nil {
		return nil, err
	}

	removed := map[string]bool{}
	for _, eventID := range combinedRemoves {
		removed[eventID] = true
	}

	joined := map[spec.ServerName]bool{}
	for _, joinedHost := range oldJoinedHosts {
		if removed[joinedHost.MemberEventID] {
			// This m.frame.member event is part of the current state of the
			// frame, but not part of the state at the event we are processing
			// Therefore we can't use it to tell whether the server was in
			// the frame at the event.
			continue
		}
		joined[joinedHost.ServerName] = true
	}

	for _, joinedHost := range combinedAddsJoinedHosts {
		// This m.frame.member event was part of the state of the frame at the
		// event, but isn't part of the current state of the frame now.
		joined[joinedHost.ServerName] = true
	}

	// handle peeking hosts
	inboundPeeks, err := s.db.GetInboundPeeks(s.ctx, ore.Event.PDU.FrameID())
	if err != nil {
		return nil, err
	}
	for _, inboundPeek := range inboundPeeks {
		joined[inboundPeek.ServerName] = true
	}

	var result []spec.ServerName
	for serverName, include := range joined {
		if include {
			result = append(result, serverName)
		}
	}
	return result, nil
}

// JoinedHostsFromEvents turns a list of state events into a list of joined hosts.
// This errors if one of the events was invalid.
// It should be impossible for an invalid event to get this far in the pipeline.
func JoinedHostsFromEvents(ctx context.Context, evs []xtools.PDU, rsAPI api.FederationDataframeAPI) ([]types.JoinedHost, error) {
	var joinedHosts []types.JoinedHost
	for _, ev := range evs {
		if ev.Type() != "m.frame.member" || ev.StateKey() == nil {
			continue
		}
		membership, err := ev.Membership()
		if err != nil {
			return nil, err
		}
		if membership != spec.Join {
			continue
		}
		validFrameID, err := spec.NewFrameID(ev.FrameID())
		if err != nil {
			return nil, err
		}
		var domain spec.ServerName
		userID, err := rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*ev.StateKey()))
		if err != nil {
			if errors.As(err, new(base64.CorruptInputError)) {
				// Fallback to using the "old" way of getting the user domain, avoids
				// "illegal base64 data at input byte 0" errors
				// FXME: we should do this in QueryUserIDForSender instead
				_, domain, err = xtools.SplitID('@', *ev.StateKey())
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			domain = userID.Domain()
		}

		joinedHosts = append(joinedHosts, types.JoinedHost{
			MemberEventID: ev.EventID(), ServerName: domain,
		})
	}
	return joinedHosts, nil
}

// combineDeltas combines two deltas into a single delta.
// Assumes that the order of operations is add(1), remove(1), add(2), remove(2).
// Removes duplicate entries and redundant operations from each delta.
func combineDeltas(adds1, removes1, adds2, removes2 []string) (adds, removes []string) {
	addSet := map[string]bool{}
	removeSet := map[string]bool{}

	// combine processes each unique value in a list.
	// If the value is in the removeFrom set then it is removed from that set.
	// Otherwise it is added to the addTo set.
	combine := func(values []string, removeFrom, addTo map[string]bool) {
		processed := map[string]bool{}
		for _, value := range values {
			if processed[value] {
				continue
			}
			processed[value] = true
			if removeFrom[value] {
				delete(removeFrom, value)
			} else {
				addTo[value] = true
			}
		}
	}

	combine(adds1, nil, addSet)
	combine(removes1, addSet, removeSet)
	combine(adds2, removeSet, addSet)
	combine(removes2, addSet, removeSet)

	for value := range addSet {
		adds = append(adds, value)
	}
	for value := range removeSet {
		removes = append(removes, value)
	}
	return
}

// lookupStateEvents looks up the state events that are added by a new event.
func (s *OutputFrameEventConsumer) lookupStateEvents(
	addsStateEventIDs []string, event xtools.PDU,
) ([]xtools.PDU, error) {
	// Fast path if there aren't any new state events.
	if len(addsStateEventIDs) == 0 {
		return nil, nil
	}

	// Fast path if the only state event added is the event itself.
	if len(addsStateEventIDs) == 1 && addsStateEventIDs[0] == event.EventID() {
		return []xtools.PDU{event}, nil
	}

	missing := addsStateEventIDs
	var result []xtools.PDU

	// Check if event itself is being added.
	for _, eventID := range missing {
		if eventID == event.EventID() {
			result = append(result, event)
			break
		}
	}
	missing = missingEventsFrom(result, addsStateEventIDs)

	if len(missing) == 0 {
		return result, nil
	}

	// At this point the missing events are neither the event itself nor are
	// they present in our local database. Our only option is to fetch them
	// from the dataframe using the query API.
	eventReq := api.QueryEventsByIDRequest{EventIDs: missing, FrameID: event.FrameID()}
	var eventResp api.QueryEventsByIDResponse
	if err := s.rsAPI.QueryEventsByID(s.ctx, &eventReq, &eventResp); err != nil {
		return nil, err
	}

	for _, headeredEvent := range eventResp.Events {
		result = append(result, headeredEvent.PDU)
	}

	missing = missingEventsFrom(result, addsStateEventIDs)

	if len(missing) != 0 {
		return nil, fmt.Errorf(
			"missing %d state events IDs at event %q", len(missing), event.EventID(),
		)
	}

	return result, nil
}

func missingEventsFrom(events []xtools.PDU, required []string) []string {
	have := map[string]bool{}
	for _, event := range events {
		have[event.EventID()] = true
	}
	var missing []string
	for _, eventID := range required {
		if !have[eventID] {
			missing = append(missing, eventID)
		}
	}
	return missing
}
