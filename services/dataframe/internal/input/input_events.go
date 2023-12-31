// Copyright 2017 Vector Creations Ltd
// Copyright 2018 New Vector Ltd
// Copyright 2019-2020 The Coddy.org Foundation C.I.C.
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

package input

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tidwall/gjson"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/dataframe/internal/helpers"

	userAPI "github.com/withqb/coddy/services/userapi/api"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/hooks"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/state"
	"github.com/withqb/coddy/services/dataframe/types"
	fedapi "github.com/withqb/coddy/services/federationapi/api"
)

// TDO: Does this value make sense?
const MaximumMissingProcessingTime = time.Minute * 2

var processFrameEventDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "dendrite",
		Subsystem: "dataframe",
		Name:      "processframeevent_duration_millis",
		Help:      "How long it takes the dataframe to process an event",
		Buckets: []float64{ // milliseconds
			5, 10, 25, 50, 75, 100, 250, 500,
			1000, 2000, 3000, 4000, 5000, 6000,
			7000, 8000, 9000, 10000, 15000, 20000,
		},
	},
	[]string{"frame_id"},
)

// processFrameEvent can only be called once at a time
//
// TODO(#375): This should be rewritten to allow concurrent calls. The
// difficulty is in ensuring that we correctly annotate events with the correct
// state deltas when sending to kafka streams
// TDO: Break up function - we should probably do transaction ID checks before calling this.
// nolint:gocyclo
func (r *Inputer) processFrameEvent(
	ctx context.Context,
	virtualHost spec.ServerName,
	input *api.InputFrameEvent,
) error {
	select {
	case <-ctx.Done():
		// Before we do anything, make sure the context hasn't expired for this pending task.
		// If it has then we'll give up straight away — it's probably a synchronous input
		// request and the caller has already given up, but the inbox task was still queued.
		return context.DeadlineExceeded
	default:
	}

	trace, ctx := internal.StartRegion(ctx, "processFrameEvent")
	trace.SetTag("frame_id", input.Event.FrameID())
	trace.SetTag("event_id", input.Event.EventID())
	defer trace.EndRegion()

	// Measure how long it takes to process this event.
	started := time.Now()
	defer func() {
		timetaken := time.Since(started)
		processFrameEventDuration.With(prometheus.Labels{
			"frame_id": input.Event.FrameID(),
		}).Observe(float64(timetaken.Milliseconds()))
	}()

	// Parse and validate the event JSON
	headered := input.Event
	event := headered.PDU
	logger := xutil.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id": event.EventID(),
		"frame_id":  event.FrameID(),
		"kind":     input.Kind,
		"origin":   input.Origin,
		"type":     event.Type(),
	})
	if input.HasState {
		logger = logger.WithFields(logrus.Fields{
			"has_state": input.HasState,
			"state_ids": len(input.StateEventIDs),
		})
	}

	// Don't waste time processing the event if the frame doesn't exist.
	// A frame entry locally will only be created in response to a create
	// event.
	frameInfo, rerr := r.DB.FrameInfo(ctx, event.FrameID())
	if rerr != nil {
		return fmt.Errorf("r.DB.FrameInfo: %w", rerr)
	}
	isCreateEvent := event.Type() == spec.MFrameCreate && event.StateKeyEquals("")
	if frameInfo == nil && !isCreateEvent {
		return fmt.Errorf("frame %s does not exist for event %s", event.FrameID(), event.EventID())
	}
	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return err
	}
	sender, err := r.Queryer.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
	if err != nil {
		return fmt.Errorf("failed getting userID for sender %q. %w", event.SenderID(), err)
	}
	senderDomain := spec.ServerName("")
	if sender != nil {
		senderDomain = sender.Domain()
	}

	// If we already know about this outlier and it hasn't been rejected
	// then we won't attempt to reprocess it. If it was rejected or has now
	// arrived as a different kind of event, then we can attempt to reprocess,
	// in case we have learned something new or need to weave the event into
	// the DAG now.
	if input.Kind == api.KindOutlier && frameInfo != nil {
		wasRejected, werr := r.DB.IsEventRejected(ctx, frameInfo.FrameNID, event.EventID())
		switch {
		case werr == sql.ErrNoRows:
			// We haven't seen this event before so continue.
		case werr != nil:
			// Something has gone wrong trying to find out if we rejected
			// this event already.
			logger.WithError(werr).Errorf("failed to check if event %q is already seen", event.EventID())
			return werr
		case !wasRejected:
			// We've seen this event before and it wasn't rejected so we
			// should ignore it.
			logger.Debugf("Already processed event %q, ignoring", event.EventID())
			return nil
		}
	}

	var missingAuth, missingPrev bool
	serverRes := &fedapi.QueryJoinedHostServerNamesInFrameResponse{}
	if !isCreateEvent {
		var missingAuthIDs, missingPrevIDs []string
		missingAuthIDs, missingPrevIDs, err = r.DB.MissingAuthPrevEvents(ctx, event)
		if err != nil {
			return fmt.Errorf("updater.MissingAuthPrevEvents: %w", err)
		}
		missingAuth = len(missingAuthIDs) > 0
		missingPrev = !input.HasState && len(missingPrevIDs) > 0
	}

	// If we have missing events (auth or prev), we build a list of servers to ask
	if missingAuth || missingPrev {
		serverReq := &fedapi.QueryJoinedHostServerNamesInFrameRequest{
			FrameID:             event.FrameID(),
			ExcludeSelf:        true,
			ExcludeBlacklisted: true,
		}
		if err = r.FSAPI.QueryJoinedHostServerNamesInFrame(ctx, serverReq, serverRes); err != nil {
			return fmt.Errorf("r.FSAPI.QueryJoinedHostServerNamesInFrame: %w", err)
		}
		// Sort all of the servers into a map so that we can randomise
		// their order. Then make sure that the input origin and the
		// event origin are first on the list.
		servers := map[spec.ServerName]struct{}{}
		for _, server := range serverRes.ServerNames {
			servers[server] = struct{}{}
		}
		// Don't try to talk to ourselves.
		delete(servers, r.Cfg.Coddy.ServerName)
		// Now build up the list of servers.
		serverRes.ServerNames = serverRes.ServerNames[:0]
		if input.Origin != "" && input.Origin != r.Cfg.Coddy.ServerName {
			serverRes.ServerNames = append(serverRes.ServerNames, input.Origin)
			delete(servers, input.Origin)
		}
		// Only perform this check if the sender mxid_mapping can be resolved.
		// Don't fail processing the event if we have no mxid_maping.
		if sender != nil && senderDomain != input.Origin && senderDomain != r.Cfg.Coddy.ServerName {
			serverRes.ServerNames = append(serverRes.ServerNames, senderDomain)
			delete(servers, senderDomain)
		}
		for server := range servers {
			serverRes.ServerNames = append(serverRes.ServerNames, server)
			delete(servers, server)
		}
	}

	isRejected := false
	var rejectionErr error

	// At this point we are checking whether we know all of the prev events, and
	// if we know the state before the prev events. This is necessary before we
	// try to do `calculateAndSetState` on the event later, otherwise it will fail
	// with missing event NIDs. If there's anything missing then we'll go and fetch
	// the prev events and state from the federation. Note that we only do this if
	// we weren't already told what the state before the event should be — if the
	// HasState option was set and a state set was provided (as is the case in a
	// typical federated frame join) then we won't bother trying to fetch prev events
	// because we may not be allowed to see them and we have no choice but to trust
	// the state event IDs provided to us in the join instead.
	if missingPrev && input.Kind == api.KindNew {
		// Don't do this for KindOld events, otherwise old events that we fetch
		// to satisfy missing prev events/state will end up recursively calling
		// processFrameEvent.
		if len(serverRes.ServerNames) > 0 {
			missingState := missingStateReq{
				origin:      input.Origin,
				virtualHost: virtualHost,
				inputer:     r,
				db:          r.DB,
				frameInfo:    frameInfo,
				federation:  r.FSAPI,
				keys:        r.KeyRing,
				framesMu:     internal.NewMutexByFrame(),
				servers:     serverRes.ServerNames,
				hadEvents:   map[string]bool{},
				haveEvents:  map[string]xtools.PDU{},
			}
			var stateSnapshot *parsedRespState
			if stateSnapshot, err = missingState.processEventWithMissingState(ctx, event, headered.Version()); err != nil {
				// Something went wrong with retrieving the missing state, so we can't
				// really do anything with the event other than reject it at this point.
				isRejected = true
				rejectionErr = fmt.Errorf("missingState.processEventWithMissingState: %w", err)
				switch e := err.(type) {
				case xtools.EventValidationError:
					if e.Persistable && stateSnapshot != nil {
						// We retrieved some state and we ended up having to call /state_ids for
						// the new event in question (probably because closing the gap by using
						// /get_missing_events didn't do what we hoped) so we'll instead overwrite
						// the state snapshot with the newly resolved state.
						missingPrev = false
						input.HasState = true
						input.StateEventIDs = make([]string, 0, len(stateSnapshot.StateEvents))
						for _, se := range stateSnapshot.StateEvents {
							input.StateEventIDs = append(input.StateEventIDs, se.EventID())
						}
					}
				}
			} else if stateSnapshot != nil {
				// We retrieved some state and we ended up having to call /state_ids for
				// the new event in question (probably because closing the gap by using
				// /get_missing_events didn't do what we hoped) so we'll instead overwrite
				// the state snapshot with the newly resolved state.
				missingPrev = false
				input.HasState = true
				input.StateEventIDs = make([]string, 0, len(stateSnapshot.StateEvents))
				for _, e := range stateSnapshot.StateEvents {
					input.StateEventIDs = append(input.StateEventIDs, e.EventID())
				}
			} else {
				// We retrieved some state and it would appear that rolling forward the
				// state did everything we needed it to do, so we can just resolve the
				// state for the event in the normal way.
				missingPrev = false
			}
		} else {
			// We're missing prev events or state for the event, but for some reason
			// we don't know any servers to ask. In this case we can't do anything but
			// reject the event and hope that it gets unrejected later.
			isRejected = true
			rejectionErr = fmt.Errorf("missing prev events and no other servers to ask")
		}
	}

	// Check that the auth events of the event are known.
	// If they aren't then we will ask the federation API for them.
	authEvents := xtools.NewAuthEvents(nil)
	knownEvents := map[string]*types.Event{}
	if err = r.fetchAuthEvents(ctx, logger, frameInfo, virtualHost, headered, &authEvents, knownEvents, serverRes.ServerNames); err != nil {
		return fmt.Errorf("r.fetchAuthEvents: %w", err)
	}

	// Check if the event is allowed by its auth events. If it isn't then
	// we consider the event to be "rejected" — it will still be persisted.
	if err = xtools.Allowed(event, &authEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
	}); err != nil {
		isRejected = true
		rejectionErr = err
		logger.WithError(rejectionErr).Warnf("Event %s not allowed by auth events", event.EventID())
	}

	// Accumulate the auth event NIDs.
	authEventIDs := event.AuthEventIDs()
	authEventNIDs := make([]types.EventNID, 0, len(authEventIDs))
	for _, authEventID := range authEventIDs {
		if _, ok := knownEvents[authEventID]; !ok {
			// Unknown auth events only really matter if the event actually failed
			// auth. If it passed auth then we can assume that everything that was
			// known was sufficient, even if extraneous auth events were specified
			// but weren't found.
			if isRejected {
				if event.StateKey() != nil {
					return fmt.Errorf(
						"missing auth event %s for state event %s (type %q, state key %q)",
						authEventID, event.EventID(), event.Type(), *event.StateKey(),
					)
				} else {
					return fmt.Errorf(
						"missing auth event %s for timeline event %s (type %q)",
						authEventID, event.EventID(), event.Type(),
					)
				}
			}
		} else {
			authEventNIDs = append(authEventNIDs, knownEvents[authEventID].EventNID)
		}
	}

	var softfail bool
	if input.Kind == api.KindNew && !isCreateEvent {
		// Check that the event passes authentication checks based on the
		// current frame state.
		softfail, err = helpers.CheckForSoftFail(ctx, r.DB, frameInfo, headered, input.StateEventIDs, r.Queryer)
		if err != nil {
			logger.WithError(err).Warn("Error authing soft-failed event")
		}
	}

	// Get the state before the event so that we can work out if the event was
	// allowed at the time, and also to get the history visibility. We won't
	// bother doing this if the event was already rejected as it just ends up
	// burning CPU time.
	historyVisibility := xtools.HistoryVisibilityShared // Default to shared.
	if input.Kind != api.KindOutlier && rejectionErr == nil && !isRejected && !isCreateEvent {
		historyVisibility, rejectionErr, err = r.processStateBefore(ctx, frameInfo, input, missingPrev)
		if err != nil {
			return fmt.Errorf("r.processStateBefore: %w", err)
		}
		if rejectionErr != nil {
			isRejected = true
		}
	}

	if frameInfo == nil {
		frameInfo, err = r.DB.GetOrCreateFrameInfo(ctx, event)
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateFrameInfo: %w", err)
		}
	}

	eventTypeNID, err := r.DB.GetOrCreateEventTypeNID(ctx, event.Type())
	if err != nil {
		return fmt.Errorf("r.DB.GetOrCreateEventTypeNID: %w", err)
	}

	eventStateKeyNID, err := r.DB.GetOrCreateEventStateKeyNID(ctx, event.StateKey())
	if err != nil {
		return fmt.Errorf("r.DB.GetOrCreateEventStateKeyNID: %w", err)
	}

	// Store the event.
	eventNID, stateAtEvent, err := r.DB.StoreEvent(ctx, event, frameInfo, eventTypeNID, eventStateKeyNID, authEventNIDs, isRejected)
	if err != nil {
		return fmt.Errorf("updater.StoreEvent: %w", err)
	}

	// For outliers we can stop after we've stored the event itself as it
	// doesn't have any associated state to store and we don't need to
	// notify anyone about it.
	if input.Kind == api.KindOutlier {
		logger.WithField("rejected", isRejected).Debug("Stored outlier")
		hooks.Run(hooks.KindNewEventPersisted, headered)
		return nil
	}

	// Request the frame info again — it's possible that the frame has been
	// created by now if it didn't exist already.
	frameInfo, err = r.DB.FrameInfo(ctx, event.FrameID())
	if err != nil {
		return fmt.Errorf("updater.FrameInfo: %w", err)
	}
	if frameInfo == nil {
		return fmt.Errorf("updater.FrameInfo missing for frame %s", event.FrameID())
	}

	if input.HasState || (!missingPrev && stateAtEvent.BeforeStateSnapshotNID == 0) {
		// We haven't calculated a state for this event yet.
		// Lets calculate one.
		err = r.calculateAndSetState(ctx, input, frameInfo, &stateAtEvent, event, isRejected)
		if err != nil {
			return fmt.Errorf("r.calculateAndSetState: %w", err)
		}
	}

	// if storing this event results in it being redacted then do so.
	// we do this after calculating state for this event as we may need to get power levels
	var (
		redactedEventID string
		redactionEvent  xtools.PDU
		redactedEvent   xtools.PDU
	)
	if !isRejected && !isCreateEvent {
		resolver := state.NewStateResolution(r.DB, frameInfo, r.Queryer)
		redactionEvent, redactedEvent, err = r.DB.MaybeRedactEvent(ctx, frameInfo, eventNID, event, &resolver, r.Queryer)
		if err != nil {
			return err
		}
		if redactedEvent != nil {
			redactedEventID = redactedEvent.EventID()
		}
	}

	// We stop here if the event is rejected: We've stored it but won't update
	// forward extremities or notify downstream components about it.
	switch {
	case isRejected:
		logger.WithError(rejectionErr).Warn("Stored rejected event")
		if rejectionErr != nil {
			return types.RejectedError(rejectionErr.Error())
		}
		return nil

	case softfail:
		logger.WithError(rejectionErr).Warn("Stored soft-failed event")
		if rejectionErr != nil {
			return types.RejectedError(rejectionErr.Error())
		}
		return nil
	}

	// TDO: Revist this to ensure we don't replace a current state mxid_mapping with an older one.
	if event.Version() == xtools.FrameVersionPseudoIDs && event.Type() == spec.MFrameMember {
		mapping := xtools.MemberContent{}
		if err = json.Unmarshal(event.Content(), &mapping); err != nil {
			return err
		}
		if mapping.MXIDMapping != nil {
			storeUserID, userErr := spec.NewUserID(mapping.MXIDMapping.UserID, true)
			if userErr != nil {
				return userErr
			}
			err = r.RSAPI.StoreUserFramePublicKey(ctx, mapping.MXIDMapping.UserFrameKey, *storeUserID, *validFrameID)
			if err != nil {
				return fmt.Errorf("failed storing user frame public key: %w", err)
			}
		}
	}

	switch input.Kind {
	case api.KindNew:
		if err = r.updateLatestEvents(
			ctx,                 // context
			frameInfo,            // frame info for the frame being updated
			stateAtEvent,        // state at event (below)
			event,               // event
			input.SendAsServer,  // send as server
			input.TransactionID, // transaction ID
			input.HasState,      // rewrites state?
			historyVisibility,   // the history visibility before the event
		); err != nil {
			return fmt.Errorf("r.updateLatestEvents: %w", err)
		}
	case api.KindOld:
		err = r.OutputProducer.ProduceFrameEvents(event.FrameID(), []api.OutputEvent{
			{
				Type: api.OutputTypeOldFrameEvent,
				OldFrameEvent: &api.OutputOldFrameEvent{
					Event:             headered,
					HistoryVisibility: historyVisibility,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("r.WriteOutputEvents (old): %w", err)
		}
	}

	// Handle remote frame upgrades, e.g. remove published frame
	if event.Type() == "m.frame.tombstone" && event.StateKeyEquals("") && !r.Cfg.Coddy.IsLocalServerName(senderDomain) {
		if err = r.handleRemoteFrameUpgrade(ctx, event); err != nil {
			return fmt.Errorf("failed to handle remote frame upgrade: %w", err)
		}
	}

	// processing this event resulted in an event (which may not be the one we're processing)
	// being redacted. We are guaranteed to have both sides (the redaction/redacted event),
	// so notify downstream components to redact this event - they should have it if they've
	// been tracking our output log.
	if redactedEventID != "" {
		err = r.OutputProducer.ProduceFrameEvents(event.FrameID(), []api.OutputEvent{
			{
				Type: api.OutputTypeRedactedEvent,
				RedactedEvent: &api.OutputRedactedEvent{
					RedactedEventID: redactedEventID,
					RedactedBecause: &types.HeaderedEvent{PDU: redactionEvent},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("r.WriteOutputEvents (redactions): %w", err)
		}
	}

	// If guest_access changed and is not can_join, kick all guest users.
	if event.Type() == spec.MFrameGuestAccess && gjson.GetBytes(event.Content(), "guest_access").Str != "can_join" {
		if err = r.kickGuests(ctx, event, frameInfo); err != nil && err != sql.ErrNoRows {
			logrus.WithError(err).Error("failed to kick guest users on m.frame.guest_access revocation")
		}
	}

	// Everything was OK — the latest events updater didn't error and
	// we've sent output events. Finally, generate a hook call.
	hooks.Run(hooks.KindNewEventPersisted, headered)
	return nil
}

// handleRemoteFrameUpgrade updates published frames and frame aliases
func (r *Inputer) handleRemoteFrameUpgrade(ctx context.Context, event xtools.PDU) error {
	oldFrameID := event.FrameID()
	newFrameID := gjson.GetBytes(event.Content(), "replacement_frame").Str
	return r.DB.UpgradeFrame(ctx, oldFrameID, newFrameID, string(event.SenderID()))
}

// processStateBefore works out what the state is before the event and
// then checks the event auths against the state at the time. It also
// tries to determine what the history visibility was of the event at
// the time, so that it can be sent in the output event to downstream
// components.
// nolint:nakedret
func (r *Inputer) processStateBefore(
	ctx context.Context,
	frameInfo *types.FrameInfo,
	input *api.InputFrameEvent,
	missingPrev bool,
) (historyVisibility xtools.HistoryVisibility, rejectionErr error, err error) {
	historyVisibility = xtools.HistoryVisibilityShared // Default to shared.
	event := input.Event.PDU
	isCreateEvent := event.Type() == spec.MFrameCreate && event.StateKeyEquals("")
	var stateBeforeEvent []xtools.PDU
	switch {
	case isCreateEvent:
		// There's no state before a create event so there is nothing
		// else to do.
		return
	case input.HasState:
		// If we're overriding the state then we need to go and retrieve
		// them from the database. It's a hard error if they are missing.
		stateEvents, err := r.DB.EventsFromIDs(ctx, frameInfo, input.StateEventIDs)
		if err != nil {
			return "", nil, fmt.Errorf("r.DB.EventsFromIDs: %w", err)
		}
		stateBeforeEvent = make([]xtools.PDU, 0, len(stateEvents))
		for _, entry := range stateEvents {
			stateBeforeEvent = append(stateBeforeEvent, entry.PDU)
		}
	case missingPrev:
		// We don't know all of the prev events, so we can't work out
		// the state before the event. Reject it in that case.
		rejectionErr = fmt.Errorf("event %q has missing prev events", event.EventID())
		return
	case len(event.PrevEventIDs()) == 0:
		// There should be prev events since it's not a create event.
		// A non-create event that claims to have no prev events is
		// invalid, so reject it.
		rejectionErr = fmt.Errorf("event %q must have prev events", event.EventID())
		return
	default:
		// For all non-create events, there must be prev events, so we'll
		// ask the query API for the relevant tuples needed for auth. We
		// will include the history visibility here even though we don't
		// actually need it for auth, because we want to send it in the
		// output events.
		tuplesNeeded := xtools.StateNeededForAuth([]xtools.PDU{event}).Tuples()
		tuplesNeeded = append(tuplesNeeded, xtools.StateKeyTuple{
			EventType: spec.MFrameHistoryVisibility,
			StateKey:  "",
		})
		stateBeforeReq := &api.QueryStateAfterEventsRequest{
			FrameID:       event.FrameID(),
			PrevEventIDs: event.PrevEventIDs(),
			StateToFetch: tuplesNeeded,
		}
		stateBeforeRes := &api.QueryStateAfterEventsResponse{}
		if err := r.Queryer.QueryStateAfterEvents(ctx, stateBeforeReq, stateBeforeRes); err != nil {
			return "", nil, fmt.Errorf("r.Queryer.QueryStateAfterEvents: %w", err)
		}
		switch {
		case !stateBeforeRes.FrameExists:
			rejectionErr = fmt.Errorf("frame %q does not exist", event.FrameID())
			return
		case !stateBeforeRes.PrevEventsExist:
			rejectionErr = fmt.Errorf("prev events of %q are not known", event.EventID())
			return
		default:
			stateBeforeEvent = make([]xtools.PDU, len(stateBeforeRes.StateEvents))
			for i := range stateBeforeRes.StateEvents {
				stateBeforeEvent[i] = stateBeforeRes.StateEvents[i].PDU
			}
		}
	}
	// At this point, stateBeforeEvent should be populated either by
	// the supplied state in the input request, or from the prev events.
	// Check whether the event is allowed or not.
	stateBeforeAuth := xtools.NewAuthEvents(
		xtools.ToPDUs(stateBeforeEvent),
	)
	if rejectionErr = xtools.Allowed(event, &stateBeforeAuth, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
	}); rejectionErr != nil {
		rejectionErr = fmt.Errorf("Allowed() failed for stateBeforeEvent: %w", rejectionErr)
		return
	}
	// Work out what the history visibility was at the time of the
	// event.
	for _, event := range stateBeforeEvent {
		if event.Type() != spec.MFrameHistoryVisibility || !event.StateKeyEquals("") {
			continue
		}
		if hisVis, err := event.HistoryVisibility(); err == nil {
			historyVisibility = hisVis
			break
		}
	}
	return
}

// fetchAuthEvents will check to see if any of the
// auth events specified by the given event are unknown. If they are
// then we will go off and request them from the federation and then
// store them in the database. By the time this function ends, either
// we've failed to retrieve the auth chain altogether (in which case
// an error is returned) or we've successfully retrieved them all and
// they are now in the database.
// nolint: gocyclo
func (r *Inputer) fetchAuthEvents(
	ctx context.Context,
	logger *logrus.Entry,
	frameInfo *types.FrameInfo,
	virtualHost spec.ServerName,
	event *types.HeaderedEvent,
	auth *xtools.AuthEvents,
	known map[string]*types.Event,
	servers []spec.ServerName,
) error {
	trace, ctx := internal.StartRegion(ctx, "fetchAuthEvents")
	defer trace.EndRegion()

	unknown := map[string]struct{}{}
	authEventIDs := event.AuthEventIDs()
	if len(authEventIDs) == 0 {
		return nil
	}

	for _, authEventID := range authEventIDs {
		authEvents, err := r.DB.EventsFromIDs(ctx, frameInfo, []string{authEventID})
		if err != nil || len(authEvents) == 0 || authEvents[0].PDU == nil {
			unknown[authEventID] = struct{}{}
			continue
		}
		ev := authEvents[0]

		isRejected := false
		if frameInfo != nil {
			isRejected, err = r.DB.IsEventRejected(ctx, frameInfo.FrameNID, ev.EventID())
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("r.DB.IsEventRejected failed: %w", err)
			}
		}
		known[authEventID] = &ev // don't take the pointer of the iterated event
		if !isRejected {
			if err = auth.AddEvent(ev.PDU); err != nil {
				return fmt.Errorf("auth.AddEvent: %w", err)
			}
		}
	}

	// If there are no missing auth events then there is nothing more
	// to do — we've loaded everything that we need.
	if len(unknown) == 0 {
		return nil
	}

	var err error
	var res fclient.RespEventAuth
	var found bool
	for _, serverName := range servers {
		// Request the entire auth chain for the event in question. This should
		// contain all of the auth events — including ones that we already know —
		// so we'll need to filter through those in the next section.
		res, err = r.FSAPI.GetEventAuth(ctx, virtualHost, serverName, event.Version(), event.FrameID(), event.EventID())
		if err != nil {
			logger.WithError(err).Warnf("failed to get event auth from federation for %q: %s", event.EventID(), err)
			continue
		}
		found = true
		break
	}
	if !found {
		return fmt.Errorf("no servers provided event auth for event ID %q, tried servers %v", event.EventID(), servers)
	}

	// Reuse these to reduce allocations.
	authEventNIDs := make([]types.EventNID, 0, 5)
	isRejected := false
nextAuthEvent:
	for _, authEvent := range xtools.ReverseTopologicalOrdering(
		xtools.ToPDUs(res.AuthEvents.UntrustedEvents(event.Version())),
		xtools.TopologicalOrderByAuthEvents,
	) {
		// If we already know about this event from the database then we don't
		// need to store it again or do anything further with it, so just skip
		// over it rather than wasting cycles.
		if ev, ok := known[authEvent.EventID()]; ok && ev != nil {
			continue nextAuthEvent
		}

		// Check the signatures of the event. If this fails then we'll simply
		// skip it, because xtools.Allowed() will notice a problem
		// if a critical event is missing anyway.
		if err := xtools.VerifyEventSignatures(ctx, authEvent, r.FSAPI.KeyRing(), func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
		}); err != nil {
			continue nextAuthEvent
		}

		// In order to store the new auth event, we need to know its auth chain
		// as NIDs for the `auth_event_nids` column. Let's see if we can find those.
		authEventNIDs = authEventNIDs[:0]
		for _, eventID := range authEvent.AuthEventIDs() {
			knownEvent, ok := known[eventID]
			if !ok {
				continue nextAuthEvent
			}
			authEventNIDs = append(authEventNIDs, knownEvent.EventNID)
		}

		// Check if the auth event should be rejected.
		err := xtools.Allowed(authEvent, auth, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
		})
		if isRejected = err != nil; isRejected {
			logger.WithError(err).Warnf("Auth event %s rejected", authEvent.EventID())
		}

		if frameInfo == nil {
			frameInfo, err = r.DB.GetOrCreateFrameInfo(ctx, authEvent)
			if err != nil {
				return fmt.Errorf("r.DB.GetOrCreateFrameInfo: %w", err)
			}
		}

		eventTypeNID, err := r.DB.GetOrCreateEventTypeNID(ctx, authEvent.Type())
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateEventTypeNID: %w", err)
		}

		eventStateKeyNID, err := r.DB.GetOrCreateEventStateKeyNID(ctx, event.StateKey())
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateEventStateKeyNID: %w", err)
		}

		// Finally, store the event in the database.
		eventNID, _, err := r.DB.StoreEvent(ctx, authEvent, frameInfo, eventTypeNID, eventStateKeyNID, authEventNIDs, isRejected)
		if err != nil {
			return fmt.Errorf("updater.StoreEvent: %w", err)
		}

		// Let's take a note of the fact that we now know about this event for
		// authenticating future events.
		if !isRejected {
			if err := auth.AddEvent(authEvent); err != nil {
				return fmt.Errorf("auth.AddEvent: %w", err)
			}
		}

		// Now we know about this event, it was stored and the signatures were OK.
		known[authEvent.EventID()] = &types.Event{
			EventNID: eventNID,
			PDU:      authEvent,
		}
	}

	return nil
}

func (r *Inputer) calculateAndSetState(
	ctx context.Context,
	input *api.InputFrameEvent,
	frameInfo *types.FrameInfo,
	stateAtEvent *types.StateAtEvent,
	event xtools.PDU,
	isRejected bool,
) error {
	trace, ctx := internal.StartRegion(ctx, "calculateAndSetState")
	defer trace.EndRegion()

	var succeeded bool
	updater, err := r.DB.GetFrameUpdater(ctx, frameInfo)
	if err != nil {
		return fmt.Errorf("r.DB.GetFrameUpdater: %w", err)
	}
	defer sqlutil.EndTransactionWithCheck(updater, &succeeded, &err)
	frameState := state.NewStateResolution(updater, frameInfo, r.Queryer)

	if input.HasState {
		// We've been told what the state at the event is so we don't need to calculate it.
		// Check that those state events are in the database and store the state.
		var entries []types.StateEntry
		if entries, err = r.DB.StateEntriesForEventIDs(ctx, input.StateEventIDs, true); err != nil {
			return fmt.Errorf("updater.StateEntriesForEventIDs: %w", err)
		}
		entries = types.DeduplicateStateEntries(entries)

		if stateAtEvent.BeforeStateSnapshotNID, err = updater.AddState(ctx, frameInfo.FrameNID, nil, entries); err != nil {
			return fmt.Errorf("updater.AddState: %w", err)
		}
	} else {
		// We haven't been told what the state at the event is so we need to calculate it from the prev_events
		if stateAtEvent.BeforeStateSnapshotNID, err = frameState.CalculateAndStoreStateBeforeEvent(ctx, event, isRejected); err != nil {
			return fmt.Errorf("frameState.CalculateAndStoreStateBeforeEvent: %w", err)
		}
	}

	err = updater.SetState(ctx, stateAtEvent.EventNID, stateAtEvent.BeforeStateSnapshotNID)
	if err != nil {
		return fmt.Errorf("r.DB.SetState: %w", err)
	}
	succeeded = true
	return nil
}

// kickGuests kicks guests users from m.frame.guest_access frames, if guest access is now prohibited.
func (r *Inputer) kickGuests(ctx context.Context, event xtools.PDU, frameInfo *types.FrameInfo) error {
	membershipNIDs, err := r.DB.GetMembershipEventNIDsForFrame(ctx, frameInfo.FrameNID, true, true)
	if err != nil {
		return err
	}

	if frameInfo == nil {
		return types.ErrorInvalidFrameInfo
	}
	memberEvents, err := r.DB.Events(ctx, frameInfo.FrameVersion, membershipNIDs)
	if err != nil {
		return err
	}

	inputEvents := make([]api.InputFrameEvent, 0, len(memberEvents))
	latestReq := &api.QueryLatestEventsAndStateRequest{
		FrameID: event.FrameID(),
	}
	latestRes := &api.QueryLatestEventsAndStateResponse{}
	if err = r.Queryer.QueryLatestEventsAndState(ctx, latestReq, latestRes); err != nil {
		return err
	}

	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return err
	}

	prevEvents := latestRes.LatestEvents
	for _, memberEvent := range memberEvents {
		if memberEvent.StateKey() == nil {
			continue
		}

		memberUserID, err := r.Queryer.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*memberEvent.StateKey()))
		if err != nil {
			continue
		}

		accountRes := &userAPI.QueryAccountByLocalpartResponse{}
		if err = r.UserAPI.QueryAccountByLocalpart(ctx, &userAPI.QueryAccountByLocalpartRequest{
			Localpart:  memberUserID.Local(),
			ServerName: memberUserID.Domain(),
		}, accountRes); err != nil {
			return err
		}
		if accountRes.Account == nil {
			continue
		}

		if accountRes.Account.AccountType != userAPI.AccountTypeGuest {
			continue
		}

		var memberContent xtools.MemberContent
		if err = json.Unmarshal(memberEvent.Content(), &memberContent); err != nil {
			return err
		}
		memberContent.Membership = spec.Leave

		stateKey := *memberEvent.StateKey()
		fledglingEvent := &xtools.ProtoEvent{
			FrameID:     event.FrameID(),
			Type:       spec.MFrameMember,
			StateKey:   &stateKey,
			SenderID:   stateKey,
			PrevEvents: prevEvents,
		}

		if fledglingEvent.Content, err = json.Marshal(memberContent); err != nil {
			return err
		}

		eventsNeeded, err := xtools.StateNeededForProtoEvent(fledglingEvent)
		if err != nil {
			return err
		}

		validFrameID, err := spec.NewFrameID(event.FrameID())
		if err != nil {
			return err
		}

		signingIdentity, err := r.SigningIdentity(ctx, *validFrameID, *memberUserID)
		if err != nil {
			return err
		}

		event, err := eventutil.BuildEvent(ctx, fledglingEvent, &signingIdentity, time.Now(), &eventsNeeded, latestRes)
		if err != nil {
			return err
		}

		inputEvents = append(inputEvents, api.InputFrameEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       memberUserID.Domain(),
			SendAsServer: string(memberUserID.Domain()),
		})
		prevEvents = []string{event.EventID()}
	}

	inputReq := &api.InputFrameEventsRequest{
		InputFrameEvents: inputEvents,
		Asynchronous:    true, // Needs to be async, as we otherwise create a deadlock
	}
	inputRes := &api.InputFrameEventsResponse{}
	r.InputFrameEvents(ctx, inputReq, inputRes)
	return nil
}
