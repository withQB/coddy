package perform

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/auth"
	"github.com/withqb/coddy/services/dataframe/internal/helpers"
	"github.com/withqb/coddy/services/dataframe/state"
	"github.com/withqb/coddy/services/dataframe/storage"
	"github.com/withqb/coddy/services/dataframe/types"
	federationAPI "github.com/withqb/coddy/services/federationapi/api"
)

// the max number of servers to backfill from per request. If this is too low we may fail to backfill when
// we could've from another server. If this is too high we may take far too long to successfully backfill
// as we try dead servers.
const maxBackfillServers = 5

type Backfiller struct {
	IsLocalServerName func(spec.ServerName) bool
	DB                storage.Database
	FSAPI             federationAPI.DataframeFederationAPI
	KeyRing           xtools.JSONVerifier
	Querier           api.QuerySenderIDAPI

	// The servers which should be preferred above other servers when backfilling
	PreferServers []spec.ServerName
}

// PerformBackfill implements api.DataFrameQueryAPI
func (r *Backfiller) PerformBackfill(
	ctx context.Context,
	request *api.PerformBackfillRequest,
	response *api.PerformBackfillResponse,
) error {
	// if we are requesting the backfill then we need to do a federation hit
	// TDO: we could be more sensible and fetch as many events we already have then request the rest
	//       which is what the syncapi does already.
	if r.IsLocalServerName(request.ServerName) {
		return r.backfillViaFederation(ctx, request, response)
	}
	// someone else is requesting the backfill, try to service their request.
	var err error
	var front []string

	// The limit defines the maximum number of events to retrieve, so it also
	// defines the highest number of elements in the map below.
	visited := make(map[string]bool, request.Limit)

	// this will include these events which is what we want
	front = request.PrevEventIDs()

	info, err := r.DB.FrameInfo(ctx, request.FrameID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return fmt.Errorf("PerformBackfill: missing frame info for frame %s", request.FrameID)
	}

	// Scan the event tree for events to send back.
	resultNIDs, redactEventIDs, err := helpers.ScanEventTree(ctx, r.DB, info, front, visited, request.Limit, request.ServerName, r.Querier)
	if err != nil {
		return err
	}

	// Retrieve events from the list that was filled previously. If we fail to get
	// events from the database then attempt once to get them from federation instead.
	var loadedEvents []xtools.PDU
	loadedEvents, err = helpers.LoadEvents(ctx, r.DB, info, resultNIDs)
	if err != nil {
		if _, ok := err.(types.MissingEventError); ok {
			return r.backfillViaFederation(ctx, request, response)
		}
		return err
	}

	for _, event := range loadedEvents {
		if _, ok := redactEventIDs[event.EventID()]; ok {
			event.Redact()
		}
		response.Events = append(response.Events, &types.HeaderedEvent{PDU: event})
	}

	return err
}

func (r *Backfiller) backfillViaFederation(ctx context.Context, req *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	info, err := r.DB.FrameInfo(ctx, req.FrameID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return fmt.Errorf("backfillViaFederation: missing frame info for frame %s", req.FrameID)
	}
	requester := newBackfillRequester(r.DB, r.FSAPI, r.Querier, req.VirtualHost, r.IsLocalServerName, req.BackwardsExtremities, r.PreferServers, info.FrameVersion)
	// Request 100 items regardless of what the query asks for.
	// We don't want to go much higher than this.
	// We can't honour exactly the limit as some sytests rely on requesting more for tests to pass
	// (so we don't need to hit /state_ids which the test has no listener for)
	// Specifically the test "Outbound federation can backfill events"
	events, err := xtools.RequestBackfill(
		ctx, req.VirtualHost, requester,
		r.KeyRing, req.FrameID, info.FrameVersion, req.PrevEventIDs(), 100, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Querier.QueryUserIDForSender(ctx, frameID, senderID)
		},
	)
	// Only return an error if we really couldn't get any events.
	if err != nil && len(events) == 0 {
		logrus.WithError(err).Errorf("xtools.RequestBackfill failed")
		return err
	}
	// If we got an error but still got events, that's fine, because a server might have returned a 404 (or something)
	// but other servers could provide the missing event.
	logrus.WithError(err).WithField("frame_id", req.FrameID).Infof("backfilled %d events", len(events))

	// persist these new events - auth checks have already been done
	frameNID, backfilledEventMap := persistEvents(ctx, r.DB, r.Querier, events)

	for _, ev := range backfilledEventMap {
		// now add state for these events
		stateIDs, ok := requester.eventIDToBeforeStateIDs[ev.EventID()]
		if !ok {
			// this should be impossible as all events returned must have pass Step 5 of the PDU checks
			// which requires a list of state IDs.
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to find state IDs for event which passed auth checks")
			continue
		}
		var entries []types.StateEntry
		if entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true); err != nil {
			// attempt to fetch the missing events
			r.fetchAndStoreMissingEvents(ctx, info.FrameVersion, requester, stateIDs, req.VirtualHost)
			// try again
			entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true)
			if err != nil {
				logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to get state entries for event")
				return err
			}
		}

		var beforeStateSnapshotNID types.StateSnapshotNID
		if beforeStateSnapshotNID, err = r.DB.AddState(ctx, frameNID, nil, entries); err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist state entries to get snapshot nid")
			return err
		}
		if err = r.DB.SetState(ctx, ev.EventNID, beforeStateSnapshotNID); err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist snapshot nid")
		}
	}

	// TDO: update backwards extremities, as that should be moved from syncapi to dataframe at some point.

	res.Events = make([]*types.HeaderedEvent, len(events))
	for i := range events {
		res.Events[i] = &types.HeaderedEvent{PDU: events[i]}
	}
	res.HistoryVisibility = requester.historyVisiblity
	return nil
}

// fetchAndStoreMissingEvents does a best-effort fetch and store of missing events specified in stateIDs. Returns no error as it is just
// best effort.
func (r *Backfiller) fetchAndStoreMissingEvents(ctx context.Context, frameVer xtools.FrameVersion,
	backfillRequester *backfillRequester, stateIDs []string, virtualHost spec.ServerName) {

	servers := backfillRequester.servers

	// work out which are missing
	nidMap, err := r.DB.EventNIDs(ctx, stateIDs)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Warn("cannot query missing events")
		return
	}
	missingMap := make(map[string]*types.HeaderedEvent) // id -> event
	for _, id := range stateIDs {
		if _, ok := nidMap[id]; !ok {
			missingMap[id] = nil
		}
	}
	xutil.GetLogger(ctx).Infof("Fetching %d missing state events (from %d possible servers)", len(missingMap), len(servers))

	// fetch the events from federation. Loop the servers first so if we find one that works we stick with them
	for _, srv := range servers {
		for id, ev := range missingMap {
			if ev != nil {
				continue // already found
			}
			logger := xutil.GetLogger(ctx).WithField("server", srv).WithField("event_id", id)
			res, err := r.FSAPI.GetEvent(ctx, virtualHost, srv, id)
			if err != nil {
				logger.WithError(err).Warn("failed to get event from server")
				continue
			}
			loader := xtools.NewEventsLoader(frameVer, r.KeyRing, backfillRequester, backfillRequester.ProvideEvents, false)
			result, err := loader.LoadAndVerify(ctx, res.PDUs, xtools.TopologicalOrderByPrevEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Querier.QueryUserIDForSender(ctx, frameID, senderID)
			})
			if err != nil {
				logger.WithError(err).Warn("failed to load and verify event")
				continue
			}
			logger.Infof("returned %d PDUs which made events %+v", len(res.PDUs), result)
			for _, res := range result {
				switch err := res.Error.(type) {
				case nil:
				case xtools.SignatureErr:
					// The signature of the event might not be valid anymore, for example if
					// the key ID was reused with a different signature.
					logger.WithError(err).Errorf("event failed PDU checks, storing anyway")
				case xtools.AuthChainErr, xtools.AuthRulesErr:
					logger.WithError(err).Warn("event failed PDU checks")
					continue
				default:
					logger.WithError(err).Warn("event failed PDU checks")
					continue
				}
				missingMap[id] = &types.HeaderedEvent{PDU: res.Event}
			}
		}
	}

	var newEvents []xtools.PDU
	for _, ev := range missingMap {
		if ev != nil {
			newEvents = append(newEvents, ev.PDU)
		}
	}
	xutil.GetLogger(ctx).Infof("Persisting %d new events", len(newEvents))
	persistEvents(ctx, r.DB, r.Querier, newEvents)
}

// backfillRequester implements xtools.BackfillRequester
type backfillRequester struct {
	db                storage.Database
	fsAPI             federationAPI.DataframeFederationAPI
	querier           api.QuerySenderIDAPI
	virtualHost       spec.ServerName
	isLocalServerName func(spec.ServerName) bool
	preferServer      map[spec.ServerName]bool
	bwExtrems         map[string][]string

	// per-request state
	servers                 []spec.ServerName
	eventIDToBeforeStateIDs map[string][]string
	eventIDMap              map[string]xtools.PDU
	historyVisiblity        xtools.HistoryVisibility
	frameVersion             xtools.FrameVersion
}

func newBackfillRequester(
	db storage.Database, fsAPI federationAPI.DataframeFederationAPI,
	querier api.QuerySenderIDAPI,
	virtualHost spec.ServerName,
	isLocalServerName func(spec.ServerName) bool,
	bwExtrems map[string][]string, preferServers []spec.ServerName,
	frameVersion xtools.FrameVersion,
) *backfillRequester {
	preferServer := make(map[spec.ServerName]bool)
	for _, p := range preferServers {
		preferServer[p] = true
	}
	return &backfillRequester{
		db:                      db,
		fsAPI:                   fsAPI,
		querier:                 querier,
		virtualHost:             virtualHost,
		isLocalServerName:       isLocalServerName,
		eventIDToBeforeStateIDs: make(map[string][]string),
		eventIDMap:              make(map[string]xtools.PDU),
		bwExtrems:               bwExtrems,
		preferServer:            preferServer,
		historyVisiblity:        xtools.HistoryVisibilityShared,
		frameVersion:             frameVersion,
	}
}

func (b *backfillRequester) StateIDsBeforeEvent(ctx context.Context, targetEvent xtools.PDU) ([]string, error) {
	b.eventIDMap[targetEvent.EventID()] = targetEvent
	if ids, ok := b.eventIDToBeforeStateIDs[targetEvent.EventID()]; ok {
		return ids, nil
	}
	if len(targetEvent.PrevEventIDs()) == 0 && targetEvent.Type() == "m.frame.create" && targetEvent.StateKeyEquals("") {
		xutil.GetLogger(ctx).WithField("frame_id", targetEvent.FrameID()).Info("Backfilled to the beginning of the frame")
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = []string{}
		return nil, nil
	}
	// if we have exactly 1 prev event and we know the state of the frame at that prev event, then just roll forward the prev event.
	// Else, we have to hit /state_ids because either we don't know the state at all at this event (new backwards extremity) or
	// we don't know the result of state res to merge forks (2 or more prev_events)
	if len(targetEvent.PrevEventIDs()) == 1 {
		prevEventID := targetEvent.PrevEventIDs()[0]
		prevEvent, ok := b.eventIDMap[prevEventID]
		if !ok {
			goto FederationHit
		}
		prevEventStateIDs, ok := b.eventIDToBeforeStateIDs[prevEventID]
		if !ok {
			goto FederationHit
		}
		newStateIDs := b.calculateNewStateIDs(targetEvent, prevEvent, prevEventStateIDs)
		if newStateIDs != nil {
			b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
			return newStateIDs, nil
		}
		// else we failed to calculate the new state, so fallthrough
	}

FederationHit:
	var lastErr error
	logrus.WithField("event_id", targetEvent.EventID()).Info("Requesting /state_ids at event")
	for _, srv := range b.servers { // hit any valid server
		c := xtools.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		res, err := c.StateIDsBeforeEvent(ctx, targetEvent)
		if err != nil {
			lastErr = err
			continue
		}
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = res
		return res, nil
	}
	return nil, lastErr
}

func (b *backfillRequester) calculateNewStateIDs(targetEvent, prevEvent xtools.PDU, prevEventStateIDs []string) []string {
	newStateIDs := prevEventStateIDs[:]
	if prevEvent.StateKey() == nil {
		// state is the same as the previous event
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}

	missingState := false // true if we are missing the info for a state event ID
	foundEvent := false   // true if we found a (type, state_key) match
	// find which state ID to replace, if any
	for i, id := range newStateIDs {
		ev, ok := b.eventIDMap[id]
		if !ok {
			missingState = true
			continue
		}
		// The state IDs BEFORE the target event are the state IDs BEFORE the prev_event PLUS the prev_event itself
		if ev.Type() == prevEvent.Type() && ev.StateKeyEquals(*prevEvent.StateKey()) {
			newStateIDs[i] = prevEvent.EventID()
			foundEvent = true
			break
		}
	}
	if !foundEvent && !missingState {
		// we can be certain that this is new state
		newStateIDs = append(newStateIDs, prevEvent.EventID())
		foundEvent = true
	}

	if foundEvent {
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}
	return nil
}

func (b *backfillRequester) StateBeforeEvent(ctx context.Context, frameVer xtools.FrameVersion,
	event xtools.PDU, eventIDs []string) (map[string]xtools.PDU, error) {

	// try to fetch the events from the database first
	events, err := b.ProvideEvents(frameVer, eventIDs)
	if err != nil {
		// non-fatal, fallthrough
		logrus.WithError(err).Info("failed to fetch events")
	} else {
		logrus.Infof("Fetched %d/%d events from the database", len(events), len(eventIDs))
		if len(events) == len(eventIDs) {
			result := make(map[string]xtools.PDU)
			for i := range events {
				result[events[i].EventID()] = events[i]
				b.eventIDMap[events[i].EventID()] = events[i]
			}
			return result, nil
		}
	}

	var lastErr error
	for _, srv := range b.servers {
		c := xtools.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		result, err := c.StateBeforeEvent(ctx, frameVer, event, eventIDs)
		if err != nil {
			lastErr = err
			continue
		}
		for eventID, ev := range result {
			b.eventIDMap[eventID] = ev
		}
		return result, nil
	}
	return nil, lastErr
}

// ServersAtEvent is called when trying to determine which server to request from.
// It returns a list of servers which can be queried for backfill requests. These servers
// will be servers that are in the frame already. The entries at the beginning are preferred servers
// and will be tried first. An empty list will fail the request.
func (b *backfillRequester) ServersAtEvent(ctx context.Context, frameID, eventID string) []spec.ServerName {
	// eventID will be a prev_event ID of a backwards extremity, meaning we will not have a database entry for it. Instead, use
	// its successor, so look it up.
	successor := ""
FindSuccessor:
	for sucID, prevEventIDs := range b.bwExtrems {
		for _, pe := range prevEventIDs {
			if pe == eventID {
				successor = sucID
				break FindSuccessor
			}
		}
	}
	if successor == "" {
		logrus.WithField("event_id", eventID).Error("ServersAtEvent: failed to find successor of this event to determine frame state")
		return nil
	}
	eventID = successor

	// getMembershipsBeforeEventNID requires a NID, so retrieving the NID for
	// the event is necessary.
	NIDs, err := b.db.EventNIDs(ctx, []string{eventID})
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get event NID for event")
		return nil
	}

	info, err := b.db.FrameInfo(ctx, frameID)
	if err != nil {
		logrus.WithError(err).WithField("frame_id", frameID).Error("ServersAtEvent: failed to get FrameInfo for frame")
		return nil
	}
	if info == nil || info.IsStub() {
		logrus.WithField("frame_id", frameID).Error("ServersAtEvent: failed to get FrameInfo for frame, frame is missing")
		return nil
	}

	stateEntries, err := helpers.StateBeforeEvent(ctx, b.db, info, NIDs[eventID].EventNID, b.querier)
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to load state before event")
		return nil
	}

	// possibly return all joined servers depending on history visiblity
	memberEventsFromVis, visibility, err := joinEventsFromHistoryVisibility(ctx, b.db, b.querier, info, stateEntries, b.virtualHost)
	b.historyVisiblity = visibility
	if err != nil {
		logrus.WithError(err).Error("ServersAtEvent: failed calculate servers from history visibility rules")
		return nil
	}
	logrus.Infof("ServersAtEvent including %d current events from history visibility", len(memberEventsFromVis))

	// Retrieve all "m.frame.member" state events of "join" membership, which
	// contains the list of users in the frame before the event, therefore all
	// the servers in it at that moment.
	memberEvents, err := helpers.GetMembershipsAtState(ctx, b.db, info, stateEntries, true)
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get memberships before event")
		return nil
	}
	memberEvents = append(memberEvents, memberEventsFromVis...)

	// Store the server names in a temporary map to avoid duplicates.
	serverSet := make(map[spec.ServerName]bool)
	for _, event := range memberEvents {
		validFrameID, err := spec.NewFrameID(event.FrameID())
		if err != nil {
			continue
		}
		if sender, err := b.querier.QueryUserIDForSender(ctx, *validFrameID, event.SenderID()); err == nil {
			serverSet[sender.Domain()] = true
		}
	}
	var servers []spec.ServerName
	for server := range serverSet {
		if b.isLocalServerName(server) {
			continue
		}
		if b.preferServer[server] { // insert at the front
			servers = append([]spec.ServerName{server}, servers...)
		} else { // insert at the back
			servers = append(servers, server)
		}
	}
	if len(servers) > maxBackfillServers {
		servers = servers[:maxBackfillServers]
	}

	b.servers = servers
	return servers
}

// Backfill performs a backfill request to the given server.
// get-coddy-federation-v1-backfill-frameid
func (b *backfillRequester) Backfill(ctx context.Context, origin, server spec.ServerName, frameID string,
	limit int, fromEventIDs []string) (xtools.Transaction, error) {

	tx, err := b.fsAPI.Backfill(ctx, origin, server, frameID, limit, fromEventIDs)
	return tx, err
}

func (b *backfillRequester) ProvideEvents(frameVer xtools.FrameVersion, eventIDs []string) ([]xtools.PDU, error) {
	ctx := context.Background()
	nidMap, err := b.db.EventNIDs(ctx, eventIDs)
	if err != nil {
		logrus.WithError(err).WithField("event_ids", eventIDs).Error("failed to find events")
		return nil, err
	}
	eventNIDs := make([]types.EventNID, len(nidMap))
	i := 0
	for _, nid := range nidMap {
		eventNIDs[i] = nid.EventNID
		i++
	}
	eventsWithNids, err := b.db.Events(ctx, b.frameVersion, eventNIDs)
	if err != nil {
		logrus.WithError(err).WithField("event_nids", eventNIDs).Error("failed to load events")
		return nil, err
	}
	events := make([]xtools.PDU, len(eventsWithNids))
	for i := range eventsWithNids {
		events[i] = eventsWithNids[i].PDU
	}
	return events, nil
}

// joinEventsFromHistoryVisibility returns all CURRENTLY joined members if our server can read the frame history
//
// TDO: Long term we probably want a history_visibility table which stores eventNID | visibility_enum so we can just
// pull all events and then filter by that table.
func joinEventsFromHistoryVisibility(
	ctx context.Context, db storage.FrameDatabase, querier api.QuerySenderIDAPI, frameInfo *types.FrameInfo, stateEntries []types.StateEntry,
	thisServer spec.ServerName) ([]types.Event, xtools.HistoryVisibility, error) {

	var eventNIDs []types.EventNID
	for _, entry := range stateEntries {
		// Filter the events to retrieve to only keep the membership events
		if entry.EventTypeNID == types.MFrameHistoryVisibilityNID && entry.EventStateKeyNID == types.EmptyStateKeyNID {
			eventNIDs = append(eventNIDs, entry.EventNID)
			break
		}
	}

	// Get all of the events in this state
	if frameInfo == nil {
		return nil, xtools.HistoryVisibilityJoined, types.ErrorInvalidFrameInfo
	}
	stateEvents, err := db.Events(ctx, frameInfo.FrameVersion, eventNIDs)
	if err != nil {
		// even though the default should be shared, restricting the visibility to joined
		// feels more secure here.
		return nil, xtools.HistoryVisibilityJoined, err
	}
	events := make([]xtools.PDU, len(stateEvents))
	for i := range stateEvents {
		events[i] = stateEvents[i].PDU
	}

	// Can we see events in the frame?
	canSeeEvents := auth.IsServerAllowed(ctx, querier, thisServer, true, events)
	visibility := auth.HistoryVisibilityForFrame(events)
	if !canSeeEvents {
		logrus.Infof("ServersAtEvent history not visible to us: %s", visibility)
		return nil, visibility, nil
	}
	// get joined members
	joinEventNIDs, err := db.GetMembershipEventNIDsForFrame(ctx, frameInfo.FrameNID, true, false)
	if err != nil {
		return nil, visibility, err
	}
	evs, err := db.Events(ctx, frameInfo.FrameVersion, joinEventNIDs)
	return evs, visibility, err
}

func persistEvents(ctx context.Context, db storage.Database, querier api.QuerySenderIDAPI, events []xtools.PDU) (types.FrameNID, map[string]types.Event) {
	var frameNID types.FrameNID
	var eventNID types.EventNID
	backfilledEventMap := make(map[string]types.Event)
	for j, ev := range events {
		nidMap, err := db.EventNIDs(ctx, ev.AuthEventIDs())
		if err != nil { // this shouldn't happen as RequestBackfill already found them
			logrus.WithError(err).WithField("auth_events", ev.AuthEventIDs()).Error("failed to find one or more auth events")
			continue
		}
		authNids := make([]types.EventNID, len(nidMap))
		i := 0
		for _, nid := range nidMap {
			authNids[i] = nid.EventNID
			i++
		}

		frameInfo, err := db.GetOrCreateFrameInfo(ctx, ev)
		if err != nil {
			logrus.WithError(err).Error("failed to get or create frameNID")
			continue
		}
		frameNID = frameInfo.FrameNID

		eventTypeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
		if err != nil {
			logrus.WithError(err).Error("failed to get or create eventType NID")
			continue
		}

		eventStateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
		if err != nil {
			logrus.WithError(err).Error("failed to get or create eventStateKey NID")
			continue
		}

		eventNID, _, err = db.StoreEvent(ctx, ev, frameInfo, eventTypeNID, eventStateKeyNID, authNids, false)
		if err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("failed to persist event")
			continue
		}

		resolver := state.NewStateResolution(db, frameInfo, querier)

		_, redactedEvent, err := db.MaybeRedactEvent(ctx, frameInfo, eventNID, ev, &resolver, querier)
		if err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("failed to redact event")
			continue
		}
		// If storing this event results in it being redacted, then do so.
		// It's also possible for this event to be a redaction which results in another event being
		// redacted, which we don't care about since we aren't returning it in this backfill.
		if redactedEvent != nil && redactedEvent.EventID() == ev.EventID() {
			ev = redactedEvent
			events[j] = ev
		}
		backfilledEventMap[ev.EventID()] = types.Event{
			EventNID: eventNID,
			PDU:      ev,
		}
	}
	return frameNID, backfilledEventMap
}
