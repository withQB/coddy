package query

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"

	//"github.com/withqb/coddy/servers/dataframe/internal"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"

	"github.com/withqb/coddy/apis/clientapi/auth/authtypes"
	fsAPI "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/servers/dataframe/acls"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/helpers"
	"github.com/withqb/coddy/servers/dataframe/state"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
)

type Queryer struct {
	DB                storage.Database
	Cache             caching.DataFrameCaches
	IsLocalServerName func(spec.ServerName) bool
	ServerACLs        *acls.ServerACLs
	Cfg               *config.Dendrite
	FSAPI             fsAPI.DataframeFederationAPI
}

func (r *Queryer) RestrictedFrameJoinInfo(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID, localServerName spec.ServerName) (*xtools.RestrictedFrameJoinInfo, error) {
	frameInfo, err := r.QueryFrameInfo(ctx, frameID)
	if err != nil || frameInfo == nil || frameInfo.IsStub() {
		return nil, err
	}

	req := api.QueryServerJoinedToFrameRequest{
		ServerName: localServerName,
		FrameID:     frameID.String(),
	}
	res := api.QueryServerJoinedToFrameResponse{}
	if err = r.QueryServerJoinedToFrame(ctx, &req, &res); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.QueryServerJoinedToFrame failed")
		return nil, fmt.Errorf("InternalServerError: Failed to query frame: %w", err)
	}

	userJoinedToFrame, err := r.UserJoinedToFrame(ctx, types.FrameNID(frameInfo.FrameNID), senderID)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.UserJoinedToFrame failed")
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	locallyJoinedUsers, err := r.LocallyJoinedUsers(ctx, frameInfo.FrameVersion, types.FrameNID(frameInfo.FrameNID))
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.GetLocallyJoinedUsers failed")
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	return &xtools.RestrictedFrameJoinInfo{
		LocalServerInFrame: res.FrameExists && res.IsInFrame,
		UserJoinedToFrame:  userJoinedToFrame,
		JoinedUsers:       locallyJoinedUsers,
	}, nil
}

// QueryLatestEventsAndState implements api.DataframeInternalAPI
func (r *Queryer) QueryLatestEventsAndState(
	ctx context.Context,
	request *api.QueryLatestEventsAndStateRequest,
	response *api.QueryLatestEventsAndStateResponse,
) error {
	return helpers.QueryLatestEventsAndState(ctx, r.DB, r, request, response)
}

// QueryStateAfterEvents implements api.DataframeInternalAPI
func (r *Queryer) QueryStateAfterEvents(
	ctx context.Context,
	request *api.QueryStateAfterEventsRequest,
	response *api.QueryStateAfterEventsResponse,
) error {
	info, err := r.DB.FrameInfo(ctx, request.FrameID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return nil
	}

	frameState := state.NewStateResolution(r.DB, info, r)
	response.FrameExists = true
	response.FrameVersion = info.FrameVersion

	prevStates, err := r.DB.StateAtEventIDs(ctx, request.PrevEventIDs)
	if err != nil {
		if _, ok := err.(types.MissingEventError); ok {
			return nil
		}
		return err
	}
	response.PrevEventsExist = true

	var stateEntries []types.StateEntry
	if len(request.StateToFetch) == 0 {
		// Look up all of the current frame state.
		stateEntries, err = frameState.LoadCombinedStateAfterEvents(
			ctx, prevStates,
		)
	} else {
		// Look up the current state for the requested tuples.
		stateEntries, err = frameState.LoadStateAfterEventsForStringTuples(
			ctx, prevStates, request.StateToFetch,
		)
	}
	if err != nil {
		if _, ok := err.(types.MissingEventError); ok {
			return nil
		}
		if _, ok := err.(types.MissingStateError); ok {
			return nil
		}
		return err
	}

	stateEvents, err := helpers.LoadStateEvents(ctx, r.DB, info, stateEntries)
	if err != nil {
		return err
	}

	if len(request.PrevEventIDs) > 1 {
		var authEventIDs []string
		for _, e := range stateEvents {
			authEventIDs = append(authEventIDs, e.AuthEventIDs()...)
		}
		authEventIDs = xutil.UniqueStrings(authEventIDs)

		authEvents, err := GetAuthChain(ctx, r.DB.EventsFromIDs, info, authEventIDs)
		if err != nil {
			return fmt.Errorf("getAuthChain: %w", err)
		}

		stateEvents, err = xtools.ResolveConflicts(
			info.FrameVersion, xtools.ToPDUs(stateEvents), xtools.ToPDUs(authEvents), func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.QueryUserIDForSender(ctx, frameID, senderID)
			},
		)
		if err != nil {
			return fmt.Errorf("state.ResolveConflictsAdhoc: %w", err)
		}
	}

	for _, event := range stateEvents {
		response.StateEvents = append(response.StateEvents, &types.HeaderedEvent{PDU: event})
	}

	return nil
}

// QueryEventsByID queries a list of events by event ID for one frame. If no frame is specified, it will try to determine
// which frame to use by querying the first events frameID.
func (r *Queryer) QueryEventsByID(
	ctx context.Context,
	request *api.QueryEventsByIDRequest,
	response *api.QueryEventsByIDResponse,
) error {
	if len(request.EventIDs) == 0 {
		return nil
	}
	var err error
	// We didn't receive a frame ID, we need to fetch it first before we can continue.
	// This happens for e.g. ` /_coddy/federation/v1/event/{eventId}`
	var frameInfo *types.FrameInfo
	if request.FrameID == "" {
		var eventNIDs map[string]types.EventMetadata
		eventNIDs, err = r.DB.EventNIDs(ctx, []string{request.EventIDs[0]})
		if err != nil {
			return err
		}
		if len(eventNIDs) == 0 {
			return nil
		}
		frameInfo, err = r.DB.FrameInfoByNID(ctx, eventNIDs[request.EventIDs[0]].FrameNID)
	} else {
		frameInfo, err = r.DB.FrameInfo(ctx, request.FrameID)
	}
	if err != nil {
		return err
	}
	if frameInfo == nil {
		return nil
	}
	events, err := r.DB.EventsFromIDs(ctx, frameInfo, request.EventIDs)
	if err != nil {
		return err
	}

	for _, event := range events {
		response.Events = append(response.Events, &types.HeaderedEvent{PDU: event.PDU})
	}

	return nil
}

// QueryMembershipForSenderID implements api.DataframeInternalAPI
func (r *Queryer) QueryMembershipForSenderID(
	ctx context.Context,
	frameID spec.FrameID,
	senderID spec.SenderID,
	response *api.QueryMembershipForUserResponse,
) error {
	return r.queryMembershipForOptionalSenderID(ctx, frameID, &senderID, response)
}

// QueryMembershipForUser implements api.DataframeInternalAPI
func (r *Queryer) QueryMembershipForUser(
	ctx context.Context,
	request *api.QueryMembershipForUserRequest,
	response *api.QueryMembershipForUserResponse,
) error {
	frameID, err := spec.NewFrameID(request.FrameID)
	if err != nil {
		return err
	}
	senderID, err := r.QuerySenderIDForUser(ctx, *frameID, request.UserID)
	if err != nil {
		return err
	}

	return r.queryMembershipForOptionalSenderID(ctx, *frameID, senderID, response)
}

// Query membership information for provided sender ID and frame ID
//
// If sender ID is nil, then act as if the provided sender is not a member of the frame.
func (r *Queryer) queryMembershipForOptionalSenderID(ctx context.Context, frameID spec.FrameID, senderID *spec.SenderID, response *api.QueryMembershipForUserResponse) error {
	response.SenderID = senderID

	info, err := r.DB.FrameInfo(ctx, frameID.String())
	if err != nil {
		return err
	}
	if info == nil {
		response.FrameExists = false
		return nil
	}
	response.FrameExists = true

	if senderID == nil {
		return nil
	}

	membershipEventNID, stillInFrame, isFrameforgotten, err := r.DB.GetMembership(ctx, info.FrameNID, *senderID)
	if err != nil {
		return err
	}

	response.IsFrameForgotten = isFrameforgotten

	if membershipEventNID == 0 {
		response.HasBeenInFrame = false
		return nil
	}

	response.IsInFrame = stillInFrame
	response.HasBeenInFrame = true

	evs, err := r.DB.Events(ctx, info.FrameVersion, []types.EventNID{membershipEventNID})
	if err != nil {
		return err
	}
	if len(evs) != 1 {
		return fmt.Errorf("failed to load membership event for event NID %d", membershipEventNID)
	}

	response.EventID = evs[0].EventID()
	response.Membership, err = evs[0].Membership()
	return err
}

// QueryMembershipAtEvent returns the known memberships at a given event.
// If the state before an event is not known, an empty list will be returned
// for that event instead.
//
// Returned map from eventID to membership event. Events that
// do not have known state will return a nil event, resulting in a "leave" membership
// when calculating history visibility.
func (r *Queryer) QueryMembershipAtEvent(
	ctx context.Context,
	frameID spec.FrameID,
	eventIDs []string,
	senderID spec.SenderID,
) (map[string]*types.HeaderedEvent, error) {
	info, err := r.DB.FrameInfo(ctx, frameID.String())
	if err != nil {
		return nil, fmt.Errorf("unable to get frameInfo: %w", err)
	}
	if info == nil {
		return nil, fmt.Errorf("no frameInfo found")
	}

	// get the users stateKeyNID
	stateKeyNIDs, err := r.DB.EventStateKeyNIDs(ctx, []string{string(senderID)})
	if err != nil {
		return nil, fmt.Errorf("unable to get stateKeyNIDs for %s: %w", senderID, err)
	}
	if _, ok := stateKeyNIDs[string(senderID)]; !ok {
		return nil, fmt.Errorf("requested stateKeyNID for %s was not found", senderID)
	}

	eventIDMembershipMap, err := r.DB.GetMembershipForHistoryVisibility(ctx, stateKeyNIDs[string(senderID)], info, eventIDs...)
	switch err {
	case nil:
		return eventIDMembershipMap, nil
	case tables.OptimisationNotSupportedError: // fallthrough, slow way of getting the membership events for each event
	default:
		return eventIDMembershipMap, err
	}

	eventIDMembershipMap = make(map[string]*types.HeaderedEvent)
	stateEntries, err := helpers.MembershipAtEvent(ctx, r.DB, nil, eventIDs, stateKeyNIDs[string(senderID)], r)
	if err != nil {
		return eventIDMembershipMap, fmt.Errorf("unable to get state before event: %w", err)
	}

	// If we only have one or less state entries, we can short circuit the below
	// loop and avoid hitting the database
	allStateEventNIDs := make(map[types.EventNID]types.StateEntry)
	for _, eventID := range eventIDs {
		stateEntry := stateEntries[eventID]
		for _, s := range stateEntry {
			allStateEventNIDs[s.EventNID] = s
		}
	}

	var canShortCircuit bool
	if len(allStateEventNIDs) <= 1 {
		canShortCircuit = true
	}

	var memberships []types.Event
	for _, eventID := range eventIDs {
		stateEntry, ok := stateEntries[eventID]
		if !ok || len(stateEntry) == 0 {
			eventIDMembershipMap[eventID] = nil
			continue
		}

		// If we can short circuit, e.g. we only have 0 or 1 membership events, we only get the memberships
		// once. If we have more than one membership event, we need to get the state for each state entry.
		if canShortCircuit {
			if len(memberships) == 0 {
				memberships, err = helpers.GetMembershipsAtState(ctx, r.DB, info, stateEntry, false)
			}
		} else {
			memberships, err = helpers.GetMembershipsAtState(ctx, r.DB, info, stateEntry, false)
		}
		if err != nil {
			return eventIDMembershipMap, fmt.Errorf("unable to get memberships at state: %w", err)
		}

		// Iterate over all membership events we got. Given we only query the membership for
		// one user and assuming this user only ever has one membership event associated to
		// a given event, overwrite any other existing membership events.
		for i := range memberships {
			ev := memberships[i]
			if ev.Type() == spec.MFrameMember && ev.StateKeyEquals(string(senderID)) {
				eventIDMembershipMap[eventID] = &types.HeaderedEvent{PDU: ev.PDU}
			}
		}
	}

	return eventIDMembershipMap, nil
}

// QueryMembershipsForFrame implements api.DataframeInternalAPI
func (r *Queryer) QueryMembershipsForFrame(
	ctx context.Context,
	request *api.QueryMembershipsForFrameRequest,
	response *api.QueryMembershipsForFrameResponse,
) error {
	info, err := r.DB.FrameInfo(ctx, request.FrameID)
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}

	// If no sender is specified then we will just return the entire
	// set of memberships for the frame, regardless of whether a specific
	// user is allowed to see them or not.
	if request.SenderID == "" {
		var events []types.Event
		var eventNIDs []types.EventNID
		eventNIDs, err = r.DB.GetMembershipEventNIDsForFrame(ctx, info.FrameNID, request.JoinedOnly, request.LocalOnly)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return fmt.Errorf("r.DB.GetMembershipEventNIDsForFrame: %w", err)
		}
		events, err = r.DB.Events(ctx, info.FrameVersion, eventNIDs)
		if err != nil {
			return fmt.Errorf("r.DB.Events: %w", err)
		}
		for _, event := range events {
			clientEvent := synctypes.ToClientEventDefault(func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.QueryUserIDForSender(ctx, frameID, senderID)
			}, event)
			response.JoinEvents = append(response.JoinEvents, clientEvent)
		}
		return nil
	}

	membershipEventNID, stillInFrame, isFrameforgotten, err := r.DB.GetMembership(ctx, info.FrameNID, request.SenderID)
	if err != nil {
		return err
	}

	response.IsFrameForgotten = isFrameforgotten

	if membershipEventNID == 0 {
		response.HasBeenInFrame = false
		response.JoinEvents = nil
		return nil
	}

	response.HasBeenInFrame = true
	response.JoinEvents = []synctypes.ClientEvent{}

	var events []types.Event
	var stateEntries []types.StateEntry
	if stillInFrame {
		var eventNIDs []types.EventNID
		eventNIDs, err = r.DB.GetMembershipEventNIDsForFrame(ctx, info.FrameNID, request.JoinedOnly, false)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		events, err = r.DB.Events(ctx, info.FrameVersion, eventNIDs)
	} else {
		stateEntries, err = helpers.StateBeforeEvent(ctx, r.DB, info, membershipEventNID, r)
		if err != nil {
			logrus.WithField("membership_event_nid", membershipEventNID).WithError(err).Error("failed to load state before event")
			return err
		}
		events, err = helpers.GetMembershipsAtState(ctx, r.DB, info, stateEntries, request.JoinedOnly)
	}

	if err != nil {
		return err
	}

	for _, event := range events {
		clientEvent := synctypes.ToClientEventDefault(func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.QueryUserIDForSender(ctx, frameID, senderID)
		}, event)
		response.JoinEvents = append(response.JoinEvents, clientEvent)
	}

	return nil
}

// QueryServerJoinedToFrame implements api.DataframeInternalAPI
func (r *Queryer) QueryServerJoinedToFrame(
	ctx context.Context,
	request *api.QueryServerJoinedToFrameRequest,
	response *api.QueryServerJoinedToFrameResponse,
) error {
	info, err := r.DB.FrameInfo(ctx, request.FrameID)
	if err != nil {
		return fmt.Errorf("r.DB.FrameInfo: %w", err)
	}
	if info != nil {
		response.FrameVersion = info.FrameVersion
	}
	if info == nil || info.IsStub() {
		return nil
	}
	response.FrameExists = true

	if r.IsLocalServerName(request.ServerName) || request.ServerName == "" {
		response.IsInFrame, err = r.DB.GetLocalServerInFrame(ctx, info.FrameNID)
		if err != nil {
			return fmt.Errorf("r.DB.GetLocalServerInFrame: %w", err)
		}
	} else {
		response.IsInFrame, err = r.DB.GetServerInFrame(ctx, info.FrameNID, request.ServerName)
		if err != nil {
			return fmt.Errorf("r.DB.GetServerInFrame: %w", err)
		}
	}

	return nil
}

// QueryServerAllowedToSeeEvent implements api.DataframeInternalAPI
func (r *Queryer) QueryServerAllowedToSeeEvent(
	ctx context.Context,
	serverName spec.ServerName,
	eventID string,
	frameID string,
) (allowed bool, err error) {
	events, err := r.DB.EventNIDs(ctx, []string{eventID})
	if err != nil {
		return
	}
	if len(events) == 0 {
		return allowed, nil
	}
	info, err := r.DB.FrameInfoByNID(ctx, events[eventID].FrameNID)
	if err != nil {
		return allowed, err
	}
	if info == nil || info.IsStub() {
		return allowed, nil
	}
	var isInFrame bool
	if r.IsLocalServerName(serverName) || serverName == "" {
		isInFrame, err = r.DB.GetLocalServerInFrame(ctx, info.FrameNID)
		if err != nil {
			return allowed, fmt.Errorf("r.DB.GetLocalServerInFrame: %w", err)
		}
	} else {
		isInFrame, err = r.DB.GetServerInFrame(ctx, info.FrameNID, serverName)
		if err != nil {
			return allowed, fmt.Errorf("r.DB.GetServerInFrame: %w", err)
		}
	}

	return helpers.CheckServerAllowedToSeeEvent(
		ctx, r.DB, info, frameID, eventID, serverName, isInFrame, r,
	)
}

// QueryMissingEvents implements api.DataframeInternalAPI
func (r *Queryer) QueryMissingEvents(
	ctx context.Context,
	request *api.QueryMissingEventsRequest,
	response *api.QueryMissingEventsResponse,
) error {
	var front []string
	eventsToFilter := make(map[string]bool, len(request.LatestEvents))
	visited := make(map[string]bool, request.Limit) // request.Limit acts as a hint to size.
	for _, id := range request.EarliestEvents {
		visited[id] = true
	}

	for _, id := range request.LatestEvents {
		if !visited[id] {
			front = append(front, id)
			eventsToFilter[id] = true
		}
	}
	if len(front) == 0 {
		return nil // no events to query, give up.
	}
	events, err := r.DB.EventNIDs(ctx, []string{front[0]})
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil // we are missing the events being asked to search from, give up.
	}
	info, err := r.DB.FrameInfoByNID(ctx, events[front[0]].FrameNID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return fmt.Errorf("missing FrameInfo for frame %d", events[front[0]].FrameNID)
	}

	resultNIDs, redactEventIDs, err := helpers.ScanEventTree(ctx, r.DB, info, front, visited, request.Limit, request.ServerName, r)
	if err != nil {
		return err
	}

	loadedEvents, err := helpers.LoadEvents(ctx, r.DB, info, resultNIDs)
	if err != nil {
		return err
	}

	response.Events = make([]*types.HeaderedEvent, 0, len(loadedEvents)-len(eventsToFilter))
	for _, event := range loadedEvents {
		if !eventsToFilter[event.EventID()] {
			if _, ok := redactEventIDs[event.EventID()]; ok {
				event.Redact()
			}
			response.Events = append(response.Events, &types.HeaderedEvent{PDU: event})
		}
	}

	return err
}

// QueryStateAndAuthChain implements api.DataframeInternalAPI
func (r *Queryer) QueryStateAndAuthChain(
	ctx context.Context,
	request *api.QueryStateAndAuthChainRequest,
	response *api.QueryStateAndAuthChainResponse,
) error {
	info, err := r.DB.FrameInfo(ctx, request.FrameID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return nil
	}
	response.FrameExists = true
	response.FrameVersion = info.FrameVersion

	// handle this entirely separately to the other case so we don't have to pull out
	// the entire current state of the frame
	// TODO: this probably means it should be a different query operation...
	if request.OnlyFetchAuthChain {
		var authEvents []xtools.PDU
		authEvents, err = GetAuthChain(ctx, r.DB.EventsFromIDs, info, request.AuthEventIDs)
		if err != nil {
			return err
		}
		for _, event := range authEvents {
			response.AuthChainEvents = append(response.AuthChainEvents, &types.HeaderedEvent{PDU: event})
		}
		return nil
	}

	var stateEvents []xtools.PDU
	stateEvents, rejected, stateMissing, err := r.loadStateAtEventIDs(ctx, info, request.PrevEventIDs)
	if err != nil {
		return err
	}
	response.StateKnown = !stateMissing
	response.IsRejected = rejected
	response.PrevEventsExist = true

	// add the auth event IDs for the current state events too
	var authEventIDs []string
	authEventIDs = append(authEventIDs, request.AuthEventIDs...)
	for _, se := range stateEvents {
		authEventIDs = append(authEventIDs, se.AuthEventIDs()...)
	}
	authEventIDs = xutil.UniqueStrings(authEventIDs) // de-dupe

	authEvents, err := GetAuthChain(ctx, r.DB.EventsFromIDs, info, authEventIDs)
	if err != nil {
		return err
	}

	if request.ResolveState {
		stateEvents, err = xtools.ResolveConflicts(
			info.FrameVersion, xtools.ToPDUs(stateEvents), xtools.ToPDUs(authEvents), func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.QueryUserIDForSender(ctx, frameID, senderID)
			},
		)
		if err != nil {
			return err
		}
	}

	for _, event := range stateEvents {
		response.StateEvents = append(response.StateEvents, &types.HeaderedEvent{PDU: event})
	}

	for _, event := range authEvents {
		response.AuthChainEvents = append(response.AuthChainEvents, &types.HeaderedEvent{PDU: event})
	}

	return err
}

// first bool: is rejected, second bool: state missing
func (r *Queryer) loadStateAtEventIDs(ctx context.Context, frameInfo *types.FrameInfo, eventIDs []string) ([]xtools.PDU, bool, bool, error) {
	frameState := state.NewStateResolution(r.DB, frameInfo, r)
	prevStates, err := r.DB.StateAtEventIDs(ctx, eventIDs)
	if err != nil {
		switch err.(type) {
		case types.MissingEventError:
			return nil, false, true, nil
		case types.MissingStateError:
			return nil, false, true, nil
		default:
			return nil, false, false, err
		}
	}
	// Currently only used on /state and /state_ids
	rejected := false
	for i := range prevStates {
		if prevStates[i].IsRejected {
			rejected = true
			break
		}
	}

	// Look up the currrent state for the requested tuples.
	stateEntries, err := frameState.LoadCombinedStateAfterEvents(
		ctx, prevStates,
	)
	if err != nil {
		return nil, rejected, false, err
	}

	events, err := helpers.LoadStateEvents(ctx, r.DB, frameInfo, stateEntries)
	return events, rejected, false, err
}

type eventsFromIDs func(context.Context, *types.FrameInfo, []string) ([]types.Event, error)

// GetAuthChain fetches the auth chain for the given auth events. An auth chain
// is the list of all events that are referenced in the auth_events section, and
// all their auth_events, recursively. The returned set of events contain the
// given events. Will *not* error if we don't have all auth events.
func GetAuthChain(
	ctx context.Context, fn eventsFromIDs, frameInfo *types.FrameInfo, authEventIDs []string,
) ([]xtools.PDU, error) {
	// List of event IDs to fetch. On each pass, these events will be requested
	// from the database and the `eventsToFetch` will be updated with any new
	// events that we have learned about and need to find. When `eventsToFetch`
	// is eventually empty, we should have reached the end of the chain.
	eventsToFetch := authEventIDs
	authEventsMap := make(map[string]xtools.PDU)

	for len(eventsToFetch) > 0 {
		// Try to retrieve the events from the database.
		events, err := fn(ctx, frameInfo, eventsToFetch)
		if err != nil {
			return nil, err
		}

		// We've now fetched these events so clear out `eventsToFetch`. Soon we may
		// add newly discovered events to this for the next pass.
		eventsToFetch = eventsToFetch[:0]

		for _, event := range events {
			// Store the event in the event map - this prevents us from requesting it
			// from the database again.
			authEventsMap[event.EventID()] = event.PDU

			// Extract all of the auth events from the newly obtained event. If we
			// don't already have a record of the event, record it in the list of
			// events we want to request for the next pass.
			for _, authEventID := range event.AuthEventIDs() {
				if _, ok := authEventsMap[authEventID]; !ok {
					eventsToFetch = append(eventsToFetch, authEventID)
				}
			}
		}
	}

	// We've now retrieved all of the events we can. Flatten them down into an
	// array and return them.
	var authEvents []xtools.PDU
	for _, event := range authEventsMap {
		authEvents = append(authEvents, event)
	}

	return authEvents, nil
}

// QueryFrameVersionForFrame implements api.DataframeInternalAPI
func (r *Queryer) QueryFrameVersionForFrame(ctx context.Context, frameID string) (xtools.FrameVersion, error) {
	if frameVersion, ok := r.Cache.GetFrameVersion(frameID); ok {
		return frameVersion, nil
	}

	info, err := r.DB.FrameInfo(ctx, frameID)
	if err != nil {
		return "", err
	}
	if info == nil {
		return "", fmt.Errorf("QueryFrameVersionForFrame: missing frame info for frame %s", frameID)
	}
	r.Cache.StoreFrameVersion(frameID, info.FrameVersion)
	return info.FrameVersion, nil
}

func (r *Queryer) QueryPublishedFrames(
	ctx context.Context,
	req *api.QueryPublishedFramesRequest,
	res *api.QueryPublishedFramesResponse,
) error {
	if req.FrameID != "" {
		visible, err := r.DB.GetPublishedFrame(ctx, req.FrameID)
		if err == nil && visible {
			res.FrameIDs = []string{req.FrameID}
			return nil
		}
		return err
	}
	frames, err := r.DB.GetPublishedFrames(ctx, req.NetworkID, req.IncludeAllNetworks)
	if err != nil {
		return err
	}
	res.FrameIDs = frames
	return nil
}

func (r *Queryer) QueryCurrentState(ctx context.Context, req *api.QueryCurrentStateRequest, res *api.QueryCurrentStateResponse) error {
	res.StateEvents = make(map[xtools.StateKeyTuple]*types.HeaderedEvent)
	for _, tuple := range req.StateTuples {
		if tuple.StateKey == "*" && req.AllowWildcards {
			events, err := r.DB.GetStateEventsWithEventType(ctx, req.FrameID, tuple.EventType)
			if err != nil {
				return err
			}
			for _, e := range events {
				res.StateEvents[xtools.StateKeyTuple{
					EventType: e.Type(),
					StateKey:  *e.StateKey(),
				}] = e
			}
		} else {
			ev, err := r.DB.GetStateEvent(ctx, req.FrameID, tuple.EventType, tuple.StateKey)
			if err != nil {
				return err
			}
			if ev != nil {
				res.StateEvents[tuple] = ev
			}
		}
	}
	return nil
}

func (r *Queryer) QueryFramesForUser(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.FrameID, error) {
	frameIDStrs, err := r.DB.GetFramesByMembership(ctx, userID, desiredMembership)
	if err != nil {
		return nil, err
	}
	frameIDs := make([]spec.FrameID, len(frameIDStrs))
	for i, frameIDStr := range frameIDStrs {
		frameID, err := spec.NewFrameID(frameIDStr)
		if err != nil {
			return nil, err
		}
		frameIDs[i] = *frameID
	}
	return frameIDs, nil
}

func (r *Queryer) QueryKnownUsers(ctx context.Context, req *api.QueryKnownUsersRequest, res *api.QueryKnownUsersResponse) error {
	users, err := r.DB.GetKnownUsers(ctx, req.UserID, req.SearchString, req.Limit)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	for _, user := range users {
		res.Users = append(res.Users, authtypes.FullyQualifiedProfile{
			UserID: user,
		})
	}
	return nil
}

func (r *Queryer) QueryBulkStateContent(ctx context.Context, req *api.QueryBulkStateContentRequest, res *api.QueryBulkStateContentResponse) error {
	events, err := r.DB.GetBulkStateContent(ctx, req.FrameIDs, req.StateTuples, req.AllowWildcards)
	if err != nil {
		return err
	}
	res.Frames = make(map[string]map[xtools.StateKeyTuple]string)
	for _, ev := range events {
		if res.Frames[ev.FrameID] == nil {
			res.Frames[ev.FrameID] = make(map[xtools.StateKeyTuple]string)
		}
		frame := res.Frames[ev.FrameID]
		frame[xtools.StateKeyTuple{
			EventType: ev.EventType,
			StateKey:  ev.StateKey,
		}] = ev.ContentValue
		res.Frames[ev.FrameID] = frame
	}
	return nil
}

func (r *Queryer) QueryLeftUsers(ctx context.Context, req *api.QueryLeftUsersRequest, res *api.QueryLeftUsersResponse) error {
	var err error
	res.LeftUsers, err = r.DB.GetLeftUsers(ctx, req.StaleDeviceListUsers)
	return err
}

func (r *Queryer) QuerySharedUsers(ctx context.Context, req *api.QuerySharedUsersRequest, res *api.QuerySharedUsersResponse) error {
	parsedUserID, err := spec.NewUserID(req.UserID, true)
	if err != nil {
		return err
	}

	frameIDs, err := r.DB.GetFramesByMembership(ctx, *parsedUserID, "join")
	if err != nil {
		return err
	}
	frameIDs = append(frameIDs, req.IncludeFrameIDs...)
	excludeMap := make(map[string]bool)
	for _, frameID := range req.ExcludeFrameIDs {
		excludeMap[frameID] = true
	}
	// filter out excluded frames
	j := 0
	for i := range frameIDs {
		// move elements to include to the beginning of the slice
		// then trim elements on the right
		if !excludeMap[frameIDs[i]] {
			frameIDs[j] = frameIDs[i]
			j++
		}
	}
	frameIDs = frameIDs[:j]

	users, err := r.DB.JoinedUsersSetInFrames(ctx, frameIDs, req.OtherUserIDs, req.LocalOnly)
	if err != nil {
		return err
	}
	res.UserIDsToCount = users
	return nil
}

func (r *Queryer) QueryServerBannedFromFrame(ctx context.Context, req *api.QueryServerBannedFromFrameRequest, res *api.QueryServerBannedFromFrameResponse) error {
	if r.ServerACLs == nil {
		return errors.New("no server ACL tracking")
	}
	res.Banned = r.ServerACLs.IsServerBannedFromFrame(req.ServerName, req.FrameID)
	return nil
}

func (r *Queryer) QueryAuthChain(ctx context.Context, req *api.QueryAuthChainRequest, res *api.QueryAuthChainResponse) error {
	chain, err := GetAuthChain(ctx, r.DB.EventsFromIDs, nil, req.EventIDs)
	if err != nil {
		return err
	}
	hchain := make([]*types.HeaderedEvent, len(chain))
	for i := range chain {
		hchain[i] = &types.HeaderedEvent{PDU: chain[i]}
	}
	res.AuthChain = hchain
	return nil
}

func (r *Queryer) InvitePending(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (bool, error) {
	pending, _, _, _, err := helpers.IsInvitePending(ctx, r.DB, frameID.String(), senderID)
	return pending, err
}

func (r *Queryer) QueryFrameInfo(ctx context.Context, frameID spec.FrameID) (*types.FrameInfo, error) {
	return r.DB.FrameInfo(ctx, frameID.String())
}

func (r *Queryer) CurrentStateEvent(ctx context.Context, frameID spec.FrameID, eventType string, stateKey string) (xtools.PDU, error) {
	res, err := r.DB.GetStateEvent(ctx, frameID.String(), eventType, stateKey)
	if res == nil {
		return nil, err
	}
	return res, err
}

func (r *Queryer) UserJoinedToFrame(ctx context.Context, frameNID types.FrameNID, senderID spec.SenderID) (bool, error) {
	_, isIn, _, err := r.DB.GetMembership(ctx, frameNID, senderID)
	return isIn, err
}

func (r *Queryer) LocallyJoinedUsers(ctx context.Context, frameVersion xtools.FrameVersion, frameNID types.FrameNID) ([]xtools.PDU, error) {
	joinNIDs, err := r.DB.GetMembershipEventNIDsForFrame(ctx, frameNID, true, true)
	if err != nil {
		return nil, err
	}

	events, err := r.DB.Events(ctx, frameVersion, joinNIDs)
	if err != nil {
		return nil, err
	}

	// For each of the joined users, let's see if we can get a valid
	// membership event.
	joinedUsers := []xtools.PDU{}
	for _, event := range events {
		if event.Type() != spec.MFrameMember || event.StateKey() == nil {
			continue // shouldn't happen
		}

		joinedUsers = append(joinedUsers, event)
	}

	return joinedUsers, nil
}

func (r *Queryer) JoinedUserCount(ctx context.Context, frameID string) (int, error) {
	info, err := r.DB.FrameInfo(ctx, frameID)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, nil
	}

	// TODO: this can be further optimised by just using a SELECT COUNT query
	nids, err := r.DB.GetMembershipEventNIDsForFrame(ctx, info.FrameNID, true, false)
	return len(nids), err
}

// nolint:gocyclo
func (r *Queryer) QueryRestrictedJoinAllowed(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (string, error) {
	// Look up if we know anything about the frame. If it doesn't exist
	// or is a stub entry then we can't do anything.
	frameInfo, err := r.DB.FrameInfo(ctx, frameID.String())
	if err != nil {
		return "", fmt.Errorf("r.DB.FrameInfo: %w", err)
	}
	if frameInfo == nil || frameInfo.IsStub() {
		return "", nil // fmt.Errorf("frame %q doesn't exist or is stub frame", req.FrameID)
	}
	verImpl, err := xtools.GetFrameVersion(frameInfo.FrameVersion)
	if err != nil {
		return "", err
	}

	return verImpl.CheckRestrictedJoin(ctx, r.Cfg.Global.ServerName, &api.JoinFrameQuerier{Dataframe: r}, frameID, senderID)
}

func (r *Queryer) QuerySenderIDForUser(ctx context.Context, frameID spec.FrameID, userID spec.UserID) (*spec.SenderID, error) {
	version, err := r.DB.GetFrameVersion(ctx, frameID.String())
	if err != nil {
		return nil, err
	}

	switch version {
	case xtools.FrameVersionPseudoIDs:
		key, err := r.DB.SelectUserFramePublicKey(ctx, userID, frameID)
		if err != nil {
			return nil, err
		} else if key == nil {
			return nil, nil
		} else {
			senderID := spec.SenderID(spec.Base64Bytes(key).Encode())
			return &senderID, nil
		}
	default:
		senderID := spec.SenderID(userID.String())
		return &senderID, nil
	}
}

func (r *Queryer) QueryUserIDForSender(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
	userID, err := spec.NewUserID(string(senderID), true)
	if err == nil {
		return userID, nil
	}

	bytes := spec.Base64Bytes{}
	err = bytes.Decode(string(senderID))
	if err != nil {
		return nil, err
	}
	queryMap := map[spec.FrameID][]ed25519.PublicKey{frameID: {ed25519.PublicKey(bytes)}}
	result, err := r.DB.SelectUserIDsForPublicKeys(ctx, queryMap)
	if err != nil {
		return nil, err
	}

	if userKeys, ok := result[frameID]; ok {
		if userID, ok := userKeys[string(senderID)]; ok {
			return spec.NewUserID(userID, true)
		}
	}

	return nil, nil
}
