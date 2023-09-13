package routing

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	dataframe "github.com/withqb/coddy/services/dataframe/api"
	rstypes "github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/syncapi/internal"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type ContextRespsonse struct {
	End          string                  `json:"end"`
	Event        *synctypes.ClientEvent  `json:"event,omitempty"`
	EventsAfter  []synctypes.ClientEvent `json:"events_after,omitempty"`
	EventsBefore []synctypes.ClientEvent `json:"events_before,omitempty"`
	Start        string                  `json:"start"`
	State        []synctypes.ClientEvent `json:"state,omitempty"`
}

func Context(
	req *http.Request, device *userapi.Device,
	rsAPI dataframe.SyncDataframeAPI,
	syncDB storage.Database,
	frameID, eventID string,
	lazyLoadCache caching.LazyLoadCache,
) xutil.JSONResponse {
	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	filter, err := parseFrameEventFilter(req)
	if err != nil {
		errMsg := ""
		switch err.(type) {
		case *json.InvalidUnmarshalError:
			errMsg = "unable to parse filter"
		case *strconv.NumError:
			errMsg = "unable to parse limit"
		default:
			errMsg = err.Error()
		}
		return xutil.JSONResponse{
			Code:    http.StatusBadRequest,
			JSON:    spec.InvalidParam(errMsg),
			Headers: nil,
		}
	}
	if filter.Frames != nil {
		*filter.Frames = append(*filter.Frames, frameID)
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Device UserID is invalid"),
		}
	}
	ctx := req.Context()
	membershipRes := dataframe.QueryMembershipForUserResponse{}
	membershipReq := dataframe.QueryMembershipForUserRequest{UserID: *userID, FrameID: frameID}
	if err = rsAPI.QueryMembershipForUser(ctx, &membershipReq, &membershipRes); err != nil {
		logrus.WithError(err).Error("unable to query membership")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipRes.FrameExists {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("frame does not exist"),
		}
	}

	stateFilter := synctypes.StateFilter{
		NotSenders:              filter.NotSenders,
		NotTypes:                filter.NotTypes,
		Senders:                 filter.Senders,
		Types:                   filter.Types,
		LazyLoadMembers:         filter.LazyLoadMembers,
		IncludeRedundantMembers: filter.IncludeRedundantMembers,
		NotFrames:                filter.NotFrames,
		Frames:                   filter.Frames,
		ContainsURL:             filter.ContainsURL,
	}

	id, requestedEvent, err := snapshot.SelectContextEvent(ctx, frameID, eventID)
	if err != nil {
		if err == sql.ErrNoRows {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(fmt.Sprintf("Event %s not found", eventID)),
			}
		}
		logrus.WithError(err).WithField("eventID", eventID).Error("unable to find requested event")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// verify the user is allowed to see the context for this frame/event
	startTime := time.Now()
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, []*rstypes.HeaderedEvent{&requestedEvent}, nil, *userID, "context")
	if err != nil {
		logrus.WithError(err).Error("unable to apply history visibility filter")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"frame_id":  frameID,
	}).Debug("applied history visibility (context)")
	if len(filteredEvents) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("User is not allowed to query context"),
		}
	}

	eventsBefore, err := snapshot.SelectContextBeforeEvent(ctx, id, frameID, filter)
	if err != nil && err != sql.ErrNoRows {
		logrus.WithError(err).Error("unable to fetch before events")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	_, eventsAfter, err := snapshot.SelectContextAfterEvent(ctx, id, frameID, filter)
	if err != nil && err != sql.ErrNoRows {
		logrus.WithError(err).Error("unable to fetch after events")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	startTime = time.Now()
	eventsBeforeFiltered, eventsAfterFiltered, err := applyHistoryVisibilityOnContextEvents(ctx, snapshot, rsAPI, eventsBefore, eventsAfter, *userID)
	if err != nil {
		logrus.WithError(err).Error("unable to apply history visibility filter")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"frame_id":  frameID,
	}).Debug("applied history visibility (context eventsBefore/eventsAfter)")

	// TDO: Get the actual state at the last event returned by SelectContextAfterEvent
	state, err := snapshot.CurrentState(ctx, frameID, &stateFilter, nil)
	if err != nil {
		logrus.WithError(err).Error("unable to fetch current frame state")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	eventsBeforeClient := synctypes.ToClientEvents(xtools.ToPDUs(eventsBeforeFiltered), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
	})
	eventsAfterClient := synctypes.ToClientEvents(xtools.ToPDUs(eventsAfterFiltered), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
	})

	newState := state
	if filter.LazyLoadMembers {
		allEvents := append(eventsBeforeFiltered, eventsAfterFiltered...)
		allEvents = append(allEvents, &requestedEvent)
		evs := synctypes.ToClientEvents(xtools.ToPDUs(allEvents), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		})
		newState, err = applyLazyLoadMembers(ctx, device, snapshot, frameID, evs, lazyLoadCache)
		if err != nil {
			logrus.WithError(err).Error("unable to load membership events")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	ev := synctypes.ToClientEventDefault(func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
	}, requestedEvent)
	response := ContextRespsonse{
		Event:        &ev,
		EventsAfter:  eventsAfterClient,
		EventsBefore: eventsBeforeClient,
		State: synctypes.ToClientEvents(xtools.ToPDUs(newState), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		}),
	}

	if len(response.State) > filter.Limit {
		response.State = response.State[len(response.State)-filter.Limit:]
	}
	start, end, err := getStartEnd(ctx, snapshot, eventsBefore, eventsAfter)
	if err == nil {
		response.End = end.String()
		response.Start = start.String()
	}
	succeeded = true
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}

// applyHistoryVisibilityOnContextEvents is a helper function to avoid roundtrips to the dataframe
// by combining the events before and after the context event. Returns the filtered events,
// and an error, if any.
func applyHistoryVisibilityOnContextEvents(
	ctx context.Context, snapshot storage.DatabaseTransaction, rsAPI dataframe.SyncDataframeAPI,
	eventsBefore, eventsAfter []*rstypes.HeaderedEvent,
	userID spec.UserID,
) (filteredBefore, filteredAfter []*rstypes.HeaderedEvent, err error) {
	eventIDsBefore := make(map[string]struct{}, len(eventsBefore))
	eventIDsAfter := make(map[string]struct{}, len(eventsAfter))

	// Remember before/after eventIDs, so we can restore them
	// after applying history visibility checks
	for _, ev := range eventsBefore {
		eventIDsBefore[ev.EventID()] = struct{}{}
	}
	for _, ev := range eventsAfter {
		eventIDsAfter[ev.EventID()] = struct{}{}
	}

	allEvents := append(eventsBefore, eventsAfter...)
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, allEvents, nil, userID, "context")
	if err != nil {
		return nil, nil, err
	}

	// "Restore" events in the correct context
	for _, ev := range filteredEvents {
		if _, ok := eventIDsBefore[ev.EventID()]; ok {
			filteredBefore = append(filteredBefore, ev)
		}
		if _, ok := eventIDsAfter[ev.EventID()]; ok {
			filteredAfter = append(filteredAfter, ev)
		}
	}
	return filteredBefore, filteredAfter, nil
}

func getStartEnd(ctx context.Context, snapshot storage.DatabaseTransaction, startEvents, endEvents []*rstypes.HeaderedEvent) (start, end types.TopologyToken, err error) {
	if len(startEvents) > 0 {
		start, err = snapshot.EventPositionInTopology(ctx, startEvents[0].EventID())
		if err != nil {
			return
		}
	}
	if len(endEvents) > 0 {
		end, err = snapshot.EventPositionInTopology(ctx, endEvents[0].EventID())
	}
	return
}

func applyLazyLoadMembers(
	ctx context.Context,
	device *userapi.Device,
	snapshot storage.DatabaseTransaction,
	frameID string,
	events []synctypes.ClientEvent,
	lazyLoadCache caching.LazyLoadCache,
) ([]*rstypes.HeaderedEvent, error) {
	eventSenders := make(map[string]struct{})
	// get members who actually send an event
	for _, e := range events {
		// Don't add membership events the client should already know about
		if _, cached := lazyLoadCache.IsLazyLoadedUserCached(device, e.FrameID, e.Sender); cached {
			continue
		}
		eventSenders[e.Sender] = struct{}{}
	}

	wantUsers := make([]string, 0, len(eventSenders))
	for userID := range eventSenders {
		wantUsers = append(wantUsers, userID)
	}

	// Query missing membership events
	filter := synctypes.DefaultStateFilter()
	filter.Senders = &wantUsers
	filter.Types = &[]string{spec.MFrameMember}
	memberships, err := snapshot.GetStateEventsForFrame(ctx, frameID, &filter)
	if err != nil {
		return nil, err
	}

	// cache the membership events
	for _, membership := range memberships {
		lazyLoadCache.StoreLazyLoadedUser(device, frameID, *membership.StateKey(), membership.EventID())
	}

	return memberships, nil
}

func parseFrameEventFilter(req *http.Request) (*synctypes.FrameEventFilter, error) {
	// Default frame filter
	filter := &synctypes.FrameEventFilter{Limit: 10}

	l := req.URL.Query().Get("limit")
	f := req.URL.Query().Get("filter")
	if l != "" {
		limit, err := strconv.Atoi(l)
		if err != nil {
			return nil, err
		}
		// NOTSPEC: feels like a good idea to have an upper bound limit
		if limit > 100 {
			limit = 100
		}
		filter.Limit = limit
	}
	if f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			return nil, err
		}
	}

	return filter, nil
}
