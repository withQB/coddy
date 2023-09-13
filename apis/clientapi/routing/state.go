package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type stateEventInStateResp struct {
	synctypes.ClientEvent
	PrevContent   json.RawMessage `json:"prev_content,omitempty"`
	ReplacesState string          `json:"replaces_state,omitempty"`
}

// OnIncomingStateRequest is called when a client makes a /frames/{frameID}/state
// request. It will fetch all the state events from the specified frame and will
// append the necessary keys to them if applicable before returning them.
// Returns an error if something went wrong in the process.
// TODO: Check if the user is in the frame. If not, check if the frame's history
// is publicly visible. Current behaviour is returning an empty array if the
// user cannot see the frame's history.
func OnIncomingStateRequest(ctx context.Context, device *userapi.Device, rsAPI api.ClientDataframeAPI, frameID string) xutil.JSONResponse {
	var worldReadable bool
	var wantLatestState bool

	// First of all, get the latest state of the frame. We need to do this
	// so that we can look at the history visibility of the frame. If the
	// frame is world-readable then we will always return the latest state.
	stateRes := api.QueryLatestEventsAndStateResponse{}
	if err := rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{
		FrameID:       frameID,
		StateToFetch: []xtools.StateKeyTuple{},
	}, &stateRes); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("queryAPI.QueryLatestEventsAndState failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !stateRes.FrameExists {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("frame does not exist"),
		}
	}

	// Look at the frame state and see if we have a history visibility event
	// that marks the frame as world-readable. If we don't then we assume that
	// the frame is not world-readable.
	for _, ev := range stateRes.StateEvents {
		if ev.Type() == spec.MFrameHistoryVisibility {
			content := map[string]string{}
			if err := json.Unmarshal(ev.Content(), &content); err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("json.Unmarshal for history visibility failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			if visibility, ok := content["history_visibility"]; ok {
				worldReadable = visibility == "world_readable"
				break
			}
		}
	}

	// If the frame isn't world-readable then we will instead try to find out
	// the state of the frame based on the user's membership. If the user is
	// in the frame then we'll want the latest state. If the user has never
	// been in the frame and the frame isn't world-readable, then we won't
	// return any state. If the user was in the frame previously but is no
	// longer then we will return the state at the time that the user left.
	// membershipRes will only be populated if the frame is not world-readable.
	var membershipRes api.QueryMembershipForUserResponse
	if !worldReadable {
		// The frame isn't world-readable so try to work out based on the
		// user's membership if we want the latest state or not.
		userID, err := spec.NewUserID(device.UserID, true)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("UserID is invalid")
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("Device UserID is invalid"),
			}
		}
		err = rsAPI.QueryMembershipForUser(ctx, &api.QueryMembershipForUserRequest{
			FrameID: frameID,
			UserID: *userID,
		}, &membershipRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryMembershipForUser")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		// If the user has never been in the frame then stop at this point.
		// We won't tell the user about a frame they have never joined.
		if !membershipRes.HasBeenInFrame {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden(fmt.Sprintf("Unknown frame %q or user %q has never joined this frame", frameID, device.UserID)),
			}
		}
		// Otherwise, if the user has been in the frame, whether or not we
		// give them the latest state will depend on if they are *still* in
		// the frame.
		wantLatestState = membershipRes.IsInFrame
	} else {
		// The frame is world-readable so the user join state is irrelevant,
		// just get the latest frame state instead.
		wantLatestState = true
	}

	xutil.GetLogger(ctx).WithFields(log.Fields{
		"frameID":         frameID,
		"state_at_event": !wantLatestState,
	}).Info("Fetching all state")

	stateEvents := []synctypes.ClientEvent{}
	if wantLatestState {
		// If we are happy to use the latest state, either because the user is
		// still in the frame, or because the frame is world-readable, then just
		// use the result of the previous QueryLatestEventsAndState response
		// to find the state event, if provided.
		for _, ev := range stateRes.StateEvents {
			stateEvents = append(
				stateEvents,
				synctypes.ToClientEventDefault(func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
					return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
				}, ev),
			)
		}
	} else {
		// Otherwise, take the event ID of their leave event and work out what
		// the state of the frame was before that event.
		var stateAfterRes api.QueryStateAfterEventsResponse
		err := rsAPI.QueryStateAfterEvents(ctx, &api.QueryStateAfterEventsRequest{
			FrameID:       frameID,
			PrevEventIDs: []string{membershipRes.EventID},
			StateToFetch: []xtools.StateKeyTuple{},
		}, &stateAfterRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryMembershipForUser")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		for _, ev := range stateAfterRes.StateEvents {
			sender := spec.UserID{}
			evFrameID, err := spec.NewFrameID(ev.FrameID())
			if err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("Event frameID is invalid")
				continue
			}
			userID, err := rsAPI.QueryUserIDForSender(ctx, *evFrameID, ev.SenderID())
			if err == nil && userID != nil {
				sender = *userID
			}

			sk := ev.StateKey()
			if sk != nil && *sk != "" {
				skUserID, err := rsAPI.QueryUserIDForSender(ctx, *evFrameID, spec.SenderID(*ev.StateKey()))
				if err == nil && skUserID != nil {
					skString := skUserID.String()
					sk = &skString
				}
			}
			stateEvents = append(
				stateEvents,
				synctypes.ToClientEvent(ev, synctypes.FormatAll, sender.String(), sk, ev.Unsigned()),
			)
		}
	}

	// Return the results to the requestor.
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: stateEvents,
	}
}

// OnIncomingStateTypeRequest is called when a client makes a
// /frames/{frameID}/state/{type}/{statekey} request. It will look in current
// state to see if there is an event with that type and state key, if there
// is then (by default) we return the content, otherwise a 404.
// If eventFormat=true, sends the whole event else just the content.
func OnIncomingStateTypeRequest(
	ctx context.Context, device *userapi.Device, rsAPI api.ClientDataframeAPI,
	frameID, evType, stateKey string, eventFormat bool,
) xutil.JSONResponse {
	var worldReadable bool
	var wantLatestState bool

	frameVer, err := rsAPI.QueryFrameVersionForFrame(ctx, frameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("Unknown frame %q or user %q has never joined this frame", frameID, device.UserID)),
		}
	}

	// Translate user ID state keys to frame keys in pseudo ID frames
	if frameVer == xtools.FrameVersionPseudoIDs {
		parsedFrameID, err := spec.NewFrameID(frameID)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.InvalidParam("invalid frame ID"),
			}
		}
		newStateKey, err := synctypes.FromClientStateKey(*parsedFrameID, stateKey, func(frameID spec.FrameID, userID spec.UserID) (*spec.SenderID, error) {
			return rsAPI.QuerySenderIDForUser(ctx, frameID, userID)
		})
		if err != nil {
			// TODO: work out better logic for failure cases (e.g. sender ID not found)
			xutil.GetLogger(ctx).WithError(err).Error("synctypes.FromClientStateKey failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("internal server error"),
			}
		}
		stateKey = *newStateKey
	}

	// Always fetch visibility so that we can work out whether to show
	// the latest events or the last event from when the user was joined.
	// Then include the requested event type and state key, assuming it
	// isn't for the same.
	stateToFetch := []xtools.StateKeyTuple{
		{
			EventType: evType,
			StateKey:  stateKey,
		},
	}
	if evType != spec.MFrameHistoryVisibility && stateKey != "" {
		stateToFetch = append(stateToFetch, xtools.StateKeyTuple{
			EventType: spec.MFrameHistoryVisibility,
			StateKey:  "",
		})
	}

	// First of all, get the latest state of the frame. We need to do this
	// so that we can look at the history visibility of the frame. If the
	// frame is world-readable then we will always return the latest state.
	stateRes := api.QueryLatestEventsAndStateResponse{}
	if err := rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{
		FrameID:       frameID,
		StateToFetch: stateToFetch,
	}, &stateRes); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("queryAPI.QueryLatestEventsAndState failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Look at the frame state and see if we have a history visibility event
	// that marks the frame as world-readable. If we don't then we assume that
	// the frame is not world-readable.
	for _, ev := range stateRes.StateEvents {
		if ev.Type() == spec.MFrameHistoryVisibility {
			content := map[string]string{}
			if err := json.Unmarshal(ev.Content(), &content); err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("json.Unmarshal for history visibility failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			if visibility, ok := content["history_visibility"]; ok {
				worldReadable = visibility == "world_readable"
				break
			}
		}
	}

	// If the frame isn't world-readable then we will instead try to find out
	// the state of the frame based on the user's membership. If the user is
	// in the frame then we'll want the latest state. If the user has never
	// been in the frame and the frame isn't world-readable, then we won't
	// return any state. If the user was in the frame previously but is no
	// longer then we will return the state at the time that the user left.
	// membershipRes will only be populated if the frame is not world-readable.
	var membershipRes api.QueryMembershipForUserResponse
	if !worldReadable {
		userID, err := spec.NewUserID(device.UserID, true)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("UserID is invalid")
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("Device UserID is invalid"),
			}
		}
		// The frame isn't world-readable so try to work out based on the
		// user's membership if we want the latest state or not.
		err = rsAPI.QueryMembershipForUser(ctx, &api.QueryMembershipForUserRequest{
			FrameID: frameID,
			UserID: *userID,
		}, &membershipRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryMembershipForUser")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		// If the user has never been in the frame then stop at this point.
		// We won't tell the user about a frame they have never joined.
		if !membershipRes.HasBeenInFrame || membershipRes.Membership == spec.Ban {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden(fmt.Sprintf("Unknown frame %q or user %q has never joined this frame", frameID, device.UserID)),
			}
		}
		// Otherwise, if the user has been in the frame, whether or not we
		// give them the latest state will depend on if they are *still* in
		// the frame.
		wantLatestState = membershipRes.IsInFrame
	} else {
		// The frame is world-readable so the user join state is irrelevant,
		// just get the latest frame state instead.
		wantLatestState = true
	}

	xutil.GetLogger(ctx).WithFields(log.Fields{
		"frameID":         frameID,
		"evType":         evType,
		"stateKey":       stateKey,
		"state_at_event": !wantLatestState,
	}).Info("Fetching state")

	var event *types.HeaderedEvent
	if wantLatestState {
		// If we are happy to use the latest state, either because the user is
		// still in the frame, or because the frame is world-readable, then just
		// use the result of the previous QueryLatestEventsAndState response
		// to find the state event, if provided.
		for _, ev := range stateRes.StateEvents {
			if ev.Type() == evType && ev.StateKeyEquals(stateKey) {
				event = ev
				break
			}
		}
	} else {
		// Otherwise, take the event ID of their leave event and work out what
		// the state of the frame was before that event.
		var stateAfterRes api.QueryStateAfterEventsResponse
		err := rsAPI.QueryStateAfterEvents(ctx, &api.QueryStateAfterEventsRequest{
			FrameID:       frameID,
			PrevEventIDs: []string{membershipRes.EventID},
			StateToFetch: []xtools.StateKeyTuple{
				{
					EventType: evType,
					StateKey:  stateKey,
				},
			},
		}, &stateAfterRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryMembershipForUser")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if len(stateAfterRes.StateEvents) > 0 {
			event = stateAfterRes.StateEvents[0]
		}
	}

	// If there was no event found that matches all of the above criteria then
	// return an error.
	if event == nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("Cannot find state event for %q", evType)),
		}
	}

	stateEvent := stateEventInStateResp{
		ClientEvent: synctypes.ToClientEventDefault(func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		}, event),
	}

	var res interface{}
	if eventFormat {
		res = stateEvent
	} else {
		res = stateEvent.Content
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}
