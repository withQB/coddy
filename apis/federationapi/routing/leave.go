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

package routing

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// MakeLeave implements the /make_leave API
func MakeLeave(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID spec.RoomID, userID spec.UserID,
) xutil.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID.String())
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("failed obtaining room version")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	req := api.QueryServerJoinedToRoomRequest{
		ServerName: request.Destination(),
		RoomID:     roomID.String(),
	}
	res := api.QueryServerJoinedToRoomResponse{}
	if err = rsAPI.QueryServerJoinedToRoom(httpReq.Context(), &req, &res); err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryServerJoinedToRoom failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	createLeaveTemplate := func(proto *xtools.ProtoEvent) (xtools.PDU, []xtools.PDU, error) {
		identity, signErr := cfg.Matrix.SigningIdentityFor(request.Destination())
		if signErr != nil {
			xutil.GetLogger(httpReq.Context()).WithError(signErr).Errorf("obtaining signing identity for %s failed", request.Destination())
			return nil, nil, spec.NotFound(fmt.Sprintf("Server name %q does not exist", request.Destination()))
		}

		queryRes := api.QueryLatestEventsAndStateResponse{}
		event, buildErr := eventutil.QueryAndBuildEvent(httpReq.Context(), proto, identity, time.Now(), rsAPI, &queryRes)
		switch e := buildErr.(type) {
		case nil:
		case eventutil.ErrRoomNoExists:
			xutil.GetLogger(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.NotFound("Room does not exist")
		case xtools.BadJSONError:
			xutil.GetLogger(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.BadJSON(e.Error())
		default:
			xutil.GetLogger(httpReq.Context()).WithError(buildErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.InternalServerError{}
		}

		stateEvents := make([]xtools.PDU, len(queryRes.StateEvents))
		for i, stateEvent := range queryRes.StateEvents {
			stateEvents[i] = stateEvent.PDU
		}
		return event, stateEvents, nil
	}

	senderID, err := rsAPI.QuerySenderIDForUser(httpReq.Context(), roomID, userID)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QuerySenderIDForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	} else if senderID == nil {
		xutil.GetLogger(httpReq.Context()).WithField("roomID", roomID).WithField("userID", userID).Error("rsAPI.QuerySenderIDForUser returned nil sender ID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	input := xtools.HandleMakeLeaveInput{
		UserID:             userID,
		SenderID:           *senderID,
		RoomID:             roomID,
		RoomVersion:        roomVersion,
		RequestOrigin:      request.Origin(),
		LocalServerName:    cfg.Matrix.ServerName,
		LocalServerInRoom:  res.RoomExists && res.IsInRoom,
		BuildEventTemplate: createLeaveTemplate,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}

	response, internalErr := xtools.HandleMakeLeave(input)
	switch e := internalErr.(type) {
	case nil:
	case spec.InternalServerError:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.MatrixError:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorNotFound:
			code = http.StatusNotFound
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return xutil.JSONResponse{
			Code: code,
			JSON: e,
		}
	default:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_leave request")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	if response == nil {
		xutil.GetLogger(httpReq.Context()).Error("gmsl.HandleMakeLeave returned invalid response")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]interface{}{
			"event":        response.LeaveTemplateEvent,
			"room_version": response.RoomVersion,
		},
	}
}

// SendLeave implements the /send_leave API
// nolint:gocyclo
func SendLeave(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys xtools.JSONVerifier,
	roomID, eventID string,
) xutil.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}

	verImpl, err := xtools.GetRoomVersion(roomVersion)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.UnsupportedRoomVersion(
				fmt.Sprintf("QueryRoomVersionForRoom returned unknown version: %s", roomVersion),
			),
		}
	}

	// Decode the event JSON from the request.
	event, err := verImpl.NewEventFromUntrustedJSON(request.Content())
	switch err.(type) {
	case xtools.BadJSONError:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case nil:
	default:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}

	// Check that the room ID is correct.
	if event.RoomID() != roomID {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The room ID in the request path must match the room ID in the leave event JSON"),
		}
	}

	// Check that the event ID is correct.
	if event.EventID() != eventID {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the leave event JSON"),
		}
	}

	if event.StateKey() == nil || event.StateKeyEquals("") {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("No state key was provided in the leave event."),
		}
	}
	if !event.StateKeyEquals(string(event.SenderID())) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Event state key must match the event sender."),
		}
	}

	// Check that the sender belongs to the server that is sending us
	// the request. By this point we've already asserted that the sender
	// and the state key are equal so we don't need to check both.
	validRoomID, err := spec.NewRoomID(event.RoomID())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Room ID is invalid."),
		}
	}
	sender, err := rsAPI.QueryUserIDForSender(httpReq.Context(), *validRoomID, event.SenderID())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The sender of the join is invalid"),
		}
	} else if sender.Domain() != request.Origin() {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The sender does not match the server that originated the request"),
		}
	}

	// Check if the user has already left. If so, no-op!
	queryReq := &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
		StateToFetch: []xtools.StateKeyTuple{
			{
				EventType: spec.MRoomMember,
				StateKey:  *event.StateKey(),
			},
		},
	}
	queryRes := &api.QueryLatestEventsAndStateResponse{}
	err = rsAPI.QueryLatestEventsAndState(httpReq.Context(), queryReq, queryRes)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryLatestEventsAndState failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	// The room doesn't exist or we weren't ever joined to it. Might as well
	// no-op here.
	if !queryRes.RoomExists || len(queryRes.StateEvents) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	// Check if we're recycling a previous leave event.
	if event.EventID() == queryRes.StateEvents[0].EventID() {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	// We are/were joined/invited/banned or something. Check if
	// we can no-op here.
	if len(queryRes.StateEvents) == 1 {
		if mem, merr := queryRes.StateEvents[0].Membership(); merr == nil && mem == spec.Leave {
			return xutil.JSONResponse{
				Code: http.StatusOK,
				JSON: struct{}{},
			}
		}
	}

	// Check that the event is signed by the server sending the request.
	redacted, err := verImpl.RedactEventJSON(event.JSON())
	if err != nil {
		logrus.WithError(err).Errorf("XXX: leave.go")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event JSON could not be redacted"),
		}
	}
	verifyRequests := []xtools.VerifyJSONRequest{{
		ServerName:           sender.Domain(),
		Message:              redacted,
		AtTS:                 event.OriginServerTS(),
		ValidityCheckingFunc: xtools.StrictValiditySignatureCheck,
	}}
	verifyResults, err := keys.VerifyJSONs(httpReq.Context(), verifyRequests)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("keys.VerifyJSONs failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if verifyResults[0].Error != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The leave must be signed by the server it originated on"),
		}
	}

	// check membership is set to leave
	mem, err := event.Membership()
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("event.Membership failed")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing content.membership key"),
		}
	}
	if mem != spec.Leave {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The membership in the event content must be set to leave"),
		}
	}

	// Send the events to the room server.
	// We are responsible for notifying other servers that the user has left
	// the room, so set SendAsServer to cfg.Matrix.ServerName
	var response api.InputRoomEventsResponse
	rsAPI.InputRoomEvents(httpReq.Context(), &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{
			{
				Kind:          api.KindNew,
				Event:         &types.HeaderedEvent{PDU: event},
				SendAsServer:  string(cfg.Matrix.ServerName),
				TransactionID: nil,
			},
		},
	}, &response)

	if response.ErrMsg != "" {
		xutil.GetLogger(httpReq.Context()).WithField(logrus.ErrorKey, response.ErrMsg).WithField("not_allowed", response.NotAllowed).Error("producer.SendEvents failed")
		if response.NotAllowed {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Forbidden(response.ErrMsg),
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
