package routing

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// InviteV3 implements /_matrix/federation/v2/invite/{roomID}/{userID}
func InviteV3(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	invitedUser spec.UserID,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys xtools.JSONVerifier,
) xutil.JSONResponse {
	inviteReq := fclient.InviteV3Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	}
	if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	input := xtools.HandleInviteV3Input{
		HandleInviteInput: xtools.HandleInviteInput{
			RoomVersion:       inviteReq.RoomVersion(),
			RoomID:            roomID,
			InvitedUser:       invitedUser,
			KeyID:             cfg.Matrix.KeyID,
			PrivateKey:        cfg.Matrix.PrivateKey,
			Verifier:          keys,
			RoomQuerier:       rsAPI,
			MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
			StateQuerier:      rsAPI.StateQuerier(),
			InviteEvent:       nil,
			StrippedState:     inviteReq.InviteRoomState(),
			UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
			},
		},
		InviteProtoEvent: inviteReq.Event(),
		GetOrCreateSenderID: func(ctx context.Context, userID spec.UserID, roomID spec.RoomID, roomVersion string) (spec.SenderID, ed25519.PrivateKey, error) {
			// assign a roomNID, otherwise we can't create a private key for the user
			_, nidErr := rsAPI.AssignRoomNID(ctx, roomID, xtools.RoomVersion(roomVersion))
			if nidErr != nil {
				return "", nil, nidErr
			}
			key, keyErr := rsAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
			if keyErr != nil {
				return "", nil, keyErr
			}

			return spec.SenderIDFromPseudoIDKey(key), key, nil
		},
	}
	event, jsonErr := handleInviteV3(httpReq.Context(), input, rsAPI)
	if jsonErr != nil {
		return *jsonErr
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInviteV2{Event: event.JSON()},
	}
}

// InviteV2 implements /_matrix/federation/v2/invite/{roomID}/{eventID}
func InviteV2(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys xtools.JSONVerifier,
) xutil.JSONResponse {
	inviteReq := fclient.InviteV2Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	switch e := err.(type) {
	case xtools.UnsupportedRoomVersionError:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(
				fmt.Sprintf("Room version %q is not supported by this server.", e.Version),
			),
		}
	case xtools.BadJSONError:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case nil:
		if inviteReq.Event().StateKey() == nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("The invite event has no state key"),
			}
		}

		invitedUser, userErr := spec.NewUserID(*inviteReq.Event().StateKey(), true)
		if userErr != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("The user ID is invalid"),
			}
		}
		if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
			}
		}

		if inviteReq.Event().EventID() != eventID {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
			}
		}

		input := xtools.HandleInviteInput{
			RoomVersion:       inviteReq.RoomVersion(),
			RoomID:            roomID,
			InvitedUser:       *invitedUser,
			KeyID:             cfg.Matrix.KeyID,
			PrivateKey:        cfg.Matrix.PrivateKey,
			Verifier:          keys,
			RoomQuerier:       rsAPI,
			MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
			StateQuerier:      rsAPI.StateQuerier(),
			InviteEvent:       inviteReq.Event(),
			StrippedState:     inviteReq.InviteRoomState(),
			UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
			},
		}
		event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI)
		if jsonErr != nil {
			return *jsonErr
		}
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: fclient.RespInviteV2{Event: event.JSON()},
		}
	default:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into an invite request. " + err.Error()),
		}
	}
}

// InviteV1 implements /_matrix/federation/v1/invite/{roomID}/{eventID}
func InviteV1(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys xtools.JSONVerifier,
) xutil.JSONResponse {
	roomVer := xtools.RoomVersionV1
	body := request.Content()
	// roomVer is hardcoded to v1 so we know we won't panic on Must
	event, err := xtools.MustGetRoomVersion(roomVer).NewEventFromTrustedJSON(body, false)
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
			JSON: spec.NotJSON("The request body could not be decoded into an invite v1 request. " + err.Error()),
		}
	}
	var strippedState []xtools.InviteStrippedState
	if jsonErr := json.Unmarshal(event.Unsigned(), &strippedState); jsonErr != nil {
		// just warn, they may not have added any.
		xutil.GetLogger(httpReq.Context()).Warnf("failed to extract stripped state from invite event")
	}

	if event.StateKey() == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The invite event has no state key"),
		}
	}

	invitedUser, err := spec.NewUserID(*event.StateKey(), true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID is invalid"),
		}
	}
	if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	if event.EventID() != eventID {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
		}
	}

	input := xtools.HandleInviteInput{
		RoomVersion:       roomVer,
		RoomID:            roomID,
		InvitedUser:       *invitedUser,
		KeyID:             cfg.Matrix.KeyID,
		PrivateKey:        cfg.Matrix.PrivateKey,
		Verifier:          keys,
		RoomQuerier:       rsAPI,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
		StateQuerier:      rsAPI.StateQuerier(),
		InviteEvent:       event,
		StrippedState:     strippedState,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}
	event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI)
	if jsonErr != nil {
		return *jsonErr
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInvite{Event: event.JSON()},
	}
}

func handleInvite(ctx context.Context, input xtools.HandleInviteInput, rsAPI api.FederationRoomserverAPI) (xtools.PDU, *xutil.JSONResponse) {
	inviteEvent, err := xtools.HandleInvite(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI)
}

func handleInviteV3(ctx context.Context, input xtools.HandleInviteV3Input, rsAPI api.FederationRoomserverAPI) (xtools.PDU, *xutil.JSONResponse) {
	inviteEvent, err := xtools.HandleInviteV3(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI)
}

func handleInviteResult(ctx context.Context, inviteEvent xtools.PDU, err error, rsAPI api.FederationRoomserverAPI) (xtools.PDU, *xutil.JSONResponse) {
	switch e := err.(type) {
	case nil:
	case spec.InternalServerError:
		xutil.GetLogger(ctx).WithError(err)
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.MatrixError:
		xutil.GetLogger(ctx).WithError(err)
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorUnsupportedRoomVersion:
			fallthrough // http.StatusBadRequest
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return nil, &xutil.JSONResponse{
			Code: code,
			JSON: e,
		}
	default:
		xutil.GetLogger(ctx).WithError(err)
		return nil, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	headeredInvite := &types.HeaderedEvent{PDU: inviteEvent}
	if err = rsAPI.HandleInvite(ctx, headeredInvite); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("HandleInvite failed")
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return inviteEvent, nil

}
