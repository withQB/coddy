package routing

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"time"

	"github.com/getsentry/sentry-go"
	appserviceAPI "github.com/withqb/coddy/appservice/api"
	"github.com/withqb/coddy/clientapi/auth/authtypes"
	"github.com/withqb/coddy/clientapi/httputil"
	"github.com/withqb/coddy/clientapi/threepid"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/roomserver/api"
	roomserverAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	userapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/xutil"
)

func SendBan(
	req *http.Request, profileAPI userapi.ClientUserAPI, device *userapi.Device,
	roomID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	body, evTime, reqErr := extractRequestData(req)
	if reqErr != nil {
		return *reqErr
	}

	if body.UserID == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing user_id"),
		}
	}

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to ban this user, bad userID"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *deviceUserID)
	if err != nil || senderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to ban this user, unknown senderID"),
		}
	}

	errRes := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if errRes != nil {
		return *errRes
	}

	pl, errRes := getPowerlevels(req, rsAPI, roomID)
	if errRes != nil {
		return *errRes
	}
	allowedToBan := pl.UserLevel(*senderID) >= pl.Ban
	if !allowedToBan {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to ban this user, power level too low."),
		}
	}

	return sendMembership(req.Context(), profileAPI, device, roomID, spec.Ban, body.Reason, cfg, body.UserID, evTime, rsAPI, asAPI)
}

func sendMembership(ctx context.Context, profileAPI userapi.ClientUserAPI, device *userapi.Device,
	roomID, membership, reason string, cfg *config.ClientAPI, targetUserID string, evTime time.Time,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI) xutil.JSONResponse {

	event, err := buildMembershipEvent(
		ctx, targetUserID, reason, profileAPI, device, membership,
		roomID, false, cfg, evTime, rsAPI, asAPI,
	)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("buildMembershipEvent failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	serverName := device.UserDomain()
	if err = roomserverAPI.SendEvents(
		ctx, rsAPI,
		roomserverAPI.KindNew,
		[]*types.HeaderedEvent{event},
		device.UserDomain(),
		serverName,
		serverName,
		nil,
		false,
	); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("SendEvents failed")
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

func SendKick(
	req *http.Request, profileAPI userapi.ClientUserAPI, device *userapi.Device,
	roomID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	body, evTime, reqErr := extractRequestData(req)
	if reqErr != nil {
		return *reqErr
	}
	if body.UserID == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing user_id"),
		}
	}

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, bad userID"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *deviceUserID)
	if err != nil || senderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, unknown senderID"),
		}
	}

	errRes := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if errRes != nil {
		return *errRes
	}

	pl, errRes := getPowerlevels(req, rsAPI, roomID)
	if errRes != nil {
		return *errRes
	}
	allowedToKick := pl.UserLevel(*senderID) >= pl.Kick
	if !allowedToKick {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, power level too low."),
		}
	}

	bodyUserID, err := spec.NewUserID(body.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("body userID is invalid"),
		}
	}
	var queryRes roomserverAPI.QueryMembershipForUserResponse
	err = rsAPI.QueryMembershipForUser(req.Context(), &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *bodyUserID,
	}, &queryRes)
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	// kick is only valid if the user is not currently banned or left (that is, they are joined or invited)
	if queryRes.Membership != spec.Join && queryRes.Membership != spec.Invite {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Unknown("cannot /kick banned or left users"),
		}
	}
	// TODO: should we be using SendLeave instead?
	return sendMembership(req.Context(), profileAPI, device, roomID, spec.Leave, body.Reason, cfg, body.UserID, evTime, rsAPI, asAPI)
}

func SendUnban(
	req *http.Request, profileAPI userapi.ClientUserAPI, device *userapi.Device,
	roomID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	body, evTime, reqErr := extractRequestData(req)
	if reqErr != nil {
		return *reqErr
	}
	if body.UserID == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing user_id"),
		}
	}

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, bad userID"),
		}
	}

	errRes := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if errRes != nil {
		return *errRes
	}

	bodyUserID, err := spec.NewUserID(body.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("body userID is invalid"),
		}
	}
	var queryRes roomserverAPI.QueryMembershipForUserResponse
	err = rsAPI.QueryMembershipForUser(req.Context(), &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *bodyUserID,
	}, &queryRes)
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	// unban is only valid if the user is currently banned
	if queryRes.Membership != spec.Ban {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("can only /unban users that are banned"),
		}
	}
	// TODO: should we be using SendLeave instead?
	return sendMembership(req.Context(), profileAPI, device, roomID, spec.Leave, body.Reason, cfg, body.UserID, evTime, rsAPI, asAPI)
}

func SendInvite(
	req *http.Request, profileAPI userapi.ClientUserAPI, device *userapi.Device,
	roomID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	body, evTime, reqErr := extractRequestData(req)
	if reqErr != nil {
		return *reqErr
	}

	inviteStored, jsonErrResp := checkAndProcessThreepid(
		req, device, body, cfg, rsAPI, profileAPI, roomID, evTime,
	)
	if jsonErrResp != nil {
		return *jsonErrResp
	}

	// If an invite has been stored on an identity server, it means that a
	// m.room.third_party_invite event has been emitted and that we shouldn't
	// emit a m.room.member one.
	if inviteStored {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	if body.UserID == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing user_id"),
		}
	}

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, bad userID"),
		}
	}

	errRes := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if errRes != nil {
		return *errRes
	}

	// We already received the return value, so no need to check for an error here.
	response, _ := sendInvite(req.Context(), profileAPI, device, roomID, body.UserID, body.Reason, cfg, rsAPI, asAPI, evTime)
	return response
}

// sendInvite sends an invitation to a user. Returns a JSONResponse and an error
func sendInvite(
	ctx context.Context,
	profileAPI userapi.ClientUserAPI,
	device *userapi.Device,
	roomID, userID, reason string,
	cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI, evTime time.Time,
) (xutil.JSONResponse, error) {
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("RoomID is invalid"),
		}, err
	}
	inviter, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}
	invitee, err := spec.NewUserID(userID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("UserID is invalid"),
		}, err
	}
	profile, err := loadProfile(ctx, userID, cfg, profileAPI, asAPI)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}
	identity, err := cfg.Matrix.SigningIdentityFor(device.UserDomain())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}
	err = rsAPI.PerformInvite(ctx, &api.PerformInviteRequest{
		InviteInput: roomserverAPI.InviteInput{
			RoomID:      *validRoomID,
			Inviter:     *inviter,
			Invitee:     *invitee,
			DisplayName: profile.DisplayName,
			AvatarURL:   profile.AvatarURL,
			Reason:      reason,
			IsDirect:    false,
			KeyID:       identity.KeyID,
			PrivateKey:  identity.PrivateKey,
			EventTime:   evTime,
		},
		InviteRoomState: nil, // ask the roomserver to draw up invite room state for us
		SendAsServer:    string(device.UserDomain()),
	})

	switch e := err.(type) {
	case roomserverAPI.ErrInvalidID:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}, e
	case roomserverAPI.ErrNotAllowed:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}, e
	case nil:
	default:
		xutil.GetLogger(ctx).WithError(err).Error("PerformInvite failed")
		sentry.CaptureException(err)
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}, nil
}

func buildMembershipEventDirect(
	ctx context.Context,
	targetSenderID spec.SenderID, reason string, userDisplayName, userAvatarURL string,
	sender spec.SenderID, senderDomain spec.ServerName,
	membership, roomID string, isDirect bool,
	keyID xtools.KeyID, privateKey ed25519.PrivateKey, evTime time.Time,
	rsAPI roomserverAPI.ClientRoomserverAPI,
) (*types.HeaderedEvent, error) {
	targetSenderString := string(targetSenderID)
	proto := xtools.ProtoEvent{
		SenderID: string(sender),
		RoomID:   roomID,
		Type:     "m.room.member",
		StateKey: &targetSenderString,
	}

	content := xtools.MemberContent{
		Membership:  membership,
		DisplayName: userDisplayName,
		AvatarURL:   userAvatarURL,
		Reason:      reason,
		IsDirect:    isDirect,
	}

	if err := proto.SetContent(content); err != nil {
		return nil, err
	}

	identity := &fclient.SigningIdentity{
		ServerName: senderDomain,
		KeyID:      keyID,
		PrivateKey: privateKey,
	}
	return eventutil.QueryAndBuildEvent(ctx, &proto, identity, evTime, rsAPI, nil)
}

func buildMembershipEvent(
	ctx context.Context,
	targetUserID, reason string, profileAPI userapi.ClientUserAPI,
	device *userapi.Device,
	membership, roomID string, isDirect bool,
	cfg *config.ClientAPI, evTime time.Time,
	rsAPI roomserverAPI.ClientRoomserverAPI, asAPI appserviceAPI.AppServiceInternalAPI,
) (*types.HeaderedEvent, error) {
	profile, err := loadProfile(ctx, targetUserID, cfg, profileAPI, asAPI)
	if err != nil {
		return nil, err
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return nil, err
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, err
	}
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *userID)
	if err != nil {
		return nil, err
	} else if senderID == nil {
		return nil, fmt.Errorf("no sender ID for %s in %s", *userID, *validRoomID)
	}

	targetID, err := spec.NewUserID(targetUserID, true)
	if err != nil {
		return nil, err
	}
	targetSenderID, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *targetID)
	if err != nil {
		return nil, err
	} else if targetSenderID == nil {
		return nil, fmt.Errorf("no sender ID for %s in %s", *targetID, *validRoomID)
	}

	identity, err := rsAPI.SigningIdentityFor(ctx, *validRoomID, *userID)
	if err != nil {
		return nil, err
	}

	return buildMembershipEventDirect(ctx, *targetSenderID, reason, profile.DisplayName, profile.AvatarURL,
		*senderID, device.UserDomain(), membership, roomID, isDirect, identity.KeyID, identity.PrivateKey, evTime, rsAPI)
}

// loadProfile lookups the profile of a given user from the database and returns
// it if the user is local to this server, or returns an empty profile if not.
// Returns an error if the retrieval failed or if the first parameter isn't a
// valid Matrix ID.
func loadProfile(
	ctx context.Context,
	userID string,
	cfg *config.ClientAPI,
	profileAPI userapi.ClientUserAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) (*authtypes.Profile, error) {
	_, serverName, err := xtools.SplitID('@', userID)
	if err != nil {
		return nil, err
	}

	var profile *authtypes.Profile
	if cfg.Matrix.IsLocalServerName(serverName) {
		profile, err = appserviceAPI.RetrieveUserProfile(ctx, userID, asAPI, profileAPI)
	} else {
		profile = &authtypes.Profile{}
	}

	return profile, err
}

func extractRequestData(req *http.Request) (body *threepid.MembershipRequest, evTime time.Time, resErr *xutil.JSONResponse) {

	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		resErr = reqErr
		return
	}

	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		resErr = &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
		return
	}
	return
}

func checkAndProcessThreepid(
	req *http.Request,
	device *userapi.Device,
	body *threepid.MembershipRequest,
	cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	profileAPI userapi.ClientUserAPI,
	roomID string,
	evTime time.Time,
) (inviteStored bool, errRes *xutil.JSONResponse) {

	inviteStored, err := threepid.CheckAndProcessInvite(
		req.Context(), device, body, cfg, rsAPI, profileAPI,
		roomID, evTime,
	)
	switch e := err.(type) {
	case nil:
	case threepid.ErrMissingParameter:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAndProcessInvite failed")
		return inviteStored, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case threepid.ErrNotTrusted:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAndProcessInvite failed")
		return inviteStored, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotTrusted(body.IDServer),
		}
	case eventutil.ErrRoomNoExists:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAndProcessInvite failed")
		return inviteStored, &xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(err.Error()),
		}
	case xtools.BadJSONError:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAndProcessInvite failed")
		return inviteStored, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(e.Error()),
		}
	default:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAndProcessInvite failed")
		return inviteStored, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return
}

func checkMemberInRoom(ctx context.Context, rsAPI roomserverAPI.ClientRoomserverAPI, userID spec.UserID, roomID string) *xutil.JSONResponse {
	var membershipRes roomserverAPI.QueryMembershipForUserResponse
	err := rsAPI.QueryMembershipForUser(ctx, &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: userID,
	}, &membershipRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryMembershipForUser: could not query membership for user")
		return &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipRes.IsInRoom {
		return &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("user does not belong to room"),
		}
	}
	return nil
}

func SendForget(
	req *http.Request, device *userapi.Device,
	roomID string, rsAPI roomserverAPI.ClientRoomserverAPI,
) xutil.JSONResponse {
	ctx := req.Context()
	logger := xutil.GetLogger(ctx).WithField("roomID", roomID).WithField("userID", device.UserID)

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to kick this user, bad userID"),
		}
	}

	var membershipRes roomserverAPI.QueryMembershipForUserResponse
	membershipReq := roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *deviceUserID,
	}
	err = rsAPI.QueryMembershipForUser(ctx, &membershipReq, &membershipRes)
	if err != nil {
		logger.WithError(err).Error("QueryMembershipForUser: could not query membership for user")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipRes.RoomExists {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("room does not exist"),
		}
	}
	if membershipRes.IsInRoom {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(fmt.Sprintf("User %s is in room %s", device.UserID, roomID)),
		}
	}

	request := roomserverAPI.PerformForgetRequest{
		RoomID: roomID,
		UserID: device.UserID,
	}
	response := roomserverAPI.PerformForgetResponse{}
	if err := rsAPI.PerformForget(ctx, &request, &response); err != nil {
		logger.WithError(err).Error("PerformForget: unable to forget room")
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

func getPowerlevels(req *http.Request, rsAPI roomserverAPI.ClientRoomserverAPI, roomID string) (*xtools.PowerLevelContent, *xutil.JSONResponse) {
	plEvent := roomserverAPI.GetStateEvent(req.Context(), rsAPI, roomID, xtools.StateKeyTuple{
		EventType: spec.MRoomPowerLevels,
		StateKey:  "",
	})
	if plEvent == nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to perform this action, no power_levels event in this room."),
		}
	}
	pl, err := plEvent.PowerLevels()
	if err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You don't have permission to perform this action, the power_levels event for this room is malformed so auth checks cannot be performed."),
		}
	}
	return pl, nil
}
