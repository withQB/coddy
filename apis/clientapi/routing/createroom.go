package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/withqb/coddy/apis/userapi/api"
	roomserverAPI "github.com/withqb/coddy/servers/roomserver/api"
	roomserverVersion "github.com/withqb/coddy/servers/roomserver/version"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/xtools/spec"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/clientapi/httputil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xutil"
)

// https://matrix.org/docs/spec/client_server/r0.2.0.html#post-matrix-client-r0-createroom
type createRoomRequest struct {
	Invite                    []string                `json:"invite"`
	Name                      string                  `json:"name"`
	Visibility                string                  `json:"visibility"`
	Topic                     string                  `json:"topic"`
	Preset                    string                  `json:"preset"`
	CreationContent           json.RawMessage         `json:"creation_content"`
	InitialState              []xtools.FledglingEvent `json:"initial_state"`
	RoomAliasName             string                  `json:"room_alias_name"`
	RoomVersion               xtools.RoomVersion      `json:"room_version"`
	PowerLevelContentOverride json.RawMessage         `json:"power_level_content_override"`
	IsDirect                  bool                    `json:"is_direct"`
}

func (r createRoomRequest) Validate() *xutil.JSONResponse {
	whitespace := "\t\n\x0b\x0c\r " // https://docs.python.org/2/library/string.html#string.whitespace
	// https://github.com/withqb/synapse/blob/v0.19.2/synapse/handlers/room.py#L81
	// Synapse doesn't check for ':' but we will else it will break parsers badly which split things into 2 segments.
	if strings.ContainsAny(r.RoomAliasName, whitespace+":") {
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("room_alias_name cannot contain whitespace or ':'"),
		}
	}
	for _, userID := range r.Invite {
		if _, err := spec.NewUserID(userID, true); err != nil {
			return &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("user id must be in the form @localpart:domain"),
			}
		}
	}
	switch r.Preset {
	case spec.PresetPrivateChat, spec.PresetTrustedPrivateChat, spec.PresetPublicChat, "":
	default:
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("preset must be any of 'private_chat', 'trusted_private_chat', 'public_chat'"),
		}
	}

	// Validate creation_content fields defined in the spec by marshalling the
	// creation_content map into bytes and then unmarshalling the bytes into
	// eventutil.CreateContent.

	creationContentBytes, err := json.Marshal(r.CreationContent)
	if err != nil {
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("malformed creation_content"),
		}
	}

	var CreationContent xtools.CreateContent
	err = json.Unmarshal(creationContentBytes, &CreationContent)
	if err != nil {
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("malformed creation_content"),
		}
	}

	return nil
}

// https://matrix.org/docs/spec/client_server/r0.2.0.html#post-matrix-client-r0-createroom
type createRoomResponse struct {
	RoomID    string `json:"room_id"`
	RoomAlias string `json:"room_alias,omitempty"` // in synapse not spec
}

// CreateRoom implements /createRoom
func CreateRoom(
	req *http.Request, device *api.Device,
	cfg *config.ClientAPI,
	profileAPI api.ClientUserAPI, rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	var createRequest createRoomRequest
	resErr := httputil.UnmarshalJSONRequest(req, &createRequest)
	if resErr != nil {
		return *resErr
	}
	if resErr = createRequest.Validate(); resErr != nil {
		return *resErr
	}
	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}
	return createRoom(req.Context(), createRequest, device, cfg, profileAPI, rsAPI, asAPI, evTime)
}

// createRoom implements /createRoom
func createRoom(
	ctx context.Context,
	createRequest createRoomRequest, device *api.Device,
	cfg *config.ClientAPI,
	profileAPI api.ClientUserAPI, rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	evTime time.Time,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("invalid userID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !cfg.Matrix.IsLocalServerName(userID.Domain()) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("User domain %q not configured locally", userID.Domain())),
		}
	}

	logger := xutil.GetLogger(ctx)

	// TODO: Check room ID doesn't clash with an existing one, and we
	//       probably shouldn't be using pseudo-random strings, maybe GUIDs?
	roomID, err := spec.NewRoomID(fmt.Sprintf("!%s:%s", xutil.RandomString(16), userID.Domain()))
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("invalid roomID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Clobber keys: creator, room_version

	roomVersion := rsAPI.DefaultRoomVersion()
	if createRequest.RoomVersion != "" {
		candidateVersion := xtools.RoomVersion(createRequest.RoomVersion)
		_, roomVersionError := roomserverVersion.SupportedRoomVersion(candidateVersion)
		if roomVersionError != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.UnsupportedRoomVersion(roomVersionError.Error()),
			}
		}
		roomVersion = candidateVersion
	}

	logger.WithFields(log.Fields{
		"userID":      userID.String(),
		"roomID":      roomID.String(),
		"roomVersion": roomVersion,
	}).Info("Creating new room")

	profile, err := appserviceAPI.RetrieveUserProfile(ctx, userID.String(), asAPI, profileAPI)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("appserviceAPI.RetrieveUserProfile failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	userDisplayName := profile.DisplayName
	userAvatarURL := profile.AvatarURL

	keyID := cfg.Matrix.KeyID
	privateKey := cfg.Matrix.PrivateKey

	req := roomserverAPI.PerformCreateRoomRequest{
		InvitedUsers:              createRequest.Invite,
		RoomName:                  createRequest.Name,
		Visibility:                createRequest.Visibility,
		Topic:                     createRequest.Topic,
		StatePreset:               createRequest.Preset,
		CreationContent:           createRequest.CreationContent,
		InitialState:              createRequest.InitialState,
		RoomAliasName:             createRequest.RoomAliasName,
		RoomVersion:               roomVersion,
		PowerLevelContentOverride: createRequest.PowerLevelContentOverride,
		IsDirect:                  createRequest.IsDirect,

		UserDisplayName: userDisplayName,
		UserAvatarURL:   userAvatarURL,
		KeyID:           keyID,
		PrivateKey:      privateKey,
		EventTime:       evTime,
	}

	roomAlias, createRes := rsAPI.PerformCreateRoom(ctx, *userID, *roomID, &req)
	if createRes != nil {
		return *createRes
	}

	response := createRoomResponse{
		RoomID:    roomID.String(),
		RoomAlias: roomAlias,
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: response,
	}
}
