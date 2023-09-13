package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/withqb/coddy/apis/userapi/api"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	dataframeVersion "github.com/withqb/coddy/servers/dataframe/version"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/xtools/spec"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/clientapi/httputil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xutil"
)

type createFrameRequest struct {
	Invite                    []string                `json:"invite"`
	Name                      string                  `json:"name"`
	Visibility                string                  `json:"visibility"`
	Topic                     string                  `json:"topic"`
	Preset                    string                  `json:"preset"`
	CreationContent           json.RawMessage         `json:"creation_content"`
	InitialState              []xtools.FledglingEvent `json:"initial_state"`
	FrameAliasName             string                  `json:"frame_alias_name"`
	FrameVersion               xtools.FrameVersion      `json:"frame_version"`
	PowerLevelContentOverride json.RawMessage         `json:"power_level_content_override"`
	IsDirect                  bool                    `json:"is_direct"`
}

func (r createFrameRequest) Validate() *xutil.JSONResponse {
	whitespace := "\t\n\x0b\x0c\r " // https://docs.python.org/2/library/string.html#string.whitespace
	// https://github.com/withqb/synapse/blob/v0.19.2/synapse/handlers/frame.py#L81
	// Synapse doesn't check for ':' but we will else it will break parsers badly which split things into 2 segments.
	if strings.ContainsAny(r.FrameAliasName, whitespace+":") {
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("frame_alias_name cannot contain whitespace or ':'"),
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

type createFrameResponse struct {
	FrameID    string `json:"frame_id"`
	FrameAlias string `json:"frame_alias,omitempty"` // in synapse not spec
}

// CreateFrame implements /createFrame
func CreateFrame(
	req *http.Request, device *api.Device,
	cfg *config.ClientAPI,
	profileAPI api.ClientUserAPI, rsAPI dataframeAPI.ClientDataframeAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	var createRequest createFrameRequest
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
	return createFrame(req.Context(), createRequest, device, cfg, profileAPI, rsAPI, asAPI, evTime)
}

// createFrame implements /createFrame
func createFrame(
	ctx context.Context,
	createRequest createFrameRequest, device *api.Device,
	cfg *config.ClientAPI,
	profileAPI api.ClientUserAPI, rsAPI dataframeAPI.ClientDataframeAPI,
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

	// TODO: Check frame ID doesn't clash with an existing one, and we
	//       probably shouldn't be using pseudo-random strings, maybe GUIDs?
	frameID, err := spec.NewFrameID(fmt.Sprintf("!%s:%s", xutil.RandomString(16), userID.Domain()))
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("invalid frameID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Clobber keys: creator, frame_version

	frameVersion := rsAPI.DefaultFrameVersion()
	if createRequest.FrameVersion != "" {
		candidateVersion := xtools.FrameVersion(createRequest.FrameVersion)
		_, frameVersionError := dataframeVersion.SupportedFrameVersion(candidateVersion)
		if frameVersionError != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.UnsupportedFrameVersion(frameVersionError.Error()),
			}
		}
		frameVersion = candidateVersion
	}

	logger.WithFields(log.Fields{
		"userID":      userID.String(),
		"frameID":      frameID.String(),
		"frameVersion": frameVersion,
	}).Info("Creating new frame")

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

	req := dataframeAPI.PerformCreateFrameRequest{
		InvitedUsers:              createRequest.Invite,
		FrameName:                  createRequest.Name,
		Visibility:                createRequest.Visibility,
		Topic:                     createRequest.Topic,
		StatePreset:               createRequest.Preset,
		CreationContent:           createRequest.CreationContent,
		InitialState:              createRequest.InitialState,
		FrameAliasName:             createRequest.FrameAliasName,
		FrameVersion:               frameVersion,
		PowerLevelContentOverride: createRequest.PowerLevelContentOverride,
		IsDirect:                  createRequest.IsDirect,

		UserDisplayName: userDisplayName,
		UserAvatarURL:   userAvatarURL,
		KeyID:           keyID,
		PrivateKey:      privateKey,
		EventTime:       evTime,
	}

	frameAlias, createRes := rsAPI.PerformCreateFrame(ctx, *userID, *frameID, &req)
	if createRes != nil {
		return *createRes
	}

	response := createFrameResponse{
		FrameID:    frameID.String(),
		FrameAlias: frameAlias,
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: response,
	}
}
