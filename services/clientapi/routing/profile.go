package routing

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/internal/eventutil"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/clientapi/auth/authtypes"
	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/types"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xcore"
	"github.com/withqb/xutil"
)

// GetProfile implements GET /profile/{userID}
func GetProfile(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) xutil.JSONResponse {
	profile, err := getProfile(req.Context(), profileAPI, cfg, userID, asAPI, federation)
	if err != nil {
		if err == appserviceAPI.ErrProfileNotExists {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("The user does not exist or does not have a profile"),
			}
		}

		xutil.GetLogger(req.Context()).WithError(err).Error("getProfile failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: eventutil.UserProfile{
			AvatarURL:   profile.AvatarURL,
			DisplayName: profile.DisplayName,
		},
	}
}

// GetAvatarURL implements GET /profile/{userID}/avatar_url
func GetAvatarURL(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string, asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) xutil.JSONResponse {
	profile := GetProfile(req, profileAPI, cfg, userID, asAPI, federation)
	p, ok := profile.JSON.(eventutil.UserProfile)
	// not a profile response, so most likely an error, return that
	if !ok {
		return profile
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: eventutil.UserProfile{
			AvatarURL: p.AvatarURL,
		},
	}
}

// SetAvatarURL implements PUT /profile/{userID}/avatar_url
func SetAvatarURL(
	req *http.Request, profileAPI userapi.ProfileAPI,
	device *userapi.Device, userID string, cfg *config.ClientAPI, rsAPI api.ClientDataframeAPI,
) xutil.JSONResponse {
	if userID != device.UserID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	var r eventutil.UserProfile
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}

	localpart, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !cfg.Coddy.IsLocalServerName(domain) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not belong to a locally configured domain"),
		}
	}

	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	profile, changed, err := profileAPI.SetAvatarURL(req.Context(), localpart, domain, r.AvatarURL)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("profileAPI.SetAvatarURL failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	// No need to build new membership events, since nothing changed
	if !changed {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	response, err := updateProfile(req.Context(), rsAPI, device, profile, userID, evTime)
	if err != nil {
		return response
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// GetDisplayName implements GET /profile/{userID}/displayname
func GetDisplayName(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string, asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) xutil.JSONResponse {
	profile := GetProfile(req, profileAPI, cfg, userID, asAPI, federation)
	p, ok := profile.JSON.(eventutil.UserProfile)
	// not a profile response, so most likely an error, return that
	if !ok {
		return profile
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: eventutil.UserProfile{
			DisplayName: p.DisplayName,
		},
	}
}

// SetDisplayName implements PUT /profile/{userID}/displayname
func SetDisplayName(
	req *http.Request, profileAPI userapi.ProfileAPI,
	device *userapi.Device, userID string, cfg *config.ClientAPI, rsAPI api.ClientDataframeAPI,
) xutil.JSONResponse {
	if userID != device.UserID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	var r eventutil.UserProfile
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}

	localpart, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !cfg.Coddy.IsLocalServerName(domain) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not belong to a locally configured domain"),
		}
	}

	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	profile, changed, err := profileAPI.SetDisplayName(req.Context(), localpart, domain, r.DisplayName)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("profileAPI.SetDisplayName failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	// No need to build new membership events, since nothing changed
	if !changed {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	response, err := updateProfile(req.Context(), rsAPI, device, profile, userID, evTime)
	if err != nil {
		return response
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

func updateProfile(
	ctx context.Context, rsAPI api.ClientDataframeAPI, device *userapi.Device,
	profile *authtypes.Profile,
	userID string, evTime time.Time,
) (xutil.JSONResponse, error) {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}, err
	}

	frames, err := rsAPI.QueryFramesForUser(ctx, *deviceUserID, "join")
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryFramesForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	frameIDStrs := make([]string, len(frames))
	for i, frame := range frames {
		frameIDStrs[i] = frame.String()
	}

	_, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	events, err := buildMembershipEvents(
		ctx, frameIDStrs, *profile, userID, evTime, rsAPI,
	)
	switch e := err.(type) {
	case nil:
	case xtools.BadJSONError:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(e.Error()),
		}, e
	default:
		xutil.GetLogger(ctx).WithError(err).Error("buildMembershipEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, e
	}

	if err := api.SendEvents(ctx, rsAPI, api.KindNew, events, device.UserDomain(), domain, domain, nil, true); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("SendEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}
	return xutil.JSONResponse{}, nil
}

// getProfile gets the full profile of a user by querying the database or a
// remote homeserver.
// Returns an error when something goes wrong or specifically
// eventutil.ErrProfileNotExists when the profile doesn't exist.
func getProfile(
	ctx context.Context, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) (*authtypes.Profile, error) {
	localpart, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		return nil, err
	}

	if !cfg.Coddy.IsLocalServerName(domain) {
		profile, fedErr := federation.LookupProfile(ctx, cfg.Coddy.ServerName, domain, userID, "")
		if fedErr != nil {
			if x, ok := fedErr.(xcore.HTTPError); ok {
				if x.Code == http.StatusNotFound {
					return nil, appserviceAPI.ErrProfileNotExists
				}
			}

			return nil, fedErr
		}

		return &authtypes.Profile{
			Localpart:   localpart,
			DisplayName: profile.DisplayName,
			AvatarURL:   profile.AvatarURL,
		}, nil
	}

	profile, err := appserviceAPI.RetrieveUserProfile(ctx, userID, asAPI, profileAPI)
	if err != nil {
		return nil, err
	}

	return profile, nil
}

func buildMembershipEvents(
	ctx context.Context,
	frameIDs []string,
	newProfile authtypes.Profile, userID string,
	evTime time.Time, rsAPI api.ClientDataframeAPI,
) ([]*types.HeaderedEvent, error) {
	evs := []*types.HeaderedEvent{}

	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return nil, err
	}
	for _, frameID := range frameIDs {
		validFrameID, err := spec.NewFrameID(frameID)
		if err != nil {
			return nil, err
		}
		senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validFrameID, *fullUserID)
		if err != nil {
			return nil, err
		} else if senderID == nil {
			return nil, fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validFrameID)
		}
		senderIDString := string(*senderID)
		proto := xtools.ProtoEvent{
			SenderID: senderIDString,
			FrameID:   frameID,
			Type:     "m.frame.member",
			StateKey: &senderIDString,
		}

		content := xtools.MemberContent{
			Membership: spec.Join,
		}

		content.DisplayName = newProfile.DisplayName
		content.AvatarURL = newProfile.AvatarURL

		if err = proto.SetContent(content); err != nil {
			return nil, err
		}

		user, err := spec.NewUserID(userID, true)
		if err != nil {
			return nil, err
		}

		identity, err := rsAPI.SigningIdentityFor(ctx, *validFrameID, *user)
		if err != nil {
			return nil, err
		}

		event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, evTime, rsAPI, nil)
		if err != nil {
			return nil, err
		}

		evs = append(evs, event)
	}

	return evs, nil
}
