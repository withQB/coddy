package routing

import (
	"context"
	"net/http"

	"github.com/withqb/coddy/services/clientapi/auth"
	"github.com/withqb/coddy/services/clientapi/userutil"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type loginResponse struct {
	UserID      string `json:"user_id"`
	AccessToken string `json:"access_token"`
	DeviceID    string `json:"device_id"`
}

type flows struct {
	Flows []flow `json:"flows"`
}

type flow struct {
	Type string `json:"type"`
}

func passwordLogin() flows {
	f := flows{}
	s := flow{
		Type: "m.login.password",
	}
	f.Flows = append(f.Flows, s)
	return f
}

// Login implements GET and POST /login
func Login(
	req *http.Request, userAPI userapi.ClientUserAPI,
	cfg *config.ClientAPI,
) xutil.JSONResponse {
	if req.Method == http.MethodGet {
		// TDO: support other forms of login other than password, depending on config options
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: passwordLogin(),
		}
	} else if req.Method == http.MethodPost {
		login, cleanup, authErr := auth.LoginFromJSONReader(req.Context(), req.Body, userAPI, userAPI, cfg)
		if authErr != nil {
			return *authErr
		}
		// make a device/access token
		authErr2 := completeAuth(req.Context(), cfg.Coddy, userAPI, login, req.RemoteAddr, req.UserAgent())
		cleanup(req.Context(), &authErr2)
		return authErr2
	}
	return xutil.JSONResponse{
		Code: http.StatusMethodNotAllowed,
		JSON: spec.NotFound("Bad method"),
	}
}

func completeAuth(
	ctx context.Context, cfg *config.Global, userAPI userapi.ClientUserAPI, login *auth.Login,
	ipAddr, userAgent string,
) xutil.JSONResponse {
	token, err := auth.GenerateAccessToken()
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("auth.GenerateAccessToken failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	localpart, serverName, err := userutil.ParseUsernameParam(login.Username(), cfg)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("auth.ParseUsernameParam failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var performRes userapi.PerformDeviceCreationResponse
	err = userAPI.PerformDeviceCreation(ctx, &userapi.PerformDeviceCreationRequest{
		DeviceDisplayName: login.InitialDisplayName,
		DeviceID:          login.DeviceID,
		AccessToken:       token,
		Localpart:         localpart,
		ServerName:        serverName,
		IPAddr:            ipAddr,
		UserAgent:         userAgent,
	}, &performRes)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("failed to create device: " + err.Error()),
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: loginResponse{
			UserID:      performRes.Device.UserID,
			AccessToken: performRes.Device.AccessToken,
			DeviceID:    performRes.Device.ID,
		},
	}
}
