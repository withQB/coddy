package routing

import (
	"io"
	"net/http"

	"github.com/withqb/coddy/clientapi/auth"
	"github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// Deactivate handles POST requests to /account/deactivate
func Deactivate(
	req *http.Request,
	userInteractiveAuth *auth.UserInteractive,
	accountAPI api.ClientUserAPI,
	deviceAPI *api.Device,
) xutil.JSONResponse {
	ctx := req.Context()
	defer req.Body.Close() // nolint:errcheck
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be read: " + err.Error()),
		}
	}

	login, errRes := userInteractiveAuth.Verify(ctx, bodyBytes, deviceAPI)
	if errRes != nil {
		return *errRes
	}

	localpart, serverName, err := xtools.SplitID('@', login.Username())
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var res api.PerformAccountDeactivationResponse
	err = accountAPI.PerformAccountDeactivation(ctx, &api.PerformAccountDeactivationRequest{
		Localpart:  localpart,
		ServerName: serverName,
	}, &res)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("userAPI.PerformAccountDeactivation failed")
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
