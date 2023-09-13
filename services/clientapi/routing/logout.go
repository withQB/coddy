package routing

import (
	"net/http"

	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// Logout handles POST /logout
func Logout(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
) xutil.JSONResponse {
	var performRes api.PerformDeviceDeletionResponse
	err := userAPI.PerformDeviceDeletion(req.Context(), &api.PerformDeviceDeletionRequest{
		UserID:    device.UserID,
		DeviceIDs: []string{device.ID},
	}, &performRes)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("PerformDeviceDeletion failed")
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

// LogoutAll handles POST /logout/all
func LogoutAll(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
) xutil.JSONResponse {
	var performRes api.PerformDeviceDeletionResponse
	err := userAPI.PerformDeviceDeletion(req.Context(), &api.PerformDeviceDeletionRequest{
		UserID:    device.UserID,
		DeviceIDs: nil,
	}, &performRes)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("PerformDeviceDeletion failed")
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
