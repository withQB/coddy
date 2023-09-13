package routing

import (
	"net/http"

	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

func LeaveFrameByID(
	req *http.Request,
	device *api.Device,
	rsAPI dataframeAPI.ClientDataframeAPI,
	frameID string,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("device userID is invalid"),
		}
	}

	// Prepare to ask the dataframe to perform the frame join.
	leaveReq := dataframeAPI.PerformLeaveRequest{
		FrameID: frameID,
		Leaver: *userID,
	}
	leaveRes := dataframeAPI.PerformLeaveResponse{}

	// Ask the dataframe to perform the leave.
	if err := rsAPI.PerformLeave(req.Context(), &leaveReq, &leaveRes); err != nil {
		if leaveRes.Code != 0 {
			return xutil.JSONResponse{
				Code: leaveRes.Code,
				JSON: spec.LeaveServerNoticeError(),
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(err.Error()),
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
