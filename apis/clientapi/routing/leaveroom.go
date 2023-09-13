package routing

import (
	"net/http"

	"github.com/withqb/coddy/apis/userapi/api"
	roomserverAPI "github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

func LeaveRoomByID(
	req *http.Request,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomID string,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("device userID is invalid"),
		}
	}

	// Prepare to ask the roomserver to perform the room join.
	leaveReq := roomserverAPI.PerformLeaveRequest{
		RoomID: roomID,
		Leaver: *userID,
	}
	leaveRes := roomserverAPI.PerformLeaveResponse{}

	// Ask the roomserver to perform the leave.
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
