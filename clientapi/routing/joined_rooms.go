package routing

import (
	"net/http"

	"github.com/withqb/xutil"

	"github.com/withqb/coddy/roomserver/api"
	userapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools/spec"
)

type getJoinedRoomsResponse struct {
	JoinedRooms []string `json:"joined_rooms"`
}

func GetJoinedRooms(
	req *http.Request,
	device *userapi.Device,
	rsAPI api.ClientRoomserverAPI,
) xutil.JSONResponse {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("Invalid device user ID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	rooms, err := rsAPI.QueryRoomsForUser(req.Context(), *deviceUserID, "join")
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QueryRoomsForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	var roomIDStrs []string
	if rooms == nil {
		roomIDStrs = []string{}
	} else {
		roomIDStrs = make([]string, len(rooms))
		for i, roomID := range rooms {
			roomIDStrs[i] = roomID.String()
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: getJoinedRoomsResponse{roomIDStrs},
	}
}
