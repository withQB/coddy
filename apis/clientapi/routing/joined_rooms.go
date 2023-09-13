package routing

import (
	"net/http"

	"github.com/withqb/xutil"

	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools/spec"
)

type getJoinedFramesResponse struct {
	JoinedFrames []string `json:"joined_frames"`
}

func GetJoinedFrames(
	req *http.Request,
	device *userapi.Device,
	rsAPI api.ClientDataframeAPI,
) xutil.JSONResponse {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("Invalid device user ID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	frames, err := rsAPI.QueryFramesForUser(req.Context(), *deviceUserID, "join")
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QueryFramesForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	var frameIDStrs []string
	if frames == nil {
		frameIDStrs = []string{}
	} else {
		frameIDStrs = make([]string, len(frames))
		for i, frameID := range frames {
			frameIDStrs[i] = frameID.String()
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: getJoinedFramesResponse{frameIDStrs},
	}
}
