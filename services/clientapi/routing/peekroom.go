package routing

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

func PeekFrameByIDOrAlias(
	req *http.Request,
	device *api.Device,
	rsAPI dataframeAPI.ClientDataframeAPI,
	frameIDOrAlias string,
) xutil.JSONResponse {
	// if this is a remote frameIDOrAlias, we have to ask the dataframe (or federation sender?) to
	// to call /peek and /state on the remote server.
	// TDO: in future we could skip this if we know we're already participating in the frame,
	// but this is fiddly in case we stop participating in the frame.

	// then we create a local peek.
	peekReq := dataframeAPI.PerformPeekRequest{
		FrameIDOrAlias: frameIDOrAlias,
		UserID:        device.UserID,
		DeviceID:      device.ID,
	}
	// Check to see if any ?server_name= query parameters were
	// given in the request.
	if serverNames, ok := req.URL.Query()["server_name"]; ok {
		for _, serverName := range serverNames {
			peekReq.ServerNames = append(
				peekReq.ServerNames,
				spec.ServerName(serverName),
			)
		}
	}

	// Ask the dataframe to perform the peek.
	frameID, err := rsAPI.PerformPeek(req.Context(), &peekReq)
	switch e := err.(type) {
	case dataframeAPI.ErrInvalidID:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}
	case dataframeAPI.ErrNotAllowed:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}
	case *xcore.HTTPError:
		return xutil.JSONResponse{
			Code: e.Code,
			JSON: json.RawMessage(e.Message),
		}
	case nil:
	default:
		logrus.WithError(err).WithField("frameID", frameIDOrAlias).Errorf("failed to peek frame")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// if this user is already joined to the frame, we let them peek anyway
	// (given they might be about to part the frame, and it makes things less fiddly)

	// Peeking stops if none of the devices who started peeking have been
	// /syncing for a while, or if everyone who was peeking calls /leave
	// (or /unpeek with a server_name param? or DELETE /peek?)
	// on the peeked frame.

	return xutil.JSONResponse{
		Code: http.StatusOK,
		// TDO: Put the response struct somewhere internal.
		JSON: struct {
			FrameID string `json:"frame_id"`
		}{frameID},
	}
}

func UnpeekFrameByID(
	req *http.Request,
	device *api.Device,
	rsAPI dataframeAPI.ClientDataframeAPI,
	frameID string,
) xutil.JSONResponse {
	err := rsAPI.PerformUnpeek(req.Context(), frameID, device.UserID, device.ID)
	switch e := err.(type) {
	case dataframeAPI.ErrInvalidID:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}
	case nil:
	default:
		logrus.WithError(err).WithField("frameID", frameID).Errorf("failed to un-peek frame")
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
