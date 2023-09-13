package routing

import (
	"errors"
	"net/http"

	appserviceAPI "github.com/withqb/coddy/appservice/api"
	"github.com/withqb/coddy/clientapi/httputil"
	"github.com/withqb/coddy/internal/eventutil"
	roomserverAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/version"
	"github.com/withqb/coddy/setup/config"
	userapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type upgradeRoomRequest struct {
	NewVersion string `json:"new_version"`
}

type upgradeRoomResponse struct {
	ReplacementRoom string `json:"replacement_room"`
}

// UpgradeRoom implements /upgrade
func UpgradeRoom(
	req *http.Request, device *userapi.Device,
	cfg *config.ClientAPI,
	roomID string, profileAPI userapi.ClientUserAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	var r upgradeRoomRequest
	if rErr := httputil.UnmarshalJSONRequest(req, &r); rErr != nil {
		return *rErr
	}

	// Validate that the room version is supported
	if _, err := version.SupportedRoomVersion(xtools.RoomVersion(r.NewVersion)); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion("This server does not support that room version"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("device UserID is invalid")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	newRoomID, err := rsAPI.PerformRoomUpgrade(req.Context(), roomID, *userID, xtools.RoomVersion(r.NewVersion))
	switch e := err.(type) {
	case nil:
	case roomserverAPI.ErrNotAllowed:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}
	default:
		if errors.Is(err, eventutil.ErrRoomNoExists{}) {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Room does not exist"),
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: upgradeRoomResponse{
			ReplacementRoom: newRoomID,
		},
	}
}
