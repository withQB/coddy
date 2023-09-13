package routing

import (
	"errors"
	"net/http"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/eventutil"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/version"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type upgradeFrameRequest struct {
	NewVersion string `json:"new_version"`
}

type upgradeFrameResponse struct {
	ReplacementFrame string `json:"replacement_frame"`
}

// UpgradeFrame implements /upgrade
func UpgradeFrame(
	req *http.Request, device *userapi.Device,
	cfg *config.ClientAPI,
	frameID string, profileAPI userapi.ClientUserAPI,
	rsAPI dataframeAPI.ClientDataframeAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) xutil.JSONResponse {
	var r upgradeFrameRequest
	if rErr := httputil.UnmarshalJSONRequest(req, &r); rErr != nil {
		return *rErr
	}

	// Validate that the frame version is supported
	if _, err := version.SupportedFrameVersion(xtools.FrameVersion(r.NewVersion)); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedFrameVersion("This server does not support that frame version"),
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
	newFrameID, err := rsAPI.PerformFrameUpgrade(req.Context(), frameID, *userID, xtools.FrameVersion(r.NewVersion))
	switch e := err.(type) {
	case nil:
	case dataframeAPI.ErrNotAllowed:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}
	default:
		if errors.Is(err, eventutil.ErrFrameNoExists{}) {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Frame does not exist"),
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: upgradeFrameResponse{
			ReplacementFrame: newFrameID,
		},
	}
}
