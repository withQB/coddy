package routing

import (
	"net/http"

	"github.com/withqb/coddy/apis/clientapi/auth/authtypes"
	"github.com/withqb/coddy/apis/clientapi/httputil"
	"github.com/withqb/coddy/apis/clientapi/threepid"
	"github.com/withqb/coddy/apis/userapi/api"
	userdb "github.com/withqb/coddy/apis/userapi/storage"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/xtools"
	"github.com/withqb/xutil"
)

type reqTokenResponse struct {
	SID string `json:"sid"`
}

type ThreePIDsResponse struct {
	ThreePIDs []authtypes.ThreePID `json:"threepids"`
}

// RequestEmailToken implements:
//
//	POST /account/3pid/email/requestToken
//	POST /register/email/requestToken
func RequestEmailToken(req *http.Request, threePIDAPI api.ClientUserAPI, cfg *config.ClientAPI, client *fclient.Client) xutil.JSONResponse {
	var body threepid.EmailAssociationRequest
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	var resp reqTokenResponse
	var err error

	// Check if the 3PID is already in use locally
	res := &api.QueryLocalpartForThreePIDResponse{}
	err = threePIDAPI.QueryLocalpartForThreePID(req.Context(), &api.QueryLocalpartForThreePIDRequest{
		ThreePID: body.Email,
		Medium:   "email",
	}, res)

	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("threePIDAPI.QueryLocalpartForThreePID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if len(res.Localpart) > 0 {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.CoddyError{
				ErrCode: spec.ErrorThreePIDInUse,
				Err:     userdb.Err3PIDInUse.Error(),
			},
		}
	}

	resp.SID, err = threepid.CreateSession(req.Context(), body, cfg, client)
	switch err.(type) {
	case nil:
	case threepid.ErrNotTrusted:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CreateSession failed")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotTrusted(body.IDServer),
		}
	default:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CreateSession failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}

// CheckAndSave3PIDAssociation implements POST /account/3pid
func CheckAndSave3PIDAssociation(
	req *http.Request, threePIDAPI api.ClientUserAPI, device *api.Device,
	cfg *config.ClientAPI, client *fclient.Client,
) xutil.JSONResponse {
	var body threepid.EmailAssociationCheckRequest
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	// Check if the association has been validated
	verified, address, medium, err := threepid.CheckAssociation(req.Context(), body.Creds, cfg, client)
	switch err.(type) {
	case nil:
	case threepid.ErrNotTrusted:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAssociation failed")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotTrusted(body.Creds.IDServer),
		}
	default:
		xutil.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAssociation failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !verified {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.CoddyError{
				ErrCode: spec.ErrorThreePIDAuthFailed,
				Err:     "Failed to auth 3pid",
			},
		}
	}

	if body.Bind {
		// Publish the association on the identity server if requested
		err = threepid.PublishAssociation(req.Context(), body.Creds, device.UserID, cfg, client)
		if err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("threepid.PublishAssociation failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// Save the association in the database
	localpart, domain, err := xtools.SplitID('@', device.UserID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if err = threePIDAPI.PerformSaveThreePIDAssociation(req.Context(), &api.PerformSaveThreePIDAssociationRequest{
		ThreePID:   address,
		Localpart:  localpart,
		ServerName: domain,
		Medium:     medium,
	}, &struct{}{}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("threePIDAPI.PerformSaveThreePIDAssociation failed")
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

// GetAssociated3PIDs implements GET /account/3pid
func GetAssociated3PIDs(
	req *http.Request, threepidAPI api.ClientUserAPI, device *api.Device,
) xutil.JSONResponse {
	localpart, domain, err := xtools.SplitID('@', device.UserID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	res := &api.QueryThreePIDsForLocalpartResponse{}
	err = threepidAPI.QueryThreePIDsForLocalpart(req.Context(), &api.QueryThreePIDsForLocalpartRequest{
		Localpart:  localpart,
		ServerName: domain,
	}, res)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("threepidAPI.QueryThreePIDsForLocalpart failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: ThreePIDsResponse{res.ThreePIDs},
	}
}

// Forget3PID implements POST /account/3pid/delete
func Forget3PID(req *http.Request, threepidAPI api.ClientUserAPI) xutil.JSONResponse {
	var body authtypes.ThreePID
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	if err := threepidAPI.PerformForgetThreePID(req.Context(), &api.PerformForgetThreePIDRequest{
		ThreePID: body.Address,
		Medium:   body.Medium,
	}, &struct{}{}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("threepidAPI.PerformForgetThreePID failed")
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
