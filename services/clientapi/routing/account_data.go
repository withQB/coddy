package routing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/clientapi/producers"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/xutil"
)

// GetAccountData implements GET /user/{userId}/[frames/{frameid}/]account_data/{type}
func GetAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, frameID string, dataType string,
) xutil.JSONResponse {
	if userID != device.UserID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	dataReq := api.QueryAccountDataRequest{
		UserID:   userID,
		DataType: dataType,
		FrameID:   frameID,
	}
	dataRes := api.QueryAccountDataResponse{}
	if err := userAPI.QueryAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryAccountData failed")
		return xutil.ErrorResponse(fmt.Errorf("userAPI.QueryAccountData: %w", err))
	}

	var data json.RawMessage
	var ok bool
	if frameID != "" {
		data, ok = dataRes.FrameAccountData[frameID][dataType]
	} else {
		data, ok = dataRes.GlobalAccountData[dataType]
	}
	if ok {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: data,
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("data not found"),
	}
}

// SaveAccountData implements PUT /user/{userId}/[frames/{frameId}/]account_data/{type}
func SaveAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, frameID string, dataType string, syncProducer *producers.SyncAPIProducer,
) xutil.JSONResponse {
	if userID != device.UserID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	defer req.Body.Close() // nolint: errcheck

	if req.Body == http.NoBody {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("Content not JSON"),
		}
	}

	if dataType == "m.fully_read" || dataType == "m.push_rules" {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("Unable to modify %q using this API", dataType)),
		}
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("io.ReadAll failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !json.Valid(body) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Bad JSON content"),
		}
	}

	dataReq := api.InputAccountDataRequest{
		UserID:      userID,
		DataType:    dataType,
		FrameID:      frameID,
		AccountData: json.RawMessage(body),
	}
	dataRes := api.InputAccountDataResponse{}
	if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
		return xutil.ErrorResponse(err)
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

type fullyReadEvent struct {
	EventID string `json:"event_id"`
}

// SaveReadMarker implements POST /frames/{frameId}/read_markers
func SaveReadMarker(
	req *http.Request,
	userAPI api.ClientUserAPI, rsAPI dataframeAPI.ClientDataframeAPI,
	syncProducer *producers.SyncAPIProducer, device *api.Device, frameID string,
) xutil.JSONResponse {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("userID for this device is invalid"),
		}
	}

	// Verify that the user is a member of this frame
	resErr := checkMemberInFrame(req.Context(), rsAPI, *deviceUserID, frameID)
	if resErr != nil {
		return *resErr
	}

	var r eventutil.ReadMarkerJSON
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	if r.FullyRead != "" {
		data, err := json.Marshal(fullyReadEvent{EventID: r.FullyRead})
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		dataReq := api.InputAccountDataRequest{
			UserID:      device.UserID,
			DataType:    "m.fully_read",
			FrameID:      frameID,
			AccountData: data,
		}
		dataRes := api.InputAccountDataResponse{}
		if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
			return xutil.ErrorResponse(err)
		}
	}

	// Handle the read receipts that may be included in the read marker.
	if r.Read != "" {
		return SetReceipt(req, userAPI, syncProducer, device, frameID, "m.read", r.Read)
	}
	if r.ReadPrivate != "" {
		return SetReceipt(req, userAPI, syncProducer, device, frameID, "m.read.private", r.ReadPrivate)
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
