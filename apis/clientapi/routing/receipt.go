package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/withqb/coddy/apis/clientapi/producers"
	"github.com/withqb/xtools/spec"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/userapi/api"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/xutil"
)

func SetReceipt(req *http.Request, userAPI api.ClientUserAPI, syncProducer *producers.SyncAPIProducer, device *userapi.Device, roomID, receiptType, eventID string) xutil.JSONResponse {
	timestamp := spec.AsTimestamp(time.Now())
	logrus.WithFields(logrus.Fields{
		"roomID":      roomID,
		"receiptType": receiptType,
		"eventID":     eventID,
		"userId":      device.UserID,
		"timestamp":   timestamp,
	}).Debug("Setting receipt")

	switch receiptType {
	case "m.read", "m.read.private":
		if err := syncProducer.SendReceipt(req.Context(), device.UserID, roomID, eventID, receiptType, timestamp); err != nil {
			return xutil.ErrorResponse(err)
		}

	case "m.fully_read":
		data, err := json.Marshal(fullyReadEvent{EventID: eventID})
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		dataReq := api.InputAccountDataRequest{
			UserID:      device.UserID,
			DataType:    "m.fully_read",
			RoomID:      roomID,
			AccountData: data,
		}
		dataRes := api.InputAccountDataResponse{}
		if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
			return xutil.ErrorResponse(err)
		}

	default:
		return xutil.MessageResponse(400, fmt.Sprintf("Receipt type '%s' not known", receiptType))
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
