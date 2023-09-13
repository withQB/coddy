package routing

import (
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/internal"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

// GetEvent implements
//
//	GET /_coddy/client/r0/frames/{frameId}/event/{eventId}
func GetEvent(
	req *http.Request,
	device *userapi.Device,
	rawFrameID string,
	eventID string,
	cfg *config.SyncAPI,
	syncDB storage.Database,
	rsAPI api.SyncDataframeAPI,
) xutil.JSONResponse {
	ctx := req.Context()
	db, err := syncDB.NewDatabaseTransaction(ctx)
	logger := xutil.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id": eventID,
		"frame_id":  rawFrameID,
	})
	if err != nil {
		logger.WithError(err).Error("GetEvent: syncDB.NewDatabaseTransaction failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	frameID, err := spec.NewFrameID(rawFrameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid frame ID"),
		}
	}

	events, err := db.Events(ctx, []string{eventID})
	if err != nil {
		logger.WithError(err).Error("GetEvent: syncDB.Events failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// The requested event does not exist in our database
	if len(events) == 0 {
		logger.Debugf("GetEvent: requested event doesn't exist locally")
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you do not have permission to read this event"),
		}
	}

	// If the request is coming from an appservice, get the user from the request
	rawUserID := device.UserID
	if asUserID := req.FormValue("user_id"); device.AppserviceID != "" && asUserID != "" {
		rawUserID = asUserID
	}

	userID, err := spec.NewUserID(rawUserID, true)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("invalid device.UserID")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	// Apply history visibility to determine if the user is allowed to view the event
	events, err = internal.ApplyHistoryVisibilityFilter(ctx, db, rsAPI, events, nil, *userID, "event")
	if err != nil {
		logger.WithError(err).Error("GetEvent: internal.ApplyHistoryVisibilityFilter failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// We only ever expect there to be one event
	if len(events) != 1 {
		// 0 events -> not allowed to view event; > 1 events -> something that shouldn't happen
		logger.WithField("event_count", len(events)).Debug("GetEvent: can't return the requested event")
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you do not have permission to read this event"),
		}
	}

	senderUserID, err := rsAPI.QueryUserIDForSender(req.Context(), *frameID, events[0].SenderID())
	if err != nil || senderUserID == nil {
		xutil.GetLogger(req.Context()).WithError(err).WithField("senderID", events[0].SenderID()).WithField("frameID", *frameID).Error("QueryUserIDForSender errored or returned nil-user ID when user should be part of a frame")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	sk := events[0].StateKey()
	if sk != nil && *sk != "" {
		evFrameID, err := spec.NewFrameID(events[0].FrameID())
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("frameID is invalid"),
			}
		}
		skUserID, err := rsAPI.QueryUserIDForSender(ctx, *evFrameID, spec.SenderID(*events[0].StateKey()))
		if err == nil && skUserID != nil {
			skString := skUserID.String()
			sk = &skString
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: synctypes.ToClientEvent(events[0], synctypes.FormatAll, senderUserID.String(), sk, events[0].Unsigned()),
	}
}
