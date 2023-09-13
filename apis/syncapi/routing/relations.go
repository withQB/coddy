package routing

import (
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/syncapi/internal"
	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	rstypes "github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/spec"
)

type RelationsResponse struct {
	Chunk     []synctypes.ClientEvent `json:"chunk"`
	NextBatch string                  `json:"next_batch,omitempty"`
	PrevBatch string                  `json:"prev_batch,omitempty"`
}

// nolint:gocyclo
func Relations(
	req *http.Request, device *userapi.Device,
	syncDB storage.Database,
	rsAPI api.SyncDataframeAPI,
	rawFrameID, eventID, relType, eventType string,
) xutil.JSONResponse {
	frameID, err := spec.NewFrameID(rawFrameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid frame ID"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("device.UserID invalid")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	var from, to types.StreamPosition
	var limit int
	dir := req.URL.Query().Get("dir")
	if f := req.URL.Query().Get("from"); f != "" {
		if from, err = types.NewStreamPositionFromString(f); err != nil {
			return xutil.ErrorResponse(err)
		}
	}
	if t := req.URL.Query().Get("to"); t != "" {
		if to, err = types.NewStreamPositionFromString(t); err != nil {
			return xutil.ErrorResponse(err)
		}
	}
	if l := req.URL.Query().Get("limit"); l != "" {
		if limit, err = strconv.Atoi(l); err != nil {
			return xutil.ErrorResponse(err)
		}
	}
	if limit == 0 || limit > 50 {
		limit = 50
	}
	if dir == "" {
		dir = "b"
	}
	if dir != "b" && dir != "f" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Bad or missing dir query parameter (should be either 'b' or 'f')"),
		}
	}

	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		logrus.WithError(err).Error("Failed to get snapshot for relations")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	res := &RelationsResponse{
		Chunk: []synctypes.ClientEvent{},
	}
	var events []types.StreamEvent
	events, res.PrevBatch, res.NextBatch, err = snapshot.RelationsFor(
		req.Context(), frameID.String(), eventID, relType, eventType, from, to, dir == "b", limit,
	)
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	headeredEvents := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, event := range events {
		headeredEvents = append(headeredEvents, event.HeaderedEvent)
	}

	// Apply history visibility to the result events.
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(req.Context(), snapshot, rsAPI, headeredEvents, nil, *userID, "relations")
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	// Convert the events into client events, and optionally filter based on the event
	// type if it was specified.
	res.Chunk = make([]synctypes.ClientEvent, 0, len(filteredEvents))
	for _, event := range filteredEvents {
		sender := spec.UserID{}
		userID, err := rsAPI.QueryUserIDForSender(req.Context(), *frameID, event.SenderID())
		if err == nil && userID != nil {
			sender = *userID
		}

		sk := event.StateKey()
		if sk != nil && *sk != "" {
			skUserID, err := rsAPI.QueryUserIDForSender(req.Context(), *frameID, spec.SenderID(*event.StateKey()))
			if err == nil && skUserID != nil {
				skString := skUserID.String()
				sk = &skString
			}
		}
		res.Chunk = append(
			res.Chunk,
			synctypes.ToClientEvent(event.PDU, synctypes.FormatAll, sender.String(), sk, event.Unsigned()),
		)
	}

	succeeded = true
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}
