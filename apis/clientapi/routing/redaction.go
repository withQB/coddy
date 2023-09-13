package routing

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/transactions"
	roomserverAPI "github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/types"
	"github.com/withqb/coddy/setup/config"
)

type redactionContent struct {
	Reason string `json:"reason"`
}

type redactionResponse struct {
	EventID string `json:"event_id"`
}

func SendRedaction(
	req *http.Request, device *userapi.Device, roomID, eventID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	txnID *string,
	txnCache *transactions.Cache,
) xutil.JSONResponse {
	deviceUserID, userIDErr := spec.NewUserID(device.UserID, true)
	if userIDErr != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to redact"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, queryErr := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *deviceUserID)
	if queryErr != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to redact"),
		}
	}

	resErr := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if resErr != nil {
		return *resErr
	}

	// if user is member of room, and sender ID is nil, then this user doesn't have a pseudo ID for some reason,
	// which is unexpected.
	if senderID == nil {
		xutil.GetLogger(req.Context()).WithField("userID", *deviceUserID).WithField("roomID", roomID).Error("missing sender ID for user, despite having membership")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	if txnID != nil {
		// Try to fetch response from transactionsCache
		if res, ok := txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}

	ev := roomserverAPI.GetEvent(req.Context(), rsAPI, roomID, eventID)
	if ev == nil {
		return xutil.JSONResponse{
			Code: 400,
			JSON: spec.NotFound("unknown event ID"), // TODO: is it ok to leak existence?
		}
	}
	if ev.RoomID() != roomID {
		return xutil.JSONResponse{
			Code: 400,
			JSON: spec.NotFound("cannot redact event in another room"),
		}
	}

	// "Users may redact their own events, and any user with a power level greater than or equal
	// to the redact power level of the room may redact events there"
	// https://matrix.org/docs/spec/client_server/r0.6.1#put-matrix-client-r0-rooms-roomid-redact-eventid-txnid
	allowedToRedact := ev.SenderID() == *senderID
	if !allowedToRedact {
		plEvent := roomserverAPI.GetStateEvent(req.Context(), rsAPI, roomID, xtools.StateKeyTuple{
			EventType: spec.MRoomPowerLevels,
			StateKey:  "",
		})
		if plEvent == nil {
			return xutil.JSONResponse{
				Code: 403,
				JSON: spec.Forbidden("You don't have permission to redact this event, no power_levels event in this room."),
			}
		}
		pl, plErr := plEvent.PowerLevels()
		if plErr != nil {
			return xutil.JSONResponse{
				Code: 403,
				JSON: spec.Forbidden(
					"You don't have permission to redact this event, the power_levels event for this room is malformed so auth checks cannot be performed.",
				),
			}
		}
		allowedToRedact = pl.UserLevel(*senderID) >= pl.Redact
	}
	if !allowedToRedact {
		return xutil.JSONResponse{
			Code: 403,
			JSON: spec.Forbidden("You don't have permission to redact this event, power level too low."),
		}
	}

	var r redactionContent
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	// create the new event and set all the fields we can
	proto := xtools.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   roomID,
		Type:     spec.MRoomRedaction,
		Redacts:  eventID,
	}
	err = proto.SetContent(r)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("proto.SetContent failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	identity, err := rsAPI.SigningIdentityFor(req.Context(), *validRoomID, *deviceUserID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var queryRes roomserverAPI.QueryLatestEventsAndStateResponse
	e, err := eventutil.QueryAndBuildEvent(req.Context(), &proto, &identity, time.Now(), rsAPI, &queryRes)
	if errors.Is(err, eventutil.ErrRoomNoExists{}) {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room does not exist"),
		}
	}
	domain := device.UserDomain()
	if err = roomserverAPI.SendEvents(context.Background(), rsAPI, roomserverAPI.KindNew, []*types.HeaderedEvent{e}, device.UserDomain(), domain, domain, nil, false); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Errorf("failed to SendEvents")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	res := xutil.JSONResponse{
		Code: 200,
		JSON: redactionResponse{
			EventID: e.EventID(),
		},
	}

	// Add response to transactionsCache
	if txnID != nil {
		txnCache.AddTransaction(device.AccessToken, *txnID, req.URL, &res)
	}

	return res
}
