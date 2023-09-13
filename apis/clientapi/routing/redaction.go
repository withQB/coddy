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
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
)

type redactionContent struct {
	Reason string `json:"reason"`
}

type redactionResponse struct {
	EventID string `json:"event_id"`
}

func SendRedaction(
	req *http.Request, device *userapi.Device, frameID, eventID string, cfg *config.ClientAPI,
	rsAPI dataframeAPI.ClientDataframeAPI,
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
	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("FrameID is invalid"),
		}
	}
	senderID, queryErr := rsAPI.QuerySenderIDForUser(req.Context(), *validFrameID, *deviceUserID)
	if queryErr != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to redact"),
		}
	}

	resErr := checkMemberInFrame(req.Context(), rsAPI, *deviceUserID, frameID)
	if resErr != nil {
		return *resErr
	}

	// if user is member of frame, and sender ID is nil, then this user doesn't have a pseudo ID for some reason,
	// which is unexpected.
	if senderID == nil {
		xutil.GetLogger(req.Context()).WithField("userID", *deviceUserID).WithField("frameID", frameID).Error("missing sender ID for user, despite having membership")
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

	ev := dataframeAPI.GetEvent(req.Context(), rsAPI, frameID, eventID)
	if ev == nil {
		return xutil.JSONResponse{
			Code: 400,
			JSON: spec.NotFound("unknown event ID"), // TODO: is it ok to leak existence?
		}
	}
	if ev.FrameID() != frameID {
		return xutil.JSONResponse{
			Code: 400,
			JSON: spec.NotFound("cannot redact event in another frame"),
		}
	}

	// "Users may redact their own events, and any user with a power level greater than or equal
	// to the redact power level of the frame may redact events there"
	allowedToRedact := ev.SenderID() == *senderID
	if !allowedToRedact {
		plEvent := dataframeAPI.GetStateEvent(req.Context(), rsAPI, frameID, xtools.StateKeyTuple{
			EventType: spec.MFramePowerLevels,
			StateKey:  "",
		})
		if plEvent == nil {
			return xutil.JSONResponse{
				Code: 403,
				JSON: spec.Forbidden("You don't have permission to redact this event, no power_levels event in this frame."),
			}
		}
		pl, plErr := plEvent.PowerLevels()
		if plErr != nil {
			return xutil.JSONResponse{
				Code: 403,
				JSON: spec.Forbidden(
					"You don't have permission to redact this event, the power_levels event for this frame is malformed so auth checks cannot be performed.",
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
		FrameID:   frameID,
		Type:     spec.MFrameRedaction,
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

	identity, err := rsAPI.SigningIdentityFor(req.Context(), *validFrameID, *deviceUserID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var queryRes dataframeAPI.QueryLatestEventsAndStateResponse
	e, err := eventutil.QueryAndBuildEvent(req.Context(), &proto, &identity, time.Now(), rsAPI, &queryRes)
	if errors.Is(err, eventutil.ErrFrameNoExists{}) {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Frame does not exist"),
		}
	}
	domain := device.UserDomain()
	if err = dataframeAPI.SendEvents(context.Background(), rsAPI, dataframeAPI.KindNew, []*types.HeaderedEvent{e}, device.UserDomain(), domain, domain, nil, false); err != nil {
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
