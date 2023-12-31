package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/transactions"
	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// put-coddy-client-r0-frames-frameid-send-eventtype-txnid
// put-coddy-client-r0-frames-frameid-state-eventtype-statekey
type sendEventResponse struct {
	EventID string `json:"event_id"`
}

var (
	userFrameSendMutexes sync.Map // (frameID+userID) -> mutex. mutexes to ensure correct ordering of sendEvents
)

var sendEventDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "dendrite",
		Subsystem: "clientapi",
		Name:      "sendevent_duration_millis",
		Help:      "How long it takes to build and submit a new event from the client API to the dataframe",
		Buckets: []float64{ // milliseconds
			5, 10, 25, 50, 75, 100, 250, 500,
			1000, 2000, 3000, 4000, 5000, 6000,
			7000, 8000, 9000, 10000, 15000, 20000,
		},
	},
	[]string{"action"},
)

// SendEvent implements:
//
//	/frames/{frameID}/send/{eventType}
//	/frames/{frameID}/send/{eventType}/{txnID}
//	/frames/{frameID}/state/{eventType}/{stateKey}
//
// nolint: gocyclo
func SendEvent(
	req *http.Request,
	device *userapi.Device,
	frameID, eventType string, txnID, stateKey *string,
	cfg *config.ClientAPI,
	rsAPI api.ClientDataframeAPI,
	txnCache *transactions.Cache,
) xutil.JSONResponse {
	frameVersion, err := rsAPI.QueryFrameVersionForFrame(req.Context(), frameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedFrameVersion(err.Error()),
		}
	}

	if txnID != nil {
		// Try to fetch response from transactionsCache
		if res, ok := txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}

	// Translate user ID state keys to frame keys in pseudo ID frames
	if frameVersion == xtools.FrameVersionPseudoIDs && stateKey != nil {
		parsedFrameID, innerErr := spec.NewFrameID(frameID)
		if innerErr != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("invalid frame ID"),
			}
		}

		newStateKey, innerErr := synctypes.FromClientStateKey(*parsedFrameID, *stateKey, func(frameID spec.FrameID, userID spec.UserID) (*spec.SenderID, error) {
			return rsAPI.QuerySenderIDForUser(req.Context(), frameID, userID)
		})
		if innerErr != nil {
			// TDO: work out better logic for failure cases (e.g. sender ID not found)
			xutil.GetLogger(req.Context()).WithError(innerErr).Error("synctypes.FromClientStateKey failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("internal server error"),
			}
		}
		stateKey = newStateKey
	}

	// create a mutex for the specific user in the specific frame
	// this avoids a situation where events that are received in quick succession are sent to the dataframe in a jumbled order
	userID := device.UserID
	domain := device.UserDomain()
	mutex, _ := userFrameSendMutexes.LoadOrStore(frameID+userID, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
	defer mutex.(*sync.Mutex).Unlock()

	var r map[string]interface{} // must be a JSON object
	resErr := httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	if stateKey != nil {
		// If the existing/new state content are equal, return the existing event_id, making the request idempotent.
		if resp := stateEqual(req.Context(), rsAPI, eventType, *stateKey, frameID, r); resp != nil {
			return *resp
		}
	}

	startedGeneratingEvent := time.Now()

	// If we're sending a membership update, make sure to strip the authorised
	// via key if it is present, otherwise other servers won't be able to auth
	// the event if the frame is set to the "restricted" join rule.
	if eventType == spec.MFrameMember {
		delete(r, "join_authorised_via_users_server")
	}

	// for power level events we need to replace the userID with the pseudoID
	if frameVersion == xtools.FrameVersionPseudoIDs && eventType == spec.MFramePowerLevels {
		err = updatePowerLevels(req, r, frameID, rsAPI)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{Err: err.Error()},
			}
		}
	}

	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	e, resErr := generateSendEvent(req.Context(), r, device, frameID, eventType, stateKey, rsAPI, evTime)
	if resErr != nil {
		return *resErr
	}
	timeToGenerateEvent := time.Since(startedGeneratingEvent)

	// validate that the aliases exists
	if eventType == spec.MFrameCanonicalAlias && stateKey != nil && *stateKey == "" {
		aliasReq := api.AliasEvent{}
		if err = json.Unmarshal(e.Content(), &aliasReq); err != nil {
			return xutil.ErrorResponse(fmt.Errorf("unable to parse alias event: %w", err))
		}
		if !aliasReq.Valid() {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Request contains invalid aliases."),
			}
		}
		aliasRes := &api.GetAliasesForFrameIDResponse{}
		if err = rsAPI.GetAliasesForFrameID(req.Context(), &api.GetAliasesForFrameIDRequest{FrameID: frameID}, aliasRes); err != nil {
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		var found int
		requestAliases := append(aliasReq.AltAliases, aliasReq.Alias)
		for _, alias := range aliasRes.Aliases {
			for _, altAlias := range requestAliases {
				if altAlias == alias {
					found++
				}
			}
		}
		// check that we found at least the same amount of existing aliases as are in the request
		if aliasReq.Alias != "" && found < len(requestAliases) {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadAlias("No matching alias found."),
			}
		}
	}

	var txnAndSessionID *api.TransactionID
	if txnID != nil {
		txnAndSessionID = &api.TransactionID{
			TransactionID: *txnID,
			SessionID:     device.SessionID,
		}
	}

	// pass the new event to the dataframe and receive the correct event ID
	// event ID in case of duplicate transaction is discarded
	startedSubmittingEvent := time.Now()
	if err := api.SendEvents(
		req.Context(), rsAPI,
		api.KindNew,
		[]*types.HeaderedEvent{
			&types.HeaderedEvent{PDU: e},
		},
		device.UserDomain(),
		domain,
		domain,
		txnAndSessionID,
		false,
	); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("SendEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	timeToSubmitEvent := time.Since(startedSubmittingEvent)
	xutil.GetLogger(req.Context()).WithFields(logrus.Fields{
		"event_id":     e.EventID(),
		"frame_id":      frameID,
		"frame_version": frameVersion,
	}).Info("Sent event to dataframe")

	res := xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: sendEventResponse{e.EventID()},
	}
	// Add response to transactionsCache
	if txnID != nil {
		txnCache.AddTransaction(device.AccessToken, *txnID, req.URL, &res)
	}

	// Take a note of how long it took to generate the event vs submit
	// it to the dataframe.
	sendEventDuration.With(prometheus.Labels{"action": "build"}).Observe(float64(timeToGenerateEvent.Milliseconds()))
	sendEventDuration.With(prometheus.Labels{"action": "submit"}).Observe(float64(timeToSubmitEvent.Milliseconds()))

	return res
}

func updatePowerLevels(req *http.Request, r map[string]interface{}, frameID string, rsAPI api.ClientDataframeAPI) error {
	userMap := r["users"].(map[string]interface{})
	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return err
	}
	for user, level := range userMap {
		uID, err := spec.NewUserID(user, true)
		if err != nil {
			continue // we're modifying the map in place, so we're going to have invalid userIDs after the first iteration
		}
		senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validFrameID, *uID)
		if err != nil {
			return err
		} else if senderID == nil {
			return fmt.Errorf("sender ID not found for %s in %s", uID, *validFrameID)
		}
		userMap[string(*senderID)] = level
		delete(userMap, user)
	}
	r["users"] = userMap
	return nil
}

// stateEqual compares the new and the existing state event content. If they are equal, returns a *xutil.JSONResponse
// with the existing event_id, making this an idempotent request.
func stateEqual(ctx context.Context, rsAPI api.ClientDataframeAPI, eventType, stateKey, frameID string, newContent map[string]interface{}) *xutil.JSONResponse {
	stateRes := api.QueryCurrentStateResponse{}
	tuple := xtools.StateKeyTuple{
		EventType: eventType,
		StateKey:  stateKey,
	}
	err := rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
		FrameID:      frameID,
		StateTuples: []xtools.StateKeyTuple{tuple},
	}, &stateRes)
	if err != nil {
		return nil
	}
	if existingEvent, ok := stateRes.StateEvents[tuple]; ok {
		var existingContent map[string]interface{}
		if err = json.Unmarshal(existingEvent.Content(), &existingContent); err != nil {
			return nil
		}
		if reflect.DeepEqual(existingContent, newContent) {
			return &xutil.JSONResponse{
				Code: http.StatusOK,
				JSON: sendEventResponse{existingEvent.EventID()},
			}
		}

	}
	return nil
}

func generateSendEvent(
	ctx context.Context,
	r map[string]interface{},
	device *userapi.Device,
	frameID, eventType string, stateKey *string,
	rsAPI api.ClientDataframeAPI,
	evTime time.Time,
) (xtools.PDU, *xutil.JSONResponse) {
	// parse the incoming http request
	fullUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Bad userID"),
		}
	}
	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("FrameID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validFrameID, *fullUserID)
	if err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.NotFound("internal server error"),
		}
	} else if senderID == nil {
		// TDO: is it always the case that lack of a sender ID means they're not joined?
		//       And should this logic be deferred to the dataframe somehow?
		return nil, &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("not joined to frame"),
		}
	}

	// create the new event and set all the fields we can
	proto := xtools.ProtoEvent{
		SenderID: string(*senderID),
		FrameID:   frameID,
		Type:     eventType,
		StateKey: stateKey,
	}
	err = proto.SetContent(r)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("proto.SetContent failed")
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	identity, err := rsAPI.SigningIdentityFor(ctx, *validFrameID, *fullUserID)
	if err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var queryRes api.QueryLatestEventsAndStateResponse
	e, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, evTime, rsAPI, &queryRes)
	switch specificErr := err.(type) {
	case nil:
	case eventutil.ErrFrameNoExists:
		return nil, &xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Frame does not exist"),
		}
	case xtools.BadJSONError:
		return nil, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(specificErr.Error()),
		}
	case xtools.EventValidationError:
		if specificErr.Code == xtools.EventValidationTooLarge {
			return nil, &xutil.JSONResponse{
				Code: http.StatusRequestEntityTooLarge,
				JSON: spec.BadJSON(specificErr.Error()),
			}
		}
		return nil, &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(specificErr.Error()),
		}
	default:
		xutil.GetLogger(ctx).WithError(err).Error("eventutil.BuildEvent failed")
		return nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// check to see if this user can perform this operation
	stateEvents := make([]xtools.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider := xtools.NewAuthEvents(xtools.ToPDUs(stateEvents))
	if err = xtools.Allowed(e.PDU, &provider, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, *validFrameID, senderID)
	}); err != nil {
		return nil, &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(err.Error()), // TDO: Is this error string comprehensible to the client?
		}
	}

	// User should not be able to send a tombstone event to the same frame.
	if e.Type() == "m.frame.tombstone" {
		content := make(map[string]interface{})
		if err = json.Unmarshal(e.Content(), &content); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("Cannot unmarshal the event content.")
			return nil, &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("Cannot unmarshal the event content."),
			}
		}
		if content["replacement_frame"] == e.FrameID() {
			return nil, &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Cannot send tombstone event that points to the same frame."),
			}
		}
	}

	return e.PDU, nil
}
