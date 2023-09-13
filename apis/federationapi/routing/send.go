package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/federationapi/producers"
	userAPI "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

const (
	// Event was passed to the Roomserver
	MetricsOutcomeOK = "ok"
	// Event failed to be processed
	MetricsOutcomeFail = "fail"
	// Event failed auth checks
	MetricsOutcomeRejected = "rejected"
	// Terminated the transaction
	MetricsOutcomeFatal = "fatal"
	// The event has missing auth_events we need to fetch
	MetricsWorkMissingAuthEvents = "missing_auth_events"
	// No work had to be done as we had all prev/auth events
	MetricsWorkDirect = "direct"
	// The event has missing prev_events we need to call /g_m_e for
	MetricsWorkMissingPrevEvents = "missing_prev_events"
)

var inFlightTxnsPerOrigin sync.Map // transaction ID -> chan xutil.JSONResponse

// Send implements /_matrix/federation/v1/send/{txnID}
func Send(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	txnID xtools.TransactionID,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keyAPI userAPI.FederationUserAPI,
	keys xtools.JSONVerifier,
	federation fclient.FederationClient,
	mu *internal.MutexByRoom,
	producer *producers.SyncAPIProducer,
) xutil.JSONResponse {
	// First we should check if this origin has already submitted this
	// txn ID to us. If they have and the txnIDs map contains an entry,
	// the transaction is still being worked on. The new client can wait
	// for it to complete rather than creating more work.
	index := string(request.Origin()) + "\000" + string(txnID)
	v, ok := inFlightTxnsPerOrigin.LoadOrStore(index, make(chan xutil.JSONResponse, 1))
	ch := v.(chan xutil.JSONResponse)
	if ok {
		// This origin already submitted this txn ID to us, and the work
		// is still taking place, so we'll just wait for it to finish.
		ctx, cancel := context.WithTimeout(httpReq.Context(), time.Minute*5)
		defer cancel()
		select {
		case <-ctx.Done():
			// If the caller gives up then return straight away. We don't
			// want to attempt to process what they sent us any further.
			return xutil.JSONResponse{Code: http.StatusRequestTimeout}
		case res := <-ch:
			// The original task just finished processing so let's return
			// the result of it.
			if res.Code == 0 {
				return xutil.JSONResponse{Code: http.StatusAccepted}
			}
			return res
		}
	}
	// Otherwise, store that we're currently working on this txn from
	// this origin. When we're done processing, close the channel.
	defer close(ch)
	defer inFlightTxnsPerOrigin.Delete(index)

	var txnEvents struct {
		PDUs []json.RawMessage `json:"pdus"`
		EDUs []xtools.EDU      `json:"edus"`
	}

	if err := json.Unmarshal(request.Content(), &txnEvents); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// Transactions are limited in size; they can have at most 50 PDUs and 100 EDUs.
	// https://matrix.org/docs/spec/server_server/latest#transactions
	if len(txnEvents.PDUs) > 50 || len(txnEvents.EDUs) > 100 {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("max 50 pdus / 100 edus"),
		}
	}

	t := internal.NewTxnReq(
		rsAPI,
		keyAPI,
		cfg.Matrix.ServerName,
		keys,
		mu,
		producer,
		cfg.Matrix.Presence.EnableInbound,
		txnEvents.PDUs,
		txnEvents.EDUs,
		request.Origin(),
		txnID,
		cfg.Matrix.ServerName)

	xutil.GetLogger(httpReq.Context()).Debugf("Received transaction %q from %q containing %d PDUs, %d EDUs", txnID, request.Origin(), len(t.PDUs), len(t.EDUs))

	resp, jsonErr := t.ProcessTransaction(httpReq.Context())
	if jsonErr != nil {
		xutil.GetLogger(httpReq.Context()).WithField("jsonErr", jsonErr).Error("t.processTransaction failed")
		return *jsonErr
	}

	// https://matrix.org/docs/spec/server_server/r0.1.3#put-matrix-federation-v1-send-txnid
	// Status code 200:
	// The result of processing the transaction. The server is to use this response
	// even in the event of one or more PDUs failing to be processed.
	res := xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
	ch <- res
	return res
}
