package routing

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/relayapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// SendTransactionToRelay implements PUT /_matrix/federation/v1/send_relay/{txnID}/{userID}
// This endpoint can be extracted into a separate relay server service.
func SendTransactionToRelay(
	httpReq *http.Request,
	fedReq *fclient.FederationRequest,
	relayAPI api.RelayInternalAPI,
	txnID xtools.TransactionID,
	userID spec.UserID,
) xutil.JSONResponse {
	logrus.Infof("Processing send_relay for %s", userID.String())

	var txnEvents fclient.RelayEvents
	if err := json.Unmarshal(fedReq.Content(), &txnEvents); err != nil {
		logrus.Info("The request body could not be decoded into valid JSON." + err.Error())
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON." + err.Error()),
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

	t := xtools.Transaction{}
	t.PDUs = txnEvents.PDUs
	t.EDUs = txnEvents.EDUs
	t.Origin = fedReq.Origin()
	t.TransactionID = txnID
	t.Destination = userID.Domain()

	xutil.GetLogger(httpReq.Context()).Warnf("Received transaction %q from %q containing %d PDUs, %d EDUs", txnID, fedReq.Origin(), len(t.PDUs), len(t.EDUs))

	err := relayAPI.PerformStoreTransaction(httpReq.Context(), t, userID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("could not store the transaction for forwarding"),
		}
	}

	return xutil.JSONResponse{Code: 200}
}
