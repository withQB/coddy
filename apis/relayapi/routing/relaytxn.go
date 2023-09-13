package routing

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/relayapi/api"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetTransactionFromRelay implements GET /_matrix/federation/v1/relay_txn/{userID}
// This endpoint can be extracted into a separate relay server service.
func GetTransactionFromRelay(
	httpReq *http.Request,
	fedReq *fclient.FederationRequest,
	relayAPI api.RelayInternalAPI,
	userID spec.UserID,
) xutil.JSONResponse {
	logrus.Infof("Processing relay_txn for %s", userID.String())

	var previousEntry fclient.RelayEntry
	if err := json.Unmarshal(fedReq.Content(), &previousEntry); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("invalid json provided"),
		}
	}
	if previousEntry.EntryID < 0 {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.BadJSON("Invalid entry id provided. Must be >= 0."),
		}
	}
	logrus.Infof("Previous entry provided: %v", previousEntry.EntryID)

	response, err := relayAPI.QueryTransactions(httpReq.Context(), userID, previousEntry)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespGetRelayTransaction{
			Transaction:   response.Transaction,
			EntryID:       response.EntryID,
			EntriesQueued: response.EntriesQueued,
		},
	}
}
