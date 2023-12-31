// Copyright 2018 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// Backfill implements the /backfill federation endpoint.
func Backfill(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	rsAPI api.FederationDataframeAPI,
	frameID string,
	cfg *config.FederationAPI,
) xutil.JSONResponse {
	var res api.PerformBackfillResponse
	var eIDs []string
	var limit string
	var exists bool
	var err error

	// Check the frame ID's format.
	if _, _, err = xtools.SplitID('!', frameID); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Bad frame ID: " + err.Error()),
		}
	}

	// If we don't think we belong to this frame then don't waste the effort
	// responding to expensive requests for it.
	if err := ErrorIfLocalServerNotInFrame(httpReq.Context(), rsAPI, frameID); err != nil {
		return *err
	}

	// Check if all of the required parameters are there.
	eIDs, exists = httpReq.URL.Query()["v"]
	if !exists {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("v is missing"),
		}
	}
	limit = httpReq.URL.Query().Get("limit")
	if len(limit) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("limit is missing"),
		}
	}

	// Populate the request.
	req := api.PerformBackfillRequest{
		FrameID: frameID,
		// we don't know who the successors are for these events, which won't
		// be a problem because we don't use that information when servicing /backfill requests,
		// only when making them. TODO: Think of a better API shape
		BackwardsExtremities: map[string][]string{
			"": eIDs,
		},
		ServerName:  request.Origin(),
		VirtualHost: request.Destination(),
	}
	if req.Limit, err = strconv.Atoi(limit); err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("limit %q is invalid format", limit)),
		}
	}

	// Query the Dataframe.
	if err = rsAPI.PerformBackfill(httpReq.Context(), &req, &res); err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("query.PerformBackfill failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Filter any event that's not from the requested frame out.
	evs := make([]xtools.PDU, 0)

	var ev *types.HeaderedEvent
	for _, ev = range res.Events {
		if ev.FrameID() == frameID {
			evs = append(evs, ev.PDU)
		}
	}

	eventJSONs := []json.RawMessage{}
	for _, e := range xtools.ReverseTopologicalOrdering(
		evs,
		xtools.TopologicalOrderByPrevEvents,
	) {
		eventJSONs = append(eventJSONs, e.JSON())
	}

	// sytest wants these in reversed order, similar to /messages, so reverse them now.
	for i := len(eventJSONs)/2 - 1; i >= 0; i-- {
		opp := len(eventJSONs) - 1 - i
		eventJSONs[i], eventJSONs[opp] = eventJSONs[opp], eventJSONs[i]
	}

	txn := xtools.Transaction{
		Origin:         request.Destination(),
		PDUs:           eventJSONs,
		OriginServerTS: spec.AsTimestamp(time.Now()),
	}

	// Send the events to the client.
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: txn,
	}
}
