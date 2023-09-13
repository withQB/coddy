// Copyright 2017 New Vector Ltd
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
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetEvent returns the requested event
func GetEvent(
	ctx context.Context,
	request *fclient.FederationRequest,
	rsAPI api.FederationDataframeAPI,
	eventID string,
	origin spec.ServerName,
) xutil.JSONResponse {
	// /_coddy/federation/v1/event/{eventId} doesn't have a frameID, we use an empty string,
	// which results in `QueryEventsByID` to first get the event and use that to determine the frameID.
	event, err := fetchEvent(ctx, rsAPI, "", eventID)
	if err != nil {
		return *err
	}

	err = allowedToSeeEvent(ctx, request.Origin(), rsAPI, eventID, event.FrameID())
	if err != nil {
		return *err
	}

	return xutil.JSONResponse{Code: http.StatusOK, JSON: xtools.Transaction{
		Origin:         origin,
		OriginServerTS: spec.AsTimestamp(time.Now()),
		PDUs: []json.RawMessage{
			event.JSON(),
		},
	}}
}

// allowedToSeeEvent returns no error if the server is allowed to see this event,
// otherwise it returns an error response which can be sent to the client.
func allowedToSeeEvent(
	ctx context.Context,
	origin spec.ServerName,
	rsAPI api.FederationDataframeAPI,
	eventID string,
	frameID string,
) *xutil.JSONResponse {
	allowed, err := rsAPI.QueryServerAllowedToSeeEvent(ctx, origin, eventID, frameID)
	if err != nil {
		resErr := xutil.ErrorResponse(err)
		return &resErr
	}

	if !allowed {
		resErr := xutil.MessageResponse(http.StatusForbidden, "server not allowed to see event")
		return &resErr
	}

	return nil
}

// fetchEvent fetches the event without auth checks. Returns an error if the event cannot be found.
func fetchEvent(ctx context.Context, rsAPI api.FederationDataframeAPI, frameID, eventID string) (xtools.PDU, *xutil.JSONResponse) {
	var eventsResponse api.QueryEventsByIDResponse
	err := rsAPI.QueryEventsByID(
		ctx,
		&api.QueryEventsByIDRequest{EventIDs: []string{eventID}, FrameID: frameID},
		&eventsResponse,
	)
	if err != nil {
		resErr := xutil.ErrorResponse(err)
		return nil, &resErr
	}

	if len(eventsResponse.Events) == 0 {
		return nil, &xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Event not found"),
		}
	}

	return eventsResponse.Events[0].PDU, nil
}
