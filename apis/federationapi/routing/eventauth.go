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
	"net/http"

	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetEventAuth returns event auth for the frameID and eventID
func GetEventAuth(
	ctx context.Context,
	request *fclient.FederationRequest,
	rsAPI api.FederationDataframeAPI,
	frameID string,
	eventID string,
) xutil.JSONResponse {
	// If we don't think we belong to this frame then don't waste the effort
	// responding to expensive requests for it.
	if err := ErrorIfLocalServerNotInFrame(ctx, rsAPI, frameID); err != nil {
		return *err
	}

	event, resErr := fetchEvent(ctx, rsAPI, frameID, eventID)
	if resErr != nil {
		return *resErr
	}

	if event.FrameID() != frameID {
		return xutil.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("event does not belong to this frame")}
	}
	resErr = allowedToSeeEvent(ctx, request.Origin(), rsAPI, eventID, event.FrameID())
	if resErr != nil {
		return *resErr
	}

	var response api.QueryStateAndAuthChainResponse
	err := rsAPI.QueryStateAndAuthChain(
		ctx,
		&api.QueryStateAndAuthChainRequest{
			FrameID:             frameID,
			PrevEventIDs:       []string{eventID},
			AuthEventIDs:       event.AuthEventIDs(),
			OnlyFetchAuthChain: true,
		},
		&response,
	)
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	if !response.FrameExists {
		return xutil.JSONResponse{Code: http.StatusNotFound, JSON: nil}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespEventAuth{
			AuthEvents: types.NewEventJSONsFromHeaderedEvents(response.AuthChainEvents),
		},
	}
}
