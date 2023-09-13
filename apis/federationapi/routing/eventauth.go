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

	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/types"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetEventAuth returns event auth for the roomID and eventID
func GetEventAuth(
	ctx context.Context,
	request *fclient.FederationRequest,
	rsAPI api.FederationRoomserverAPI,
	roomID string,
	eventID string,
) xutil.JSONResponse {
	// If we don't think we belong to this room then don't waste the effort
	// responding to expensive requests for it.
	if err := ErrorIfLocalServerNotInRoom(ctx, rsAPI, roomID); err != nil {
		return *err
	}

	event, resErr := fetchEvent(ctx, rsAPI, roomID, eventID)
	if resErr != nil {
		return *resErr
	}

	if event.RoomID() != roomID {
		return xutil.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("event does not belong to this room")}
	}
	resErr = allowedToSeeEvent(ctx, request.Origin(), rsAPI, eventID, event.RoomID())
	if resErr != nil {
		return *resErr
	}

	var response api.QueryStateAndAuthChainResponse
	err := rsAPI.QueryStateAndAuthChain(
		ctx,
		&api.QueryStateAndAuthChainRequest{
			RoomID:             roomID,
			PrevEventIDs:       []string{eventID},
			AuthEventIDs:       event.AuthEventIDs(),
			OnlyFetchAuthChain: true,
		},
		&response,
	)
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	if !response.RoomExists {
		return xutil.JSONResponse{Code: http.StatusNotFound, JSON: nil}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespEventAuth{
			AuthEvents: types.NewEventJSONsFromHeaderedEvents(response.AuthChainEvents),
		},
	}
}
