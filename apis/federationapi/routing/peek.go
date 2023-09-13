// Copyright 2020 New Vector Ltd
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
	"net/http"

	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// Peek implements the SS /peek API, handling inbound peeks
func Peek(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID, peekID string,
	remoteVersions []xtools.RoomVersion,
) xutil.JSONResponse {
	// TODO: check if we're just refreshing an existing peek by querying the federationapi
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Check that the room that the peeking server is trying to peek is actually
	// one of the room versions that they listed in their supported ?ver= in
	// the peek URL.
	remoteSupportsVersion := false
	for _, v := range remoteVersions {
		if v == roomVersion {
			remoteSupportsVersion = true
			break
		}
	}
	// If it isn't, stop trying to peek the room.
	if !remoteSupportsVersion {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.IncompatibleRoomVersion(string(roomVersion)),
		}
	}

	// TODO: Check history visibility

	// tell the peeking server to renew every hour
	renewalInterval := int64(60 * 60 * 1000 * 1000)

	var response api.PerformInboundPeekResponse
	err = rsAPI.PerformInboundPeek(
		httpReq.Context(),
		&api.PerformInboundPeekRequest{
			RoomID:          roomID,
			PeekID:          peekID,
			ServerName:      request.Origin(),
			RenewalInterval: renewalInterval,
		},
		&response,
	)
	if err != nil {
		resErr := xutil.ErrorResponse(err)
		return resErr
	}

	if !response.RoomExists {
		return xutil.JSONResponse{Code: http.StatusNotFound, JSON: nil}
	}

	respPeek := fclient.RespPeek{
		StateEvents:     types.NewEventJSONsFromHeaderedEvents(response.StateEvents),
		AuthEvents:      types.NewEventJSONsFromHeaderedEvents(response.AuthChainEvents),
		RoomVersion:     response.RoomVersion,
		LatestEvent:     response.LatestEvent.PDU,
		RenewalInterval: renewalInterval,
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: respPeek,
	}
}
