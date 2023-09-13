// Copyright 2021 The Coddy.org Foundation C.I.C.
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

	"github.com/withqb/coddy/services/dataframe/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetAliases implements GET /_coddy/client/r0/frames/{frameId}/aliases
func GetAliases(
	req *http.Request, rsAPI api.ClientDataframeAPI, device *userapi.Device, frameID string,
) xutil.JSONResponse {
	stateTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameHistoryVisibility,
		StateKey:  "",
	}
	stateReq := &api.QueryCurrentStateRequest{
		FrameID:      frameID,
		StateTuples: []xtools.StateKeyTuple{stateTuple},
	}
	stateRes := &api.QueryCurrentStateResponse{}
	if err := rsAPI.QueryCurrentState(req.Context(), stateReq, stateRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.QueryCurrentState failed")
		return xutil.ErrorResponse(fmt.Errorf("rsAPI.QueryCurrentState: %w", err))
	}

	visibility := xtools.HistoryVisibilityInvited
	if historyVisEvent, ok := stateRes.StateEvents[stateTuple]; ok {
		var err error
		var content xtools.HistoryVisibilityContent
		if err = json.Unmarshal(historyVisEvent.Content(), &content); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("historyVisEvent.HistoryVisibility failed")
			return xutil.ErrorResponse(fmt.Errorf("historyVisEvent.HistoryVisibility: %w", err))
		}
		visibility = content.HistoryVisibility
	}
	if visibility != spec.WorldReadable {
		deviceUserID, err := spec.NewUserID(device.UserID, true)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("userID doesn't have power level to change visibility"),
			}
		}
		queryReq := api.QueryMembershipForUserRequest{
			FrameID: frameID,
			UserID: *deviceUserID,
		}
		var queryRes api.QueryMembershipForUserResponse
		if err := rsAPI.QueryMembershipForUser(req.Context(), &queryReq, &queryRes); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.QueryMembershipsForFrame failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if !queryRes.IsInFrame {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("You aren't a member of this frame."),
			}
		}
	}

	aliasesReq := api.GetAliasesForFrameIDRequest{
		FrameID: frameID,
	}
	aliasesRes := api.GetAliasesForFrameIDResponse{}
	if err := rsAPI.GetAliasesForFrameID(req.Context(), &aliasesReq, &aliasesRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.GetAliasesForFrameID failed")
		return xutil.ErrorResponse(fmt.Errorf("rsAPI.GetAliasesForFrameID: %w", err))
	}

	response := struct {
		Aliases []string `json:"aliases"`
	}{
		Aliases: aliasesRes.Aliases,
	}
	if response.Aliases == nil {
		response.Aliases = []string{} // pleases sytest
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: response,
	}
}
