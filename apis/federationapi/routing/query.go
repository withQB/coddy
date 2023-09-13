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
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"
	federationAPI "github.com/withqb/coddy/apis/federationapi/api"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// FrameAliasToID converts the queried alias into a frame ID and returns it
func FrameAliasToID(
	httpReq *http.Request,
	federation fclient.FederationClient,
	cfg *config.FederationAPI,
	rsAPI dataframeAPI.FederationDataframeAPI,
	senderAPI federationAPI.FederationInternalAPI,
) xutil.JSONResponse {
	frameAlias := httpReq.FormValue("frame_alias")
	if frameAlias == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Must supply frame alias parameter."),
		}
	}
	_, domain, err := xtools.SplitID('#', frameAlias)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Frame alias must be in the form '#localpart:domain'"),
		}
	}

	var resp fclient.RespDirectory

	if domain == cfg.Matrix.ServerName {
		queryReq := &dataframeAPI.GetFrameIDForAliasRequest{
			Alias:              frameAlias,
			IncludeAppservices: true,
		}
		queryRes := &dataframeAPI.GetFrameIDForAliasResponse{}
		if err = rsAPI.GetFrameIDForAlias(httpReq.Context(), queryReq, queryRes); err != nil {
			xutil.GetLogger(httpReq.Context()).WithError(err).Error("aliasAPI.GetFrameIDForAlias failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		if queryRes.FrameID != "" {
			serverQueryReq := federationAPI.QueryJoinedHostServerNamesInFrameRequest{FrameID: queryRes.FrameID}
			var serverQueryRes federationAPI.QueryJoinedHostServerNamesInFrameResponse
			if err = senderAPI.QueryJoinedHostServerNamesInFrame(httpReq.Context(), &serverQueryReq, &serverQueryRes); err != nil {
				xutil.GetLogger(httpReq.Context()).WithError(err).Error("senderAPI.QueryJoinedHostServerNamesInFrame failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			resp = fclient.RespDirectory{
				FrameID:  queryRes.FrameID,
				Servers: serverQueryRes.ServerNames,
			}
		} else {
			// If no alias was found, return an error
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(fmt.Sprintf("Frame alias %s not found", frameAlias)),
			}
		}
	} else {
		resp, err = federation.LookupFrameAlias(httpReq.Context(), domain, cfg.Matrix.ServerName, frameAlias)
		if err != nil {
			switch x := err.(type) {
			case xcore.HTTPError:
				if x.Code == http.StatusNotFound {
					return xutil.JSONResponse{
						Code: http.StatusNotFound,
						JSON: spec.NotFound("Frame alias not found"),
					}
				}
			}
			// TODO: Return 502 if the remote server errored.
			// TODO: Return 504 if the remote server timed out.
			xutil.GetLogger(httpReq.Context()).WithError(err).Error("federation.LookupFrameAlias failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}

// Query the immediate children of a frame/space
//
// Implements /_coddy/federation/v1/hierarchy/{frameID}
func QueryFrameHierarchy(httpReq *http.Request, request *fclient.FederationRequest, frameIDStr string, rsAPI dataframeAPI.FederationDataframeAPI) xutil.JSONResponse {
	parsedFrameID, err := spec.NewFrameID(frameIDStr)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.InvalidParam("frame is unknown/forbidden"),
		}
	}
	frameID := *parsedFrameID

	suggestedOnly := false // Defaults to false (spec-defined)
	switch httpReq.URL.Query().Get("suggested_only") {
	case "true":
		suggestedOnly = true
	case "false":
	case "": // Empty string is returned when query param is not set
	default:
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("query parameter 'suggested_only', if set, must be 'true' or 'false'"),
		}
	}

	walker := dataframeAPI.NewFrameHierarchyWalker(types.NewServerNameNotDevice(request.Origin()), frameID, suggestedOnly, 1)
	discoveredFrames, _, err := rsAPI.QueryNextFrameHierarchyPage(httpReq.Context(), walker, -1)

	if err != nil {
		switch err.(type) {
		case dataframeAPI.ErrFrameUnknownOrNotAllowed:
			xutil.GetLogger(httpReq.Context()).WithError(err).Debugln("frame unknown/forbidden when handling SS frame hierarchy request")
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("frame is unknown/forbidden"),
			}
		default:
			log.WithError(err).Errorf("failed to fetch next page of frame hierarchy (SS API)")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("internal server error"),
			}
		}
	}

	if len(discoveredFrames) == 0 {
		xutil.GetLogger(httpReq.Context()).Debugln("no frames found when handling SS frame hierarchy request")
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("frame is unknown/forbidden"),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: fclient.FrameHierarchyResponse{
			Frame:     discoveredFrames[0],
			Children: discoveredFrames[1:],
		},
	}
}
