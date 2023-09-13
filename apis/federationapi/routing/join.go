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
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
)

// MakeJoin implements the /make_join API
func MakeJoin(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationDataframeAPI,
	frameID spec.FrameID, userID spec.UserID,
	remoteVersions []xtools.FrameVersion,
) xutil.JSONResponse {
	frameVersion, err := rsAPI.QueryFrameVersionForFrame(httpReq.Context(), frameID.String())
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("failed obtaining frame version")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	req := api.QueryServerJoinedToFrameRequest{
		ServerName: request.Destination(),
		FrameID:     frameID.String(),
	}
	res := api.QueryServerJoinedToFrameResponse{}
	if err = rsAPI.QueryServerJoinedToFrame(httpReq.Context(), &req, &res); err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryServerJoinedToFrame failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	createJoinTemplate := func(proto *xtools.ProtoEvent) (xtools.PDU, []xtools.PDU, error) {
		identity, signErr := cfg.Matrix.SigningIdentityFor(request.Destination())
		if signErr != nil {
			xutil.GetLogger(httpReq.Context()).WithError(signErr).Errorf("obtaining signing identity for %s failed", request.Destination())
			return nil, nil, spec.NotFound(fmt.Sprintf("Server name %q does not exist", request.Destination()))
		}

		queryRes := api.QueryLatestEventsAndStateResponse{
			FrameVersion: frameVersion,
		}
		event, signErr := eventutil.QueryAndBuildEvent(httpReq.Context(), proto, identity, time.Now(), rsAPI, &queryRes)
		switch e := signErr.(type) {
		case nil:
		case eventutil.ErrFrameNoExists:
			xutil.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.NotFound("Frame does not exist")
		case xtools.BadJSONError:
			xutil.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.BadJSON(e.Error())
		default:
			xutil.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
			return nil, nil, spec.InternalServerError{}
		}

		stateEvents := make([]xtools.PDU, len(queryRes.StateEvents))
		for i, stateEvent := range queryRes.StateEvents {
			stateEvents[i] = stateEvent.PDU
		}
		return event, stateEvents, nil
	}

	frameQuerier := api.JoinFrameQuerier{
		Dataframe: rsAPI,
	}

	senderIDPtr, err := rsAPI.QuerySenderIDForUser(httpReq.Context(), frameID, userID)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QuerySenderIDForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var senderID spec.SenderID
	if senderIDPtr == nil {
		senderID = spec.SenderID(userID.String())
	} else {
		senderID = *senderIDPtr
	}

	input := xtools.HandleMakeJoinInput{
		Context:           httpReq.Context(),
		UserID:            userID,
		SenderID:          senderID,
		FrameID:            frameID,
		FrameVersion:       frameVersion,
		RemoteVersions:    remoteVersions,
		RequestOrigin:     request.Origin(),
		LocalServerName:   cfg.Matrix.ServerName,
		LocalServerInFrame: res.FrameExists && res.IsInFrame,
		FrameQuerier:       &frameQuerier,
		UserIDQuerier: func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), frameID, senderID)
		},
		BuildEventTemplate: createJoinTemplate,
	}
	response, internalErr := xtools.HandleMakeJoin(input)
	switch e := internalErr.(type) {
	case nil:
	case spec.InternalServerError:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.CoddyError:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorNotFound:
			code = http.StatusNotFound
		case spec.ErrorUnableToAuthoriseJoin:
			fallthrough // http.StatusBadRequest
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return xutil.JSONResponse{
			Code: code,
			JSON: e,
		}
	case spec.IncompatibleFrameVersionError:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: e,
		}
	default:
		xutil.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	if response == nil {
		xutil.GetLogger(httpReq.Context()).Error("gmsl.HandleMakeJoin returned invalid response")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]interface{}{
			"event":        response.JoinTemplateEvent,
			"frame_version": response.FrameVersion,
		},
	}
}

// SendJoin implements the /send_join API
func SendJoin(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationDataframeAPI,
	keys xtools.JSONVerifier,
	frameID spec.FrameID,
	eventID string,
) xutil.JSONResponse {
	frameVersion, err := rsAPI.QueryFrameVersionForFrame(httpReq.Context(), frameID.String())
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryFrameVersionForFrame failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	input := xtools.HandleSendJoinInput{
		Context:           httpReq.Context(),
		FrameID:            frameID,
		EventID:           eventID,
		JoinEvent:         request.Content(),
		FrameVersion:       frameVersion,
		RequestOrigin:     request.Origin(),
		LocalServerName:   cfg.Matrix.ServerName,
		KeyID:             cfg.Matrix.KeyID,
		PrivateKey:        cfg.Matrix.PrivateKey,
		Verifier:          keys,
		MembershipQuerier: &api.MembershipQuerier{Dataframe: rsAPI},
		UserIDQuerier: func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(httpReq.Context(), frameID, senderID)
		},
		StoreSenderIDFromPublicID: func(ctx context.Context, senderID spec.SenderID, userIDRaw string, frameID spec.FrameID) error {
			userID, userErr := spec.NewUserID(userIDRaw, true)
			if userErr != nil {
				return userErr
			}
			return rsAPI.StoreUserFramePublicKey(ctx, senderID, *userID, frameID)
		},
	}
	response, joinErr := xtools.HandleSendJoin(input)
	switch e := joinErr.(type) {
	case nil:
	case spec.InternalServerError:
		xutil.GetLogger(httpReq.Context()).WithError(joinErr)
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case spec.CoddyError:
		xutil.GetLogger(httpReq.Context()).WithError(joinErr)
		code := http.StatusInternalServerError
		switch e.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorNotFound:
			code = http.StatusNotFound
		case spec.ErrorUnsupportedFrameVersion:
			code = http.StatusInternalServerError
		case spec.ErrorBadJSON:
			code = http.StatusBadRequest
		}

		return xutil.JSONResponse{
			Code: code,
			JSON: e,
		}
	default:
		xutil.GetLogger(httpReq.Context()).WithError(joinErr)
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	if response == nil {
		xutil.GetLogger(httpReq.Context()).Error("gmsl.HandleMakeJoin returned invalid response")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}

	}

	// Fetch the state and auth chain. We do this before we send the events
	// on, in case this fails.
	var stateAndAuthChainResponse api.QueryStateAndAuthChainResponse
	err = rsAPI.QueryStateAndAuthChain(httpReq.Context(), &api.QueryStateAndAuthChainRequest{
		PrevEventIDs: response.JoinEvent.PrevEventIDs(),
		AuthEventIDs: response.JoinEvent.AuthEventIDs(),
		FrameID:       frameID.String(),
		ResolveState: true,
	}, &stateAndAuthChainResponse)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryStateAndAuthChain failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !stateAndAuthChainResponse.FrameExists {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Frame does not exist"),
		}
	}
	if !stateAndAuthChainResponse.StateKnown {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("State not known"),
		}
	}

	// Send the events to the frame server.
	// We are responsible for notifying other servers that the user has joined
	// the frame, so set SendAsServer to cfg.Matrix.ServerName
	if !response.AlreadyJoined {
		var rsResponse api.InputFrameEventsResponse
		rsAPI.InputFrameEvents(httpReq.Context(), &api.InputFrameEventsRequest{
			InputFrameEvents: []api.InputFrameEvent{
				{
					Kind:          api.KindNew,
					Event:         &types.HeaderedEvent{PDU: response.JoinEvent},
					SendAsServer:  string(cfg.Matrix.ServerName),
					TransactionID: nil,
				},
			},
		}, &rsResponse)
		if rsResponse.ErrMsg != "" {
			xutil.GetLogger(httpReq.Context()).WithField(logrus.ErrorKey, rsResponse.ErrMsg).Error("SendEvents failed")
			if rsResponse.NotAllowed {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.Forbidden(rsResponse.ErrMsg),
				}
			}
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// sort events deterministically by depth (lower is earlier)
	// We also do this because sytest's basic federation server isn't good at using the correct
	// state if these lists are randomised, resulting in flakey tests. :(
	sort.Sort(eventsByDepth(stateAndAuthChainResponse.StateEvents))
	sort.Sort(eventsByDepth(stateAndAuthChainResponse.AuthChainEvents))

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespSendJoin{
			StateEvents: types.NewEventJSONsFromHeaderedEvents(stateAndAuthChainResponse.StateEvents),
			AuthEvents:  types.NewEventJSONsFromHeaderedEvents(stateAndAuthChainResponse.AuthChainEvents),
			Origin:      cfg.Matrix.ServerName,
			Event:       response.JoinEvent.JSON(),
		},
	}
}

type eventsByDepth []*types.HeaderedEvent

func (e eventsByDepth) Len() int {
	return len(e)
}
func (e eventsByDepth) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
func (e eventsByDepth) Less(i, j int) bool {
	return e[i].Depth() < e[j].Depth()
}
