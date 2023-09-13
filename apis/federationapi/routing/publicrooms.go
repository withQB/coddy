package routing

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
)

type PublicFrameReq struct {
	Since              string `json:"since,omitempty"`
	Limit              int16  `json:"limit,omitempty"`
	Filter             filter `json:"filter,omitempty"`
	IncludeAllNetworks bool   `json:"include_all_networks,omitempty"`
	NetworkID          string `json:"third_party_instance_id,omitempty"`
}

type filter struct {
	SearchTerms string   `json:"generic_search_term,omitempty"`
	FrameTypes   []string `json:"frame_types,omitempty"`
}

// GetPostPublicFrames implements GET and POST /publicFrames
func GetPostPublicFrames(req *http.Request, rsAPI dataframeAPI.FederationDataframeAPI) xutil.JSONResponse {
	var request PublicFrameReq
	if fillErr := fillPublicFramesReq(req, &request); fillErr != nil {
		return *fillErr
	}
	if request.Limit == 0 {
		request.Limit = 50
	}
	response, err := publicFrames(req.Context(), request, rsAPI)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}

func publicFrames(
	ctx context.Context, request PublicFrameReq, rsAPI dataframeAPI.FederationDataframeAPI,
) (*fclient.RespPublicFrames, error) {

	var response fclient.RespPublicFrames
	var limit int16
	var offset int64
	limit = request.Limit
	offset, err := strconv.ParseInt(request.Since, 10, 64)
	// ParseInt returns 0 and an error when trying to parse an empty string
	// In that case, we want to assign 0 so we ignore the error
	if err != nil && len(request.Since) > 0 {
		xutil.GetLogger(ctx).WithError(err).Error("strconv.ParseInt failed")
		return nil, err
	}

	if request.IncludeAllNetworks && request.NetworkID != "" {
		return nil, fmt.Errorf("include_all_networks and third_party_instance_id can not be used together")
	}

	var queryRes dataframeAPI.QueryPublishedFramesResponse
	err = rsAPI.QueryPublishedFrames(ctx, &dataframeAPI.QueryPublishedFramesRequest{
		NetworkID: request.NetworkID,
	}, &queryRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryPublishedFrames failed")
		return nil, err
	}
	response.TotalFrameCountEstimate = len(queryRes.FrameIDs)

	if offset > 0 {
		response.PrevBatch = strconv.Itoa(int(offset) - 1)
	}
	nextIndex := int(offset) + int(limit)
	if response.TotalFrameCountEstimate > nextIndex {
		response.NextBatch = strconv.Itoa(nextIndex)
	}

	if offset < 0 {
		offset = 0
	}
	if nextIndex > len(queryRes.FrameIDs) {
		nextIndex = len(queryRes.FrameIDs)
	}
	frameIDs := queryRes.FrameIDs[offset:nextIndex]
	response.Chunk, err = fillInFrames(ctx, frameIDs, rsAPI)
	return &response, err
}

// fillPublicFramesReq fills the Limit, Since and Filter attributes of a GET or POST request
// on /publicFrames by parsing the incoming HTTP request
// Filter is only filled for POST requests
func fillPublicFramesReq(httpReq *http.Request, request *PublicFrameReq) *xutil.JSONResponse {
	if httpReq.Method == http.MethodGet {
		limit, err := strconv.Atoi(httpReq.FormValue("limit"))
		// Atoi returns 0 and an error when trying to parse an empty string
		// In that case, we want to assign 0 so we ignore the error
		if err != nil && len(httpReq.FormValue("limit")) > 0 {
			xutil.GetLogger(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
			return &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		request.Limit = int16(limit)
		request.Since = httpReq.FormValue("since")
		return nil
	} else if httpReq.Method == http.MethodPost {
		return httputil.UnmarshalJSONRequest(httpReq, request)
	}

	return &xutil.JSONResponse{
		Code: http.StatusMethodNotAllowed,
		JSON: spec.NotFound("Bad method"),
	}
}

// due to lots of switches
func fillInFrames(ctx context.Context, frameIDs []string, rsAPI dataframeAPI.FederationDataframeAPI) ([]fclient.PublicFrame, error) {
	avatarTuple := xtools.StateKeyTuple{EventType: "m.frame.avatar", StateKey: ""}
	nameTuple := xtools.StateKeyTuple{EventType: "m.frame.name", StateKey: ""}
	canonicalTuple := xtools.StateKeyTuple{EventType: spec.MFrameCanonicalAlias, StateKey: ""}
	topicTuple := xtools.StateKeyTuple{EventType: "m.frame.topic", StateKey: ""}
	guestTuple := xtools.StateKeyTuple{EventType: "m.frame.guest_access", StateKey: ""}
	visibilityTuple := xtools.StateKeyTuple{EventType: spec.MFrameHistoryVisibility, StateKey: ""}
	joinRuleTuple := xtools.StateKeyTuple{EventType: spec.MFrameJoinRules, StateKey: ""}

	var stateRes dataframeAPI.QueryBulkStateContentResponse
	err := rsAPI.QueryBulkStateContent(ctx, &dataframeAPI.QueryBulkStateContentRequest{
		FrameIDs:        frameIDs,
		AllowWildcards: true,
		StateTuples: []xtools.StateKeyTuple{
			nameTuple, canonicalTuple, topicTuple, guestTuple, visibilityTuple, joinRuleTuple, avatarTuple,
			{EventType: spec.MFrameMember, StateKey: "*"},
		},
	}, &stateRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryBulkStateContent failed")
		return nil, err
	}
	chunk := make([]fclient.PublicFrame, len(frameIDs))
	i := 0
	for frameID, data := range stateRes.Frames {
		pub := fclient.PublicFrame{
			FrameID: frameID,
		}
		joinCount := 0
		var joinRule, guestAccess string
		for tuple, contentVal := range data {
			if tuple.EventType == spec.MFrameMember && contentVal == "join" {
				joinCount++
				continue
			}
			switch tuple {
			case avatarTuple:
				pub.AvatarURL = contentVal
			case nameTuple:
				pub.Name = contentVal
			case topicTuple:
				pub.Topic = contentVal
			case canonicalTuple:
				if _, _, err := xtools.SplitID('#', contentVal); err == nil {
					pub.CanonicalAlias = contentVal
				}
			case visibilityTuple:
				pub.WorldReadable = contentVal == "world_readable"
			// need both of these to determine whether guests can join
			case joinRuleTuple:
				joinRule = contentVal
			case guestTuple:
				guestAccess = contentVal
			}
		}
		if joinRule == spec.Public && guestAccess == "can_join" {
			pub.GuestCanJoin = true
		}
		pub.JoinedMembersCount = joinCount
		chunk[i] = pub
		i++
	}
	return chunk, nil
}
