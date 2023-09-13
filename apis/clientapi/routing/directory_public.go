package routing

import (
	"context"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/api"
	"github.com/withqb/coddy/apis/clientapi/httputil"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/config"
)

var (
	cacheMu          sync.Mutex
	publicFramesCache []fclient.PublicFrame
)

type PublicFrameReq struct {
	Since              string `json:"since,omitempty"`
	Limit              int64  `json:"limit,omitempty"`
	Filter             filter `json:"filter,omitempty"`
	Server             string `json:"server,omitempty"`
	IncludeAllNetworks bool   `json:"include_all_networks,omitempty"`
	NetworkID          string `json:"third_party_instance_id,omitempty"`
}

type filter struct {
	SearchTerms string   `json:"generic_search_term,omitempty"`
	FrameTypes   []string `json:"frame_types,omitempty"` // TDO: Implement filter on this
}

// GetPostPublicFrames implements GET and POST /publicFrames
func GetPostPublicFrames(
	req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI,
	extFramesProvider api.ExtraPublicFramesProvider,
	federation fclient.FederationClient,
	cfg *config.ClientAPI,
) xutil.JSONResponse {
	var request PublicFrameReq
	if fillErr := fillPublicFramesReq(req, &request); fillErr != nil {
		return *fillErr
	}

	if request.IncludeAllNetworks && request.NetworkID != "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("include_all_networks and third_party_instance_id can not be used together"),
		}
	}

	serverName := spec.ServerName(request.Server)
	if serverName != "" && !cfg.Matrix.IsLocalServerName(serverName) {
		res, err := federation.GetPublicFramesFiltered(
			req.Context(), cfg.Matrix.ServerName, serverName,
			int(request.Limit), request.Since,
			request.Filter.SearchTerms, false,
			"",
		)
		if err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("failed to get public frames")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: res,
		}
	}

	response, err := publicFrames(req.Context(), request, rsAPI, extFramesProvider)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Errorf("failed to work out public frames")
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
	ctx context.Context, request PublicFrameReq, rsAPI dataframeAPI.ClientDataframeAPI, extFramesProvider api.ExtraPublicFramesProvider,
) (*fclient.RespPublicFrames, error) {

	response := fclient.RespPublicFrames{
		Chunk: []fclient.PublicFrame{},
	}
	var limit int64
	var offset int64
	limit = request.Limit
	if limit == 0 {
		limit = 50
	}
	offset, err := strconv.ParseInt(request.Since, 10, 64)
	// ParseInt returns 0 and an error when trying to parse an empty string
	// In that case, we want to assign 0 so we ignore the error
	if err != nil && len(request.Since) > 0 {
		xutil.GetLogger(ctx).WithError(err).Error("strconv.ParseInt failed")
		return nil, err
	}
	err = nil

	var frames []fclient.PublicFrame
	if request.Since == "" {
		frames = refreshPublicFrameCache(ctx, rsAPI, extFramesProvider, request)
	} else {
		frames = getPublicFramesFromCache()
	}

	response.TotalFrameCountEstimate = len(frames)

	frames = filterFrames(frames, request.Filter.SearchTerms)

	chunk, prev, next := sliceInto(frames, offset, limit)
	if prev >= 0 {
		response.PrevBatch = "T" + strconv.Itoa(prev)
	}
	if next >= 0 {
		response.NextBatch = "T" + strconv.Itoa(next)
	}
	if chunk != nil {
		response.Chunk = chunk
	}
	return &response, err
}

func filterFrames(frames []fclient.PublicFrame, searchTerm string) []fclient.PublicFrame {
	if searchTerm == "" {
		return frames
	}

	normalizedTerm := strings.ToLower(searchTerm)

	result := make([]fclient.PublicFrame, 0)
	for _, frame := range frames {
		if strings.Contains(strings.ToLower(frame.Name), normalizedTerm) ||
			strings.Contains(strings.ToLower(frame.Topic), normalizedTerm) ||
			strings.Contains(strings.ToLower(frame.CanonicalAlias), normalizedTerm) {
			result = append(result, frame)
		}
	}

	return result
}

// fillPublicFramesReq fills the Limit, Since and Filter attributes of a GET or POST request
// on /publicFrames by parsing the incoming HTTP request
// Filter is only filled for POST requests
func fillPublicFramesReq(httpReq *http.Request, request *PublicFrameReq) *xutil.JSONResponse {
	if httpReq.Method != "GET" && httpReq.Method != "POST" {
		return &xutil.JSONResponse{
			Code: http.StatusMethodNotAllowed,
			JSON: spec.NotFound("Bad method"),
		}
	}
	if httpReq.Method == "GET" {
		limit, err := strconv.Atoi(httpReq.FormValue("limit"))
		// Atoi returns 0 and an error when trying to parse an empty string
		// In that case, we want to assign 0 so we ignore the error
		if err != nil && len(httpReq.FormValue("limit")) > 0 {
			xutil.GetLogger(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
			return &xutil.JSONResponse{
				Code: 400,
				JSON: spec.BadJSON("limit param is not a number"),
			}
		}
		request.Limit = int64(limit)
		request.Since = httpReq.FormValue("since")
		request.Server = httpReq.FormValue("server")
	} else {
		resErr := httputil.UnmarshalJSONRequest(httpReq, request)
		if resErr != nil {
			return resErr
		}
		request.Server = httpReq.FormValue("server")
	}

	// strip the 'T' which is only required because when sytest does pagination tests it stops
	// iterating when !prev_batch which then fails if prev_batch==0, so add arbitrary text to
	// make it truthy not falsey.
	request.Since = strings.TrimPrefix(request.Since, "T")
	return nil
}

// sliceInto returns a subslice of `slice` which honours the since/limit values given.
//
//	  0  1  2  3  4  5  6   index
//	 [A, B, C, D, E, F, G]  slice
//
//	 limit=3          => A,B,C (prev='', next='3')
//	 limit=3&since=3  => D,E,F (prev='0', next='6')
//	 limit=3&since=6  => G     (prev='3', next='')
//
//	A value of '-1' for prev/next indicates no position.
func sliceInto(slice []fclient.PublicFrame, since int64, limit int64) (subset []fclient.PublicFrame, prev, next int) {
	prev = -1
	next = -1

	if since > 0 {
		prev = int(since) - int(limit)
	}
	nextIndex := int(since) + int(limit)
	if len(slice) > nextIndex { // there are more frames ahead of us
		next = nextIndex
	}

	// apply sanity caps
	if since < 0 {
		since = 0
	}
	if nextIndex > len(slice) {
		nextIndex = len(slice)
	}

	subset = slice[since:nextIndex]
	return
}

func refreshPublicFrameCache(
	ctx context.Context, rsAPI dataframeAPI.ClientDataframeAPI, extFramesProvider api.ExtraPublicFramesProvider,
	request PublicFrameReq,
) []fclient.PublicFrame {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	var extraFrames []fclient.PublicFrame
	if extFramesProvider != nil {
		extraFrames = extFramesProvider.Frames()
	}

	// TDO: this is only here to make Sytest happy, for now.
	ns := strings.Split(request.NetworkID, "|")
	if len(ns) == 2 {
		request.NetworkID = ns[1]
	}

	var queryRes dataframeAPI.QueryPublishedFramesResponse
	err := rsAPI.QueryPublishedFrames(ctx, &dataframeAPI.QueryPublishedFramesRequest{
		NetworkID:          request.NetworkID,
		IncludeAllNetworks: request.IncludeAllNetworks,
	}, &queryRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryPublishedFrames failed")
		return publicFramesCache
	}
	pubFrames, err := dataframeAPI.PopulatePublicFrames(ctx, queryRes.FrameIDs, rsAPI)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("PopulatePublicFrames failed")
		return publicFramesCache
	}
	publicFramesCache = []fclient.PublicFrame{}
	publicFramesCache = append(publicFramesCache, pubFrames...)
	publicFramesCache = append(publicFramesCache, extraFrames...)
	publicFramesCache = dedupeAndShuffle(publicFramesCache)

	// sort by total joined member count (big to small)
	sort.SliceStable(publicFramesCache, func(i, j int) bool {
		return publicFramesCache[i].JoinedMembersCount > publicFramesCache[j].JoinedMembersCount
	})
	return publicFramesCache
}

func getPublicFramesFromCache() []fclient.PublicFrame {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	return publicFramesCache
}

func dedupeAndShuffle(in []fclient.PublicFrame) []fclient.PublicFrame {
	// de-duplicate frames with the same frame ID. We can join the frame via any of these aliases as we know these servers
	// are alive and well, so we arbitrarily pick one (purposefully shuffling them to spread the load a bit)
	var publicFrames []fclient.PublicFrame
	haveFrameIDs := make(map[string]bool)
	rand.Shuffle(len(in), func(i, j int) {
		in[i], in[j] = in[j], in[i]
	})
	for _, r := range in {
		if haveFrameIDs[r.FrameID] {
			continue
		}
		haveFrameIDs[r.FrameID] = true
		publicFrames = append(publicFrames, r)
	}
	return publicFrames
}
