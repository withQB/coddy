package routing

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/types"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// For storing pagination information for frame hierarchies
type FrameHierarchyPaginationCache struct {
	cache map[string]dataframeAPI.FrameHierarchyWalker
	mu    sync.Mutex
}

// Create a new, empty, pagination cache.
func NewFrameHierarchyPaginationCache() FrameHierarchyPaginationCache {
	return FrameHierarchyPaginationCache{
		cache: map[string]dataframeAPI.FrameHierarchyWalker{},
	}
}

// Get a cached page, or nil if there is no associated page in the cache.
func (c *FrameHierarchyPaginationCache) Get(token string) *dataframeAPI.FrameHierarchyWalker {
	c.mu.Lock()
	defer c.mu.Unlock()
	line, ok := c.cache[token]
	if ok {
		return &line
	} else {
		return nil
	}
}

// Add a cache line to the pagination cache.
func (c *FrameHierarchyPaginationCache) AddLine(line dataframeAPI.FrameHierarchyWalker) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	token := uuid.NewString()
	c.cache[token] = line
	return token
}

// Query the hierarchy of a frame/space
//
// Implements /_coddy/client/v1/frames/{frameID}/hierarchy
func QueryFrameHierarchy(req *http.Request, device *userapi.Device, frameIDStr string, rsAPI dataframeAPI.ClientDataframeAPI, paginationCache *FrameHierarchyPaginationCache) xutil.JSONResponse {
	parsedFrameID, err := spec.NewFrameID(frameIDStr)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.InvalidParam("frame is unknown/forbidden"),
		}
	}
	frameID := *parsedFrameID

	suggestedOnly := false // Defaults to false (spec-defined)
	switch req.URL.Query().Get("suggested_only") {
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

	limit := 1000 // Default to 1000
	limitStr := req.URL.Query().Get("limit")
	if limitStr != "" {
		var maybeLimit int
		maybeLimit, err = strconv.Atoi(limitStr)
		if err != nil || maybeLimit < 0 {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("query parameter 'limit', if set, must be a positive integer"),
			}
		}
		limit = maybeLimit
		if limit > 1000 {
			limit = 1000 // Maximum limit of 1000
		}
	}

	maxDepth := -1 // '-1' representing no maximum depth
	maxDepthStr := req.URL.Query().Get("max_depth")
	if maxDepthStr != "" {
		var maybeMaxDepth int
		maybeMaxDepth, err = strconv.Atoi(maxDepthStr)
		if err != nil || maybeMaxDepth < 0 {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("query parameter 'max_depth', if set, must be a positive integer"),
			}
		}
		maxDepth = maybeMaxDepth
	}

	from := req.URL.Query().Get("from")

	var walker dataframeAPI.FrameHierarchyWalker
	if from == "" { // No pagination token provided, so start new hierarchy walker
		walker = dataframeAPI.NewFrameHierarchyWalker(types.NewDeviceNotServerName(*device), frameID, suggestedOnly, maxDepth)
	} else { // Attempt to resume cached walker
		cachedWalker := paginationCache.Get(from)

		if cachedWalker == nil || cachedWalker.SuggestedOnly != suggestedOnly || cachedWalker.MaxDepth != maxDepth {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("pagination not found for provided token ('from') with given 'max_depth', 'suggested_only' and frame ID"),
			}
		}

		walker = *cachedWalker
	}

	discoveredFrames, nextWalker, err := rsAPI.QueryNextFrameHierarchyPage(req.Context(), walker, limit)

	if err != nil {
		switch err.(type) {
		case dataframeAPI.ErrFrameUnknownOrNotAllowed:
			xutil.GetLogger(req.Context()).WithError(err).Debugln("frame unknown/forbidden when handling CS frame hierarchy request")
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("frame is unknown/forbidden"),
			}
		default:
			log.WithError(err).Errorf("failed to fetch next page of frame hierarchy (CS API)")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.Unknown("internal server error"),
			}
		}
	}

	nextBatch := ""
	// nextWalker will be nil if there's no more frames left to walk
	if nextWalker != nil {
		nextBatch = paginationCache.AddLine(*nextWalker)
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: FrameHierarchyClientResponse{
			Frames:     discoveredFrames,
			NextBatch: nextBatch,
		},
	}

}

// Success response for /_coddy/client/v1/frames/{frameID}/hierarchy
type FrameHierarchyClientResponse struct {
	Frames     []fclient.FrameHierarchyFrame `json:"frames"`
	NextBatch string                      `json:"next_batch,omitempty"`
}
