package routing

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/fulltext"
	"github.com/withqb/coddy/internal/sqlutil"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
)

// nolint:gocyclo
func Search(req *http.Request, device *api.Device, syncDB storage.Database, fts fulltext.Indexer, from *string, rsAPI dataframeAPI.SyncDataframeAPI) xutil.JSONResponse {
	start := time.Now()
	var (
		searchReq SearchRequest
		err       error
		ctx       = req.Context()
	)
	resErr := httputil.UnmarshalJSONRequest(req, &searchReq)
	if resErr != nil {
		logrus.Error("failed to unmarshal search request")
		return *resErr
	}

	nextBatch := 0
	if from != nil && *from != "" {
		nextBatch, err = strconv.Atoi(*from)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	if searchReq.SearchCategories.FrameEvents.Filter.Limit == 0 {
		searchReq.SearchCategories.FrameEvents.Filter.Limit = 5
	}

	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	// only search frames the user is actually joined to
	joinedFrames, err := snapshot.FrameIDsWithMembership(ctx, device.UserID, "join")
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if len(joinedFrames) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not joined to any frames."),
		}
	}
	joinedFramesMap := make(map[string]struct{}, len(joinedFrames))
	for _, frameID := range joinedFrames {
		joinedFramesMap[frameID] = struct{}{}
	}
	frames := []string{}
	if searchReq.SearchCategories.FrameEvents.Filter.Frames != nil {
		for _, frameID := range *searchReq.SearchCategories.FrameEvents.Filter.Frames {
			if _, ok := joinedFramesMap[frameID]; ok {
				frames = append(frames, frameID)
			}
		}
	} else {
		frames = joinedFrames
	}

	if len(frames) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Unknown("User not allowed to search in this frame(s)."),
		}
	}

	orderByTime := searchReq.SearchCategories.FrameEvents.OrderBy == "recent"

	result, err := fts.Search(
		searchReq.SearchCategories.FrameEvents.SearchTerm,
		frames,
		searchReq.SearchCategories.FrameEvents.Keys,
		searchReq.SearchCategories.FrameEvents.Filter.Limit,
		nextBatch,
		orderByTime,
	)
	if err != nil {
		logrus.WithError(err).Error("failed to search fulltext")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	logrus.Debugf("Search took %s", result.Took)

	// From was specified but empty, return no results, only the count
	if from != nil && *from == "" {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: SearchResponse{
				SearchCategories: SearchCategoriesResponse{
					FrameEvents: FrameEventsResponse{
						Count:     int(result.Total),
						NextBatch: nil,
					},
				},
			},
		}
	}

	results := []Result{}

	wantEvents := make([]string, 0, len(result.Hits))
	eventScore := make(map[string]*search.DocumentMatch)

	for _, hit := range result.Hits {
		wantEvents = append(wantEvents, hit.ID)
		eventScore[hit.ID] = hit
	}

	// Filter on m.frame.message, as otherwise we also get events like m.reaction
	// which "breaks" displaying results in Element Web.
	types := []string{"m.frame.message"}
	frameFilter := &synctypes.FrameEventFilter{
		Frames: &frames,
		Types: &types,
	}

	evs, err := syncDB.Events(ctx, wantEvents)
	if err != nil {
		logrus.WithError(err).Error("failed to get events from database")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	groups := make(map[string]FrameResult)
	knownUsersProfiles := make(map[string]ProfileInfoResponse)

	// Sort the events by depth, as the returned values aren't ordered
	if orderByTime {
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].Depth() > evs[j].Depth()
		})
	}

	stateForFrames := make(map[string][]synctypes.ClientEvent)
	for _, event := range evs {
		eventsBefore, eventsAfter, err := contextEvents(ctx, snapshot, event, frameFilter, searchReq)
		if err != nil {
			logrus.WithError(err).Error("failed to get context events")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		startToken, endToken, err := getStartEnd(ctx, snapshot, eventsBefore, eventsAfter)
		if err != nil {
			logrus.WithError(err).Error("failed to get start/end")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		profileInfos := make(map[string]ProfileInfoResponse)
		for _, ev := range append(eventsBefore, eventsAfter...) {
			validFrameID, frameErr := spec.NewFrameID(ev.FrameID())
			if err != nil {
				logrus.WithError(frameErr).WithField("frame_id", ev.FrameID()).Warn("failed to query userprofile")
				continue
			}
			userID, queryErr := rsAPI.QueryUserIDForSender(req.Context(), *validFrameID, ev.SenderID())
			if queryErr != nil {
				logrus.WithError(queryErr).WithField("sender_id", ev.SenderID()).Warn("failed to query userprofile")
				continue
			}

			profile, ok := knownUsersProfiles[userID.String()]
			if !ok {
				stateEvent, stateErr := snapshot.GetStateEvent(ctx, ev.FrameID(), spec.MFrameMember, string(ev.SenderID()))
				if stateErr != nil {
					logrus.WithError(stateErr).WithField("sender_id", event.SenderID()).Warn("failed to query userprofile")
					continue
				}
				if stateEvent == nil {
					continue
				}
				profile = ProfileInfoResponse{
					AvatarURL:   gjson.GetBytes(stateEvent.Content(), "avatar_url").Str,
					DisplayName: gjson.GetBytes(stateEvent.Content(), "displayname").Str,
				}
				knownUsersProfiles[userID.String()] = profile
			}
			profileInfos[userID.String()] = profile
		}

		sender := spec.UserID{}
		validFrameID, frameErr := spec.NewFrameID(event.FrameID())
		if err != nil {
			logrus.WithError(frameErr).WithField("frame_id", event.FrameID()).Warn("failed to query userprofile")
			continue
		}
		userID, err := rsAPI.QueryUserIDForSender(req.Context(), *validFrameID, event.SenderID())
		if err == nil && userID != nil {
			sender = *userID
		}

		sk := event.StateKey()
		if sk != nil && *sk != "" {
			skUserID, err := rsAPI.QueryUserIDForSender(req.Context(), *validFrameID, spec.SenderID(*event.StateKey()))
			if err == nil && skUserID != nil {
				skString := skUserID.String()
				sk = &skString
			}
		}
		results = append(results, Result{
			Context: SearchContextResponse{
				Start: startToken.String(),
				End:   endToken.String(),
				EventsAfter: synctypes.ToClientEvents(xtools.ToPDUs(eventsAfter), synctypes.FormatSync, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
					return rsAPI.QueryUserIDForSender(req.Context(), frameID, senderID)
				}),
				EventsBefore: synctypes.ToClientEvents(xtools.ToPDUs(eventsBefore), synctypes.FormatSync, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
					return rsAPI.QueryUserIDForSender(req.Context(), frameID, senderID)
				}),
				ProfileInfo: profileInfos,
			},
			Rank:   eventScore[event.EventID()].Score,
			Result: synctypes.ToClientEvent(event, synctypes.FormatAll, sender.String(), sk, event.Unsigned()),
		})
		frameGroup := groups[event.FrameID()]
		frameGroup.Results = append(frameGroup.Results, event.EventID())
		groups[event.FrameID()] = frameGroup
		if _, ok := stateForFrames[event.FrameID()]; searchReq.SearchCategories.FrameEvents.IncludeState && !ok {
			stateFilter := synctypes.DefaultStateFilter()
			state, err := snapshot.CurrentState(ctx, event.FrameID(), &stateFilter, nil)
			if err != nil {
				logrus.WithError(err).Error("unable to get current state")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			stateForFrames[event.FrameID()] = synctypes.ToClientEvents(xtools.ToPDUs(state), synctypes.FormatSync, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return rsAPI.QueryUserIDForSender(req.Context(), frameID, senderID)
			})
		}
	}

	var nextBatchResult *string = nil
	if int(result.Total) > nextBatch+len(results) {
		nb := strconv.Itoa(len(results) + nextBatch)
		nextBatchResult = &nb
	} else if int(result.Total) == nextBatch+len(results) {
		// Sytest expects a next_batch even if we don't actually have any more results
		nb := ""
		nextBatchResult = &nb
	}

	res := SearchResponse{
		SearchCategories: SearchCategoriesResponse{
			FrameEvents: FrameEventsResponse{
				Count:      int(result.Total),
				Groups:     Groups{FrameID: groups},
				Results:    results,
				NextBatch:  nextBatchResult,
				Highlights: fts.GetHighlights(result),
				State:      stateForFrames,
			},
		},
	}

	logrus.Debugf("Full search request took %v", time.Since(start))

	succeeded = true
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}

// contextEvents returns the events around a given eventID
func contextEvents(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	event *types.HeaderedEvent,
	frameFilter *synctypes.FrameEventFilter,
	searchReq SearchRequest,
) ([]*types.HeaderedEvent, []*types.HeaderedEvent, error) {
	id, _, err := snapshot.SelectContextEvent(ctx, event.FrameID(), event.EventID())
	if err != nil {
		logrus.WithError(err).Error("failed to query context event")
		return nil, nil, err
	}
	frameFilter.Limit = searchReq.SearchCategories.FrameEvents.EventContext.BeforeLimit
	eventsBefore, err := snapshot.SelectContextBeforeEvent(ctx, id, event.FrameID(), frameFilter)
	if err != nil {
		logrus.WithError(err).Error("failed to query before context event")
		return nil, nil, err
	}
	frameFilter.Limit = searchReq.SearchCategories.FrameEvents.EventContext.AfterLimit
	_, eventsAfter, err := snapshot.SelectContextAfterEvent(ctx, id, event.FrameID(), frameFilter)
	if err != nil {
		logrus.WithError(err).Error("failed to query after context event")
		return nil, nil, err
	}
	return eventsBefore, eventsAfter, err
}

type EventContext struct {
	AfterLimit     int  `json:"after_limit,omitempty"`
	BeforeLimit    int  `json:"before_limit,omitempty"`
	IncludeProfile bool `json:"include_profile,omitempty"`
}

type GroupBy struct {
	Key string `json:"key"`
}

type Groupings struct {
	GroupBy []GroupBy `json:"group_by"`
}

type FrameEvents struct {
	EventContext EventContext              `json:"event_context"`
	Filter       synctypes.FrameEventFilter `json:"filter"`
	Groupings    Groupings                 `json:"groupings"`
	IncludeState bool                      `json:"include_state"`
	Keys         []string                  `json:"keys"`
	OrderBy      string                    `json:"order_by"`
	SearchTerm   string                    `json:"search_term"`
}

type SearchCategories struct {
	FrameEvents FrameEvents `json:"frame_events"`
}

type SearchRequest struct {
	SearchCategories SearchCategories `json:"search_categories"`
}

type SearchResponse struct {
	SearchCategories SearchCategoriesResponse `json:"search_categories"`
}
type FrameResult struct {
	NextBatch *string  `json:"next_batch,omitempty"`
	Order     int      `json:"order"`
	Results   []string `json:"results"`
}

type Groups struct {
	FrameID map[string]FrameResult `json:"frame_id"`
}

type Result struct {
	Context SearchContextResponse `json:"context"`
	Rank    float64               `json:"rank"`
	Result  synctypes.ClientEvent `json:"result"`
}

type SearchContextResponse struct {
	End          string                         `json:"end"`
	EventsAfter  []synctypes.ClientEvent        `json:"events_after"`
	EventsBefore []synctypes.ClientEvent        `json:"events_before"`
	Start        string                         `json:"start"`
	ProfileInfo  map[string]ProfileInfoResponse `json:"profile_info"`
}

type ProfileInfoResponse struct {
	AvatarURL   string `json:"avatar_url"`
	DisplayName string `json:"display_name"`
}

type FrameEventsResponse struct {
	Count      int                                `json:"count"`
	Groups     Groups                             `json:"groups"`
	Highlights []string                           `json:"highlights"`
	NextBatch  *string                            `json:"next_batch,omitempty"`
	Results    []Result                           `json:"results"`
	State      map[string][]synctypes.ClientEvent `json:"state,omitempty"`
}
type SearchCategoriesResponse struct {
	FrameEvents FrameEventsResponse `json:"frame_events"`
}
