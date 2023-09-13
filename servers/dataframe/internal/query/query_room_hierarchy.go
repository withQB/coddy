package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/tidwall/gjson"
	fs "github.com/withqb/coddy/apis/federationapi/api"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	dataframe "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// Traverse the frame hierarchy using the provided walker up to the provided limit,
// returning a new walker which can be used to fetch the next page.
//
// If limit is -1, this is treated as no limit, and the entire hierarchy will be traversed.
//
// If returned walker is nil, then there are no more frames left to traverse. This method does not modify the provided walker, so it
// can be cached.
func (querier *Queryer) QueryNextFrameHierarchyPage(ctx context.Context, walker dataframe.FrameHierarchyWalker, limit int) ([]fclient.FrameHierarchyFrame, *dataframe.FrameHierarchyWalker, error) {
	if authorised, _ := authorised(ctx, querier, walker.Caller, walker.RootFrameID, nil); !authorised {
		return nil, nil, dataframe.ErrFrameUnknownOrNotAllowed{Err: fmt.Errorf("frame is unknown/forbidden")}
	}

	discoveredFrames := []fclient.FrameHierarchyFrame{}

	// Copy unvisited and processed to avoid modifying original walker (which is typically in cache)
	unvisited := make([]dataframe.FrameHierarchyWalkerQueuedFrame, len(walker.Unvisited))
	copy(unvisited, walker.Unvisited)
	processed := walker.Processed.Copy()

	// Depth first -> stack data structure
	for len(unvisited) > 0 {
		if len(discoveredFrames) >= limit && limit != -1 {
			break
		}

		// pop the stack
		queuedFrame := unvisited[len(unvisited)-1]
		unvisited = unvisited[:len(unvisited)-1]
		// If this frame has already been processed, skip.
		// If this frame exceeds the specified depth, skip.
		if processed.Contains(queuedFrame.FrameID) || (walker.MaxDepth > 0 && queuedFrame.Depth > walker.MaxDepth) {
			continue
		}

		// Mark this frame as processed.
		processed.Add(queuedFrame.FrameID)

		// if this frame is not a space frame, skip.
		var frameType string
		create := stateEvent(ctx, querier, queuedFrame.FrameID, spec.MFrameCreate, "")
		if create != nil {
			var createContent xtools.CreateContent
			err := json.Unmarshal(create.Content(), &createContent)
			if err != nil {
				xutil.GetLogger(ctx).WithError(err).WithField("create_content", create.Content()).Warn("failed to unmarshal m.frame.create event")
			}
			frameType = createContent.FrameType
		}

		// Collect frames/events to send back (either locally or fetched via federation)
		var discoveredChildEvents []fclient.FrameHierarchyStrippedEvent

		// If we know about this frame and the caller is authorised (joined/world_readable) then pull
		// events locally
		frameExists := frameExists(ctx, querier, queuedFrame.FrameID)
		if !frameExists {
			// attempt to query this frame over federation, as either we've never heard of it before
			// or we've left it and hence are not authorised (but info may be exposed regardless)
			fedRes := federatedFrameInfo(ctx, querier, walker.Caller, walker.SuggestedOnly, queuedFrame.FrameID, queuedFrame.Vias)
			if fedRes != nil {
				discoveredChildEvents = fedRes.Frame.ChildrenState
				discoveredFrames = append(discoveredFrames, fedRes.Frame)
				if len(fedRes.Children) > 0 {
					discoveredFrames = append(discoveredFrames, fedRes.Children...)
				}
				// mark this frame as a space frame as the federated server responded.
				// we need to do this so we add the children of this frame to the unvisited stack
				// as these children may be frames we do know about.
				frameType = spec.MSpace
			}
		} else if authorised, isJoinedOrInvited := authorised(ctx, querier, walker.Caller, queuedFrame.FrameID, queuedFrame.ParentFrameID); authorised {
			// Get all `m.space.child` state events for this frame
			events, err := childReferences(ctx, querier, walker.SuggestedOnly, queuedFrame.FrameID)
			if err != nil {
				xutil.GetLogger(ctx).WithError(err).WithField("frame_id", queuedFrame.FrameID).Error("failed to extract references for frame")
				continue
			}
			discoveredChildEvents = events

			pubFrame := publicFramesChunk(ctx, querier, queuedFrame.FrameID)

			discoveredFrames = append(discoveredFrames, fclient.FrameHierarchyFrame{
				PublicFrame:    *pubFrame,
				FrameType:      frameType,
				ChildrenState: events,
			})
			// don't walk children if the user is not joined/invited to the space
			if !isJoinedOrInvited {
				continue
			}
		} else {
			// frame exists but user is not authorised
			continue
		}

		// don't walk the children
		// if the parent is not a space frame
		if frameType != spec.MSpace {
			continue
		}

		// For each referenced frame ID in the child events being returned to the caller
		// add the frame ID to the queue of unvisited frames. Loop from the beginning.
		// We need to invert the order here because the child events are lo->hi on the timestamp,
		// so we need to ensure we pop in the same lo->hi order, which won't be the case if we
		// insert the highest timestamp last in a stack.
		for i := len(discoveredChildEvents) - 1; i >= 0; i-- {
			spaceContent := struct {
				Via []string `json:"via"`
			}{}
			ev := discoveredChildEvents[i]
			_ = json.Unmarshal(ev.Content, &spaceContent)

			childFrameID, err := spec.NewFrameID(ev.StateKey)

			if err != nil {
				xutil.GetLogger(ctx).WithError(err).WithField("invalid_frame_id", ev.StateKey).WithField("parent_frame_id", queuedFrame.FrameID).Warn("Invalid frame ID in m.space.child state event")
			} else {
				unvisited = append(unvisited, dataframe.FrameHierarchyWalkerQueuedFrame{
					FrameID:       *childFrameID,
					ParentFrameID: &queuedFrame.FrameID,
					Depth:        queuedFrame.Depth + 1,
					Vias:         spaceContent.Via,
				})
			}
		}
	}

	if len(unvisited) == 0 {
		// If no more frames to walk, then don't return a walker for future pages
		return discoveredFrames, nil, nil
	} else {
		// If there are more frames to walk, then return a new walker to resume walking from (for querying more pages)
		newWalker := dataframe.FrameHierarchyWalker{
			RootFrameID:    walker.RootFrameID,
			Caller:        walker.Caller,
			SuggestedOnly: walker.SuggestedOnly,
			MaxDepth:      walker.MaxDepth,
			Unvisited:     unvisited,
			Processed:     processed,
		}

		return discoveredFrames, &newWalker, nil
	}

}

// authorised returns true iff the user is joined this frame or the frame is world_readable
func authorised(ctx context.Context, querier *Queryer, caller types.DeviceOrServerName, frameID spec.FrameID, parentFrameID *spec.FrameID) (authed, isJoinedOrInvited bool) {
	if clientCaller := caller.Device(); clientCaller != nil {
		return authorisedUser(ctx, querier, clientCaller, frameID, parentFrameID)
	} else {
		return authorisedServer(ctx, querier, frameID, *caller.ServerName()), false
	}
}

// authorisedServer returns true iff the server is joined this frame or the frame is world_readable, public, or knockable
func authorisedServer(ctx context.Context, querier *Queryer, frameID spec.FrameID, callerServerName spec.ServerName) bool {
	// Check history visibility / join rules first
	hisVisTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameHistoryVisibility,
		StateKey:  "",
	}
	joinRuleTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameJoinRules,
		StateKey:  "",
	}
	var queryFrameRes dataframe.QueryCurrentStateResponse
	err := querier.QueryCurrentState(ctx, &dataframe.QueryCurrentStateRequest{
		FrameID: frameID.String(),
		StateTuples: []xtools.StateKeyTuple{
			hisVisTuple, joinRuleTuple,
		},
	}, &queryFrameRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to QueryCurrentState")
		return false
	}
	hisVisEv := queryFrameRes.StateEvents[hisVisTuple]
	if hisVisEv != nil {
		hisVis, _ := hisVisEv.HistoryVisibility()
		if hisVis == "world_readable" {
			return true
		}
	}

	// check if this frame is a restricted frame and if so, we need to check if the server is joined to an allowed frame ID
	// in addition to the actual frame ID (but always do the actual one first as it's quicker in the common case)
	allowJoinedToFrameIDs := []spec.FrameID{frameID}
	joinRuleEv := queryFrameRes.StateEvents[joinRuleTuple]

	if joinRuleEv != nil {
		rule, ruleErr := joinRuleEv.JoinRule()
		if ruleErr != nil {
			xutil.GetLogger(ctx).WithError(ruleErr).WithField("parent_frame_id", frameID).Warn("failed to get join rule")
			return false
		}

		if rule == spec.Public || rule == spec.Knock {
			return true
		}

		if rule == spec.Restricted {
			allowJoinedToFrameIDs = append(allowJoinedToFrameIDs, restrictedJoinRuleAllowedFrames(ctx, joinRuleEv)...)
		}
	}

	// check if server is joined to any allowed frame
	for _, allowedFrameID := range allowJoinedToFrameIDs {
		var queryRes fs.QueryJoinedHostServerNamesInFrameResponse
		err = querier.FSAPI.QueryJoinedHostServerNamesInFrame(ctx, &fs.QueryJoinedHostServerNamesInFrameRequest{
			FrameID: allowedFrameID.String(),
		}, &queryRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("failed to QueryJoinedHostServerNamesInFrame")
			continue
		}
		for _, srv := range queryRes.ServerNames {
			if srv == callerServerName {
				return true
			}
		}
	}

	return false
}

// authorisedUser returns true iff the user is invited/joined this frame or the frame is world_readable
// or if the frame has a public or knock join rule.
// Failing that, if the frame has a restricted join rule and belongs to the space parent listed, it will return true.
func authorisedUser(ctx context.Context, querier *Queryer, clientCaller *userapi.Device, frameID spec.FrameID, parentFrameID *spec.FrameID) (authed bool, isJoinedOrInvited bool) {
	hisVisTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameHistoryVisibility,
		StateKey:  "",
	}
	joinRuleTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameJoinRules,
		StateKey:  "",
	}
	frameMemberTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameMember,
		StateKey:  clientCaller.UserID,
	}
	var queryRes dataframe.QueryCurrentStateResponse
	err := querier.QueryCurrentState(ctx, &dataframe.QueryCurrentStateRequest{
		FrameID: frameID.String(),
		StateTuples: []xtools.StateKeyTuple{
			hisVisTuple, joinRuleTuple, frameMemberTuple,
		},
	}, &queryRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to QueryCurrentState")
		return false, false
	}
	memberEv := queryRes.StateEvents[frameMemberTuple]
	if memberEv != nil {
		membership, _ := memberEv.Membership()
		if membership == spec.Join || membership == spec.Invite {
			return true, true
		}
	}
	hisVisEv := queryRes.StateEvents[hisVisTuple]
	if hisVisEv != nil {
		hisVis, _ := hisVisEv.HistoryVisibility()
		if hisVis == "world_readable" {
			return true, false
		}
	}
	joinRuleEv := queryRes.StateEvents[joinRuleTuple]
	if parentFrameID != nil && joinRuleEv != nil {
		var allowed bool
		rule, ruleErr := joinRuleEv.JoinRule()
		if ruleErr != nil {
			xutil.GetLogger(ctx).WithError(ruleErr).WithField("parent_frame_id", parentFrameID).Warn("failed to get join rule")
		} else if rule == spec.Public || rule == spec.Knock {
			allowed = true
		} else if rule == spec.Restricted {
			allowedFrameIDs := restrictedJoinRuleAllowedFrames(ctx, joinRuleEv)
			// check parent is in the allowed set
			for _, a := range allowedFrameIDs {
				if *parentFrameID == a {
					allowed = true
					break
				}
			}
		}
		if allowed {
			// ensure caller is joined to the parent frame
			var queryRes2 dataframe.QueryCurrentStateResponse
			err = querier.QueryCurrentState(ctx, &dataframe.QueryCurrentStateRequest{
				FrameID: parentFrameID.String(),
				StateTuples: []xtools.StateKeyTuple{
					frameMemberTuple,
				},
			}, &queryRes2)
			if err != nil {
				xutil.GetLogger(ctx).WithError(err).WithField("parent_frame_id", parentFrameID).Warn("failed to check user is joined to parent frame")
			} else {
				memberEv = queryRes2.StateEvents[frameMemberTuple]
				if memberEv != nil {
					membership, _ := memberEv.Membership()
					if membership == spec.Join {
						return true, false
					}
				}
			}
		}
	}
	return false, false
}

// helper function to fetch a state event
func stateEvent(ctx context.Context, querier *Queryer, frameID spec.FrameID, evType, stateKey string) *types.HeaderedEvent {
	var queryRes dataframe.QueryCurrentStateResponse
	tuple := xtools.StateKeyTuple{
		EventType: evType,
		StateKey:  stateKey,
	}
	err := querier.QueryCurrentState(ctx, &dataframe.QueryCurrentStateRequest{
		FrameID:      frameID.String(),
		StateTuples: []xtools.StateKeyTuple{tuple},
	}, &queryRes)
	if err != nil {
		return nil
	}
	return queryRes.StateEvents[tuple]
}

// returns true if the current server is participating in the provided frame
func frameExists(ctx context.Context, querier *Queryer, frameID spec.FrameID) bool {
	var queryRes dataframe.QueryServerJoinedToFrameResponse
	err := querier.QueryServerJoinedToFrame(ctx, &dataframe.QueryServerJoinedToFrameRequest{
		FrameID:     frameID.String(),
		ServerName: querier.Cfg.Global.ServerName,
	}, &queryRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to QueryServerJoinedToFrame")
		return false
	}
	// if the frame exists but we aren't in the frame then we might have stale data so we want to fetch
	// it fresh via federation
	return queryRes.FrameExists && queryRes.IsInFrame
}

// federatedFrameInfo returns more of the spaces graph from another server. Returns nil if this was
// unsuccessful.
func federatedFrameInfo(ctx context.Context, querier *Queryer, caller types.DeviceOrServerName, suggestedOnly bool, frameID spec.FrameID, vias []string) *fclient.FrameHierarchyResponse {
	// only do federated requests for client requests
	if caller.Device() == nil {
		return nil
	}
	resp, ok := querier.Cache.GetFrameHierarchy(frameID.String())
	if ok {
		xutil.GetLogger(ctx).Debugf("Returning cached response for %s", frameID)
		return &resp
	}
	xutil.GetLogger(ctx).Debugf("Querying %s via %+v", frameID, vias)
	innerCtx := context.Background()
	// query more of the spaces graph using these servers
	for _, serverName := range vias {
		if serverName == string(querier.Cfg.Global.ServerName) {
			continue
		}
		res, err := querier.FSAPI.FrameHierarchies(innerCtx, querier.Cfg.Global.ServerName, spec.ServerName(serverName), frameID.String(), suggestedOnly)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Warnf("failed to call FrameHierarchies on server %s", serverName)
			continue
		}
		// ensure nil slices are empty as we send this to the client sometimes
		if res.Frame.ChildrenState == nil {
			res.Frame.ChildrenState = []fclient.FrameHierarchyStrippedEvent{}
		}
		for i := 0; i < len(res.Children); i++ {
			child := res.Children[i]
			if child.ChildrenState == nil {
				child.ChildrenState = []fclient.FrameHierarchyStrippedEvent{}
			}
			res.Children[i] = child
		}
		querier.Cache.StoreFrameHierarchy(frameID.String(), res)

		return &res
	}
	return nil
}

// references returns all child references pointing to or from this frame.
func childReferences(ctx context.Context, querier *Queryer, suggestedOnly bool, frameID spec.FrameID) ([]fclient.FrameHierarchyStrippedEvent, error) {
	createTuple := xtools.StateKeyTuple{
		EventType: spec.MFrameCreate,
		StateKey:  "",
	}
	var res dataframe.QueryCurrentStateResponse
	err := querier.QueryCurrentState(context.Background(), &dataframe.QueryCurrentStateRequest{
		FrameID:         frameID.String(),
		AllowWildcards: true,
		StateTuples: []xtools.StateKeyTuple{
			createTuple, {
				EventType: spec.MSpaceChild,
				StateKey:  "*",
			},
		},
	}, &res)
	if err != nil {
		return nil, err
	}

	// don't return any child refs if the frame is not a space frame
	if create := res.StateEvents[createTuple]; create != nil {
		var createContent xtools.CreateContent
		err := json.Unmarshal(create.Content(), &createContent)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).WithField("create_content", create.Content()).Warn("failed to unmarshal m.frame.create event")
		}
		frameType := createContent.FrameType
		if frameType != spec.MSpace {
			return []fclient.FrameHierarchyStrippedEvent{}, nil
		}
	}
	delete(res.StateEvents, createTuple)

	el := make([]fclient.FrameHierarchyStrippedEvent, 0, len(res.StateEvents))
	for _, ev := range res.StateEvents {
		content := gjson.ParseBytes(ev.Content())
		// only return events that have a `via` key as per MSC1772
		// else we'll incorrectly walk redacted events (as the link
		// is in the state_key)
		if content.Get("via").Exists() {
			strip := stripped(ev.PDU)
			if strip == nil {
				continue
			}
			// if suggested only and this child isn't suggested, skip it.
			// if suggested only = false we include everything so don't need to check the content.
			if suggestedOnly && !content.Get("suggested").Bool() {
				continue
			}
			el = append(el, *strip)
		}
	}
	// sort by origin_server_ts as per MSC2946
	sort.Slice(el, func(i, j int) bool {
		return el[i].OriginServerTS < el[j].OriginServerTS
	})

	return el, nil
}

// fetch public frame information for provided frame
func publicFramesChunk(ctx context.Context, querier *Queryer, frameID spec.FrameID) *fclient.PublicFrame {
	pubFrames, err := dataframe.PopulatePublicFrames(ctx, []string{frameID.String()}, querier)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to PopulatePublicFrames")
		return nil
	}
	if len(pubFrames) == 0 {
		return nil
	}
	return &pubFrames[0]
}

func stripped(ev xtools.PDU) *fclient.FrameHierarchyStrippedEvent {
	if ev.StateKey() == nil {
		return nil
	}
	return &fclient.FrameHierarchyStrippedEvent{
		Type:           ev.Type(),
		StateKey:       *ev.StateKey(),
		Content:        ev.Content(),
		Sender:         string(ev.SenderID()),
		OriginServerTS: ev.OriginServerTS(),
	}
}

// given join_rule event, return list of frames where membership of that frame allows joining.
func restrictedJoinRuleAllowedFrames(ctx context.Context, joinRuleEv *types.HeaderedEvent) (allows []spec.FrameID) {
	rule, _ := joinRuleEv.JoinRule()
	if rule != spec.Restricted {
		return nil
	}
	var jrContent xtools.JoinRuleContent
	if err := json.Unmarshal(joinRuleEv.Content(), &jrContent); err != nil {
		xutil.GetLogger(ctx).Warnf("failed to check join_rule on frame %s: %s", joinRuleEv.FrameID(), err)
		return nil
	}
	for _, allow := range jrContent.Allow {
		if allow.Type == spec.MFrameMembership {
			allowedFrameID, err := spec.NewFrameID(allow.FrameID)
			if err != nil {
				xutil.GetLogger(ctx).Warnf("invalid frame ID '%s' found in join_rule on frame %s: %s", allow.FrameID, joinRuleEv.FrameID(), err)
			} else {
				allows = append(allows, *allowedFrameID)
			}
		}
	}
	return
}
