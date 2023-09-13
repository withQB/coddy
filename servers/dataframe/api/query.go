// Copyright 2017 Vector Creations Ltd
// Copyright 2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/auth/authtypes"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/servers/dataframe/types"
)

// QueryLatestEventsAndStateRequest is a request to QueryLatestEventsAndState
type QueryLatestEventsAndStateRequest struct {
	// The frame ID to query the latest events for.
	FrameID string `json:"frame_id"`
	// The state key tuples to fetch from the frame current state.
	// If this list is empty or nil then *ALL* current state events are returned.
	StateToFetch []xtools.StateKeyTuple `json:"state_to_fetch"`
}

// QueryLatestEventsAndStateResponse is a response to QueryLatestEventsAndState
// This is used when sending events to set the prev_events, auth_events and depth.
// It is also used to tell whether the event is allowed by the event auth rules.
type QueryLatestEventsAndStateResponse struct {
	// Does the frame exist?
	// If the frame doesn't exist this will be false and LatestEvents will be empty.
	FrameExists bool `json:"frame_exists"`
	// The frame version of the frame.
	FrameVersion xtools.FrameVersion `json:"frame_version"`
	// The latest events in the frame.
	// These are used to set the prev_events when sending an event.
	LatestEvents []string `json:"latest_events"`
	// The state events requested.
	// This list will be in an arbitrary order.
	// These are used to set the auth_events when sending an event.
	// These are used to check whether the event is allowed.
	StateEvents []*types.HeaderedEvent `json:"state_events"`
	// The depth of the latest events.
	// This is one greater than the maximum depth of the latest events.
	// This is used to set the depth when sending an event.
	Depth int64 `json:"depth"`
}

// QueryStateAfterEventsRequest is a request to QueryStateAfterEvents
type QueryStateAfterEventsRequest struct {
	// The frame ID to query the state in.
	FrameID string `json:"frame_id"`
	// The list of previous events to return the events after.
	PrevEventIDs []string `json:"prev_event_ids"`
	// The state key tuples to fetch from the state. If none are specified then
	// the entire resolved frame state will be returned.
	StateToFetch []xtools.StateKeyTuple `json:"state_to_fetch"`
}

// QueryStateAfterEventsResponse is a response to QueryStateAfterEvents
type QueryStateAfterEventsResponse struct {
	// Does the frame exist on this dataframe?
	// If the frame doesn't exist this will be false and StateEvents will be empty.
	FrameExists bool `json:"frame_exists"`
	// The frame version of the frame.
	FrameVersion xtools.FrameVersion `json:"frame_version"`
	// Do all the previous events exist on this dataframe?
	// If some of previous events do not exist this will be false and StateEvents will be empty.
	PrevEventsExist bool `json:"prev_events_exist"`
	// The state events requested.
	// This list will be in an arbitrary order.
	StateEvents []*types.HeaderedEvent `json:"state_events"`
}

// QueryEventsByIDRequest is a request to QueryEventsByID
type QueryEventsByIDRequest struct {
	// The frameID to query events for. If this is empty, we first try to fetch the frameID from the database
	// as this is needed for further processing/parsing events.
	FrameID string `json:"frame_id"`
	// The event IDs to look up.
	EventIDs []string `json:"event_ids"`
}

// QueryEventsByIDResponse is a response to QueryEventsByID
type QueryEventsByIDResponse struct {
	// A list of events with the requested IDs.
	// If the dataframe does not have a copy of a requested event
	// then it will omit that event from the list.
	// If the dataframe thinks it has a copy of the event, but
	// fails to read it from the database then it will fail
	// the entire request.
	// This list will be in an arbitrary order.
	Events []*types.HeaderedEvent `json:"events"`
}

// QueryMembershipForUserRequest is a request to QueryMembership
type QueryMembershipForUserRequest struct {
	// ID of the frame to fetch membership from
	FrameID string
	// ID of the user for whom membership is requested
	UserID spec.UserID
}

// QueryMembershipForUserResponse is a response to QueryMembership
type QueryMembershipForUserResponse struct {
	// The EventID of the latest "m.frame.member" event for the sender,
	// if HasBeenInFrame is true.
	EventID string `json:"event_id"`
	// True if the user has been in frame before and has either stayed in it or left it.
	HasBeenInFrame bool `json:"has_been_in_frame"`
	// True if the user is in frame.
	IsInFrame bool `json:"is_in_frame"`
	// The current membership
	Membership string `json:"membership"`
	// True if the user asked to forget this frame.
	IsFrameForgotten bool `json:"is_frame_forgotten"`
	FrameExists      bool `json:"frame_exists"`
	// The sender ID of the user in the frame, if it exists
	SenderID *spec.SenderID
}

// QueryMembershipsForFrameRequest is a request to QueryMembershipsForFrame
type QueryMembershipsForFrameRequest struct {
	// If true, only returns the membership events of "join" membership
	JoinedOnly bool `json:"joined_only"`
	// If true, only returns the membership events of local users
	LocalOnly bool `json:"local_only"`
	// ID of the frame to fetch memberships from
	FrameID string `json:"frame_id"`
	// Optional - ID of the user sending the request, for checking if the
	// user is allowed to see the memberships. If not specified then all
	// frame memberships will be returned.
	SenderID spec.SenderID `json:"sender"`
}

// QueryMembershipsForFrameResponse is a response to QueryMembershipsForFrame
type QueryMembershipsForFrameResponse struct {
	// The "m.frame.member" events (of "join" membership) in the client format
	JoinEvents []synctypes.ClientEvent `json:"join_events"`
	// True if the user has been in frame before and has either stayed in it or
	// left it.
	HasBeenInFrame bool `json:"has_been_in_frame"`
	// True if the user asked to forget this frame.
	IsFrameForgotten bool `json:"is_frame_forgotten"`
}

// QueryServerJoinedToFrameRequest is a request to QueryServerJoinedToFrame
type QueryServerJoinedToFrameRequest struct {
	// Server name of the server to find. If not specified, we will
	// default to checking if the local server is joined.
	ServerName spec.ServerName `json:"server_name"`
	// ID of the frame to see if we are still joined to
	FrameID string `json:"frame_id"`
}

// QueryMembershipsForFrameResponse is a response to QueryServerJoinedToFrame
type QueryServerJoinedToFrameResponse struct {
	// True if the frame exists on the server
	FrameExists bool `json:"frame_exists"`
	// True if we still believe that the server is participating in the frame
	IsInFrame bool `json:"is_in_frame"`
	// The frameversion if joined to frame
	FrameVersion xtools.FrameVersion
}

// QueryServerAllowedToSeeEventRequest is a request to QueryServerAllowedToSeeEvent
type QueryServerAllowedToSeeEventRequest struct {
	// The event ID to look up invites in.
	EventID string `json:"event_id"`
	// The server interested in the event
	ServerName spec.ServerName `json:"server_name"`
}

// QueryServerAllowedToSeeEventResponse is a response to QueryServerAllowedToSeeEvent
type QueryServerAllowedToSeeEventResponse struct {
	// Wether the server in question is allowed to see the event
	AllowedToSeeEvent bool `json:"can_see_event"`
}

// QueryMissingEventsRequest is a request to QueryMissingEvents
type QueryMissingEventsRequest struct {
	// Events which are known previous to the gap in the timeline.
	EarliestEvents []string `json:"earliest_events"`
	// Latest known events.
	LatestEvents []string `json:"latest_events"`
	// Limit the number of events this query returns.
	Limit int `json:"limit"`
	// The server interested in the event
	ServerName spec.ServerName `json:"server_name"`
}

// QueryMissingEventsResponse is a response to QueryMissingEvents
type QueryMissingEventsResponse struct {
	// Missing events, arbritrary order.
	Events []*types.HeaderedEvent `json:"events"`
}

// QueryStateAndAuthChainRequest is a request to QueryStateAndAuthChain
type QueryStateAndAuthChainRequest struct {
	// The frame ID to query the state in.
	FrameID string `json:"frame_id"`
	// The list of prev events for the event. Used to calculate the state at
	// the event.
	PrevEventIDs []string `json:"prev_event_ids"`
	// The list of auth events for the event. Used to calculate the auth chain
	AuthEventIDs []string `json:"auth_event_ids"`
	// If true, the auth chain events for the auth event IDs given will be fetched only. Prev event IDs are ignored.
	// If false, state and auth chain events for the prev event IDs and entire current state will be included.
	// TDO: not a great API shape. It serves 2 main uses: false=>response for send_join, true=>response for /event_auth
	OnlyFetchAuthChain bool `json:"only_fetch_auth_chain"`
	// Should state resolution be ran on the result events?
	// TDO: check call sites and remove if we always want to do state res
	ResolveState bool `json:"resolve_state"`
}

// QueryStateAndAuthChainResponse is a response to QueryStateAndAuthChain
type QueryStateAndAuthChainResponse struct {
	// Does the frame exist on this dataframe?
	// If the frame doesn't exist this will be false and StateEvents will be empty.
	FrameExists bool `json:"frame_exists"`
	// The frame version of the frame.
	FrameVersion xtools.FrameVersion `json:"frame_version"`
	// Do all the previous events exist on this dataframe?
	// If some of previous events do not exist this will be false and StateEvents will be empty.
	PrevEventsExist bool `json:"prev_events_exist"`
	StateKnown      bool `json:"state_known"`
	// The state and auth chain events that were requested.
	// The lists will be in an arbitrary order.
	StateEvents     []*types.HeaderedEvent `json:"state_events"`
	AuthChainEvents []*types.HeaderedEvent `json:"auth_chain_events"`
	// True if the queried event was rejected earlier.
	IsRejected bool `json:"is_rejected"`
}

// QueryFrameVersionForFrameRequest asks for the frame version for a given frame.
type QueryFrameVersionForFrameRequest struct {
	FrameID string `json:"frame_id"`
}

// QueryFrameVersionForFrameResponse is a response to QueryFrameVersionForFrameRequest
type QueryFrameVersionForFrameResponse struct {
	FrameVersion xtools.FrameVersion `json:"frame_version"`
}

type QueryPublishedFramesRequest struct {
	// Optional. If specified, returns whether this frame is published or not.
	FrameID             string
	NetworkID          string
	IncludeAllNetworks bool
}

type QueryPublishedFramesResponse struct {
	// The list of published frames.
	FrameIDs []string
}

type QueryAuthChainRequest struct {
	EventIDs []string
}

type QueryAuthChainResponse struct {
	AuthChain []*types.HeaderedEvent
}

type QuerySharedUsersRequest struct {
	UserID         string
	OtherUserIDs   []string
	ExcludeFrameIDs []string
	IncludeFrameIDs []string
	LocalOnly      bool
}

type QuerySharedUsersResponse struct {
	UserIDsToCount map[string]int
}

type QueryBulkStateContentRequest struct {
	// Returns state events in these frames
	FrameIDs []string
	// If true, treats the '*' StateKey as "all state events of this type" rather than a literal value of '*'
	AllowWildcards bool
	// The state events to return. Only a small subset of tuples are allowed in this request as only certain events
	// have their content fields extracted. Specifically, the tuple Type must be one of:
	//   m.frame.avatar
	//   m.frame.create
	//   m.frame.canonical_alias
	//   m.frame.guest_access
	//   m.frame.history_visibility
	//   m.frame.join_rules
	//   m.frame.member
	//   m.frame.name
	//   m.frame.topic
	// Any other tuple type will result in the query failing.
	StateTuples []xtools.StateKeyTuple
}
type QueryBulkStateContentResponse struct {
	// map of frame ID -> tuple -> content_value
	Frames map[string]map[xtools.StateKeyTuple]string
}

type QueryCurrentStateRequest struct {
	FrameID         string
	AllowWildcards bool
	// State key tuples. If a state_key has '*' and AllowWidlcards is true, returns all matching
	// state events with that event type.
	StateTuples []xtools.StateKeyTuple
}

type QueryCurrentStateResponse struct {
	StateEvents map[xtools.StateKeyTuple]*types.HeaderedEvent
}

type QueryKnownUsersRequest struct {
	UserID       string `json:"user_id"`
	SearchString string `json:"search_string"`
	Limit        int    `json:"limit"`
}

type QueryKnownUsersResponse struct {
	Users []authtypes.FullyQualifiedProfile `json:"profiles"`
}

type QueryServerBannedFromFrameRequest struct {
	ServerName spec.ServerName `json:"server_name"`
	FrameID     string          `json:"frame_id"`
}

type QueryServerBannedFromFrameResponse struct {
	Banned bool `json:"banned"`
}

// MarshalJSON stringifies the frame ID and StateKeyTuple keys so they can be sent over the wire in HTTP API mode.
func (r *QueryBulkStateContentResponse) MarshalJSON() ([]byte, error) {
	se := make(map[string]string)
	for frameID, tupleToEvent := range r.Frames {
		for tuple, event := range tupleToEvent {
			// use 0x1F (unit separator) as the delimiter between frame ID/type/state key,
			se[fmt.Sprintf("%s\x1F%s\x1F%s", frameID, tuple.EventType, tuple.StateKey)] = event
		}
	}
	return json.Marshal(se)
}

func (r *QueryBulkStateContentResponse) UnmarshalJSON(data []byte) error {
	wireFormat := make(map[string]string)
	err := json.Unmarshal(data, &wireFormat)
	if err != nil {
		return err
	}
	r.Frames = make(map[string]map[xtools.StateKeyTuple]string)
	for frameTuple, value := range wireFormat {
		fields := strings.Split(frameTuple, "\x1F")
		frameID := fields[0]
		if r.Frames[frameID] == nil {
			r.Frames[frameID] = make(map[xtools.StateKeyTuple]string)
		}
		r.Frames[frameID][xtools.StateKeyTuple{
			EventType: fields[1],
			StateKey:  fields[2],
		}] = value
	}
	return nil
}

// MarshalJSON stringifies the StateKeyTuple keys so they can be sent over the wire in HTTP API mode.
func (r *QueryCurrentStateResponse) MarshalJSON() ([]byte, error) {
	se := make(map[string]*types.HeaderedEvent, len(r.StateEvents))
	for k, v := range r.StateEvents {
		// use 0x1F (unit separator) as the delimiter between type/state key,
		se[fmt.Sprintf("%s\x1F%s", k.EventType, k.StateKey)] = v
	}
	return json.Marshal(se)
}

func (r *QueryCurrentStateResponse) UnmarshalJSON(data []byte) error {
	res := make(map[string]*types.HeaderedEvent)
	err := json.Unmarshal(data, &res)
	if err != nil {
		return err
	}
	r.StateEvents = make(map[xtools.StateKeyTuple]*types.HeaderedEvent, len(res))
	for k, v := range res {
		fields := strings.Split(k, "\x1F")
		r.StateEvents[xtools.StateKeyTuple{
			EventType: fields[0],
			StateKey:  fields[1],
		}] = v
	}
	return nil
}

// QueryLeftUsersRequest is a request to calculate users that we (the server) don't share a
// a frame with anymore. This is used to cleanup stale device list entries, where we would
// otherwise keep on trying to get device lists.
type QueryLeftUsersRequest struct {
	StaleDeviceListUsers []string `json:"user_ids"`
}

// QueryLeftUsersResponse is the response to QueryLeftUsersRequest.
type QueryLeftUsersResponse struct {
	LeftUsers []string `json:"user_ids"`
}

type JoinFrameQuerier struct {
	Dataframe RestrictedJoinAPI
}

func (rq *JoinFrameQuerier) CurrentStateEvent(ctx context.Context, frameID spec.FrameID, eventType string, stateKey string) (xtools.PDU, error) {
	return rq.Dataframe.CurrentStateEvent(ctx, frameID, eventType, stateKey)
}

func (rq *JoinFrameQuerier) InvitePending(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (bool, error) {
	return rq.Dataframe.InvitePending(ctx, frameID, senderID)
}

func (rq *JoinFrameQuerier) RestrictedFrameJoinInfo(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID, localServerName spec.ServerName) (*xtools.RestrictedFrameJoinInfo, error) {
	frameInfo, err := rq.Dataframe.QueryFrameInfo(ctx, frameID)
	if err != nil || frameInfo == nil || frameInfo.IsStub() {
		return nil, err
	}

	req := QueryServerJoinedToFrameRequest{
		ServerName: localServerName,
		FrameID:     frameID.String(),
	}
	res := QueryServerJoinedToFrameResponse{}
	if err = rq.Dataframe.QueryServerJoinedToFrame(ctx, &req, &res); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.QueryServerJoinedToFrame failed")
		return nil, fmt.Errorf("InternalServerError: Failed to query frame: %w", err)
	}

	userJoinedToFrame, err := rq.Dataframe.UserJoinedToFrame(ctx, types.FrameNID(frameInfo.FrameNID), senderID)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.UserJoinedToFrame failed")
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	locallyJoinedUsers, err := rq.Dataframe.LocallyJoinedUsers(ctx, frameInfo.FrameVersion, types.FrameNID(frameInfo.FrameNID))
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsAPI.GetLocallyJoinedUsers failed")
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	return &xtools.RestrictedFrameJoinInfo{
		LocalServerInFrame: res.FrameExists && res.IsInFrame,
		UserJoinedToFrame:  userJoinedToFrame,
		JoinedUsers:       locallyJoinedUsers,
	}, nil
}

type MembershipQuerier struct {
	Dataframe FederationDataframeAPI
}

func (mq *MembershipQuerier) CurrentMembership(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (string, error) {
	res := QueryMembershipForUserResponse{}
	err := mq.Dataframe.QueryMembershipForSenderID(ctx, frameID, senderID, &res)

	membership := ""
	if err == nil {
		membership = res.Membership
	}
	return membership, err
}

type QueryFrameHierarchyRequest struct {
	SuggestedOnly bool `json:"suggested_only"`
	Limit         int  `json:"limit"`
	MaxDepth      int  `json:"max_depth"`
	From          int  `json:"json"`
}

// A struct storing the intermediate state of a frame hierarchy query for pagination purposes.
//
// Used for implementing space summaries / frame hierarchies
//
// Use NewFrameHierarchyWalker to construct this, and QueryNextFrameHierarchyPage on the dataframe API
// to traverse the frame hierarchy.
type FrameHierarchyWalker struct {
	RootFrameID    spec.FrameID
	Caller        types.DeviceOrServerName
	SuggestedOnly bool
	MaxDepth      int
	Processed     FrameSet
	Unvisited     []FrameHierarchyWalkerQueuedFrame
}

type FrameHierarchyWalkerQueuedFrame struct {
	FrameID       spec.FrameID
	ParentFrameID *spec.FrameID
	Depth        int
	Vias         []string // vias to query this frame by
}

// Create a new frame hierarchy walker, starting from the provided root frame ID.
//
// Use the resulting struct with QueryNextFrameHierarchyPage on the dataframe API to traverse the frame hierarchy.
func NewFrameHierarchyWalker(caller types.DeviceOrServerName, frameID spec.FrameID, suggestedOnly bool, maxDepth int) FrameHierarchyWalker {
	walker := FrameHierarchyWalker{
		RootFrameID:    frameID,
		Caller:        caller,
		SuggestedOnly: suggestedOnly,
		MaxDepth:      maxDepth,
		Unvisited: []FrameHierarchyWalkerQueuedFrame{{
			FrameID:       frameID,
			ParentFrameID: nil,
			Depth:        0,
		}},
		Processed: NewFrameSet(),
	}

	return walker
}

// A set of frame IDs.
type FrameSet map[spec.FrameID]struct{}

// Create a new empty frame set.
func NewFrameSet() FrameSet {
	return FrameSet{}
}

// Check if a frame ID is in a frame set.
func (s FrameSet) Contains(val spec.FrameID) bool {
	_, ok := s[val]
	return ok
}

// Add a frame ID to a frame set.
func (s FrameSet) Add(val spec.FrameID) {
	s[val] = struct{}{}
}

func (s FrameSet) Copy() FrameSet {
	copied := make(FrameSet, len(s))
	for k := range s {
		copied.Add(k)
	}
	return copied
}
