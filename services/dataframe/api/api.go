package api

import (
	"context"
	"crypto/ed25519"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	asAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/dataframe/types"
	fsAPI "github.com/withqb/coddy/services/federationapi/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
)

// ErrInvalidID is an error returned if the userID is invalid
type ErrInvalidID struct {
	Err error
}

func (e ErrInvalidID) Error() string {
	return e.Err.Error()
}

// ErrNotAllowed is an error returned if the user is not allowed
// to execute some action (e.g. invite)
type ErrNotAllowed struct {
	Err error
}

func (e ErrNotAllowed) Error() string {
	return e.Err.Error()
}

// ErrFrameUnknownOrNotAllowed is an error return if either the provided
// frame ID does not exist, or points to a frame that the requester does
// not have access to.
type ErrFrameUnknownOrNotAllowed struct {
	Err error
}

func (e ErrFrameUnknownOrNotAllowed) Error() string {
	return e.Err.Error()
}

type RestrictedJoinAPI interface {
	CurrentStateEvent(ctx context.Context, frameID spec.FrameID, eventType string, stateKey string) (xtools.PDU, error)
	InvitePending(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (bool, error)
	RestrictedFrameJoinInfo(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID, localServerName spec.ServerName) (*xtools.RestrictedFrameJoinInfo, error)
	QueryFrameInfo(ctx context.Context, frameID spec.FrameID) (*types.FrameInfo, error)
	QueryServerJoinedToFrame(ctx context.Context, req *QueryServerJoinedToFrameRequest, res *QueryServerJoinedToFrameResponse) error
	UserJoinedToFrame(ctx context.Context, frameID types.FrameNID, senderID spec.SenderID) (bool, error)
	LocallyJoinedUsers(ctx context.Context, frameVersion xtools.FrameVersion, frameNID types.FrameNID) ([]xtools.PDU, error)
}

type DefaultFrameVersionAPI interface {
	// Returns the default frame version used.
	DefaultFrameVersion() xtools.FrameVersion
}

// DataframeInputAPI is used to write events to the frame server.
type DataframeInternalAPI interface {
	SyncDataframeAPI
	AppserviceDataframeAPI
	ClientDataframeAPI
	UserDataframeAPI
	FederationDataframeAPI
	QuerySenderIDAPI
	UserFramePrivateKeyCreator
	DefaultFrameVersionAPI

	// needed to avoid chicken and egg scenario when setting up the
	// interdependencies between the dataframe and other input APIs
	SetFederationAPI(fsAPI fsAPI.DataframeFederationAPI, keyRing *xtools.KeyRing)
	SetAppserviceAPI(asAPI asAPI.AppServiceInternalAPI)
	SetUserAPI(userAPI userapi.DataframeUserAPI)

	// QueryAuthChain returns the entire auth chain for the event IDs given.
	// The response includes the events in the request.
	// Omits without error for any missing auth events. There will be no duplicates.
	// Used in MSC2836.
	QueryAuthChain(
		ctx context.Context,
		req *QueryAuthChainRequest,
		res *QueryAuthChainResponse,
	) error
}

type UserFramePrivateKeyCreator interface {
	// GetOrCreateUserFramePrivateKey gets the user frame key for the specified user. If no key exists yet, a new one is created.
	GetOrCreateUserFramePrivateKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (ed25519.PrivateKey, error)
	StoreUserFramePublicKey(ctx context.Context, senderID spec.SenderID, userID spec.UserID, frameID spec.FrameID) error
}

type InputFrameEventsAPI interface {
	InputFrameEvents(
		ctx context.Context,
		req *InputFrameEventsRequest,
		res *InputFrameEventsResponse,
	)
}

type QuerySenderIDAPI interface {
	QuerySenderIDForUser(ctx context.Context, frameID spec.FrameID, userID spec.UserID) (*spec.SenderID, error)
	QueryUserIDForSender(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error)
}

// Query the latest events and state for a frame from the frame server.
type QueryLatestEventsAndStateAPI interface {
	QueryLatestEventsAndState(ctx context.Context, req *QueryLatestEventsAndStateRequest, res *QueryLatestEventsAndStateResponse) error
}

// QueryBulkStateContent does a bulk query for state event content in the given frames.
type QueryBulkStateContentAPI interface {
	QueryBulkStateContent(ctx context.Context, req *QueryBulkStateContentRequest, res *QueryBulkStateContentResponse) error
}

type QueryEventsAPI interface {
	// QueryEventsByID queries a list of events by event ID for one frame. If no frame is specified, it will try to determine
	// which frame to use by querying the first events frameID.
	QueryEventsByID(
		ctx context.Context,
		req *QueryEventsByIDRequest,
		res *QueryEventsByIDResponse,
	) error
	// QueryCurrentState retrieves the requested state events. If state events are not found, they will be missing from
	// the response.
	QueryCurrentState(ctx context.Context, req *QueryCurrentStateRequest, res *QueryCurrentStateResponse) error
}

type QueryFrameHierarchyAPI interface {
	// Traverse the frame hierarchy using the provided walker up to the provided limit,
	// returning a new walker which can be used to fetch the next page.
	//
	// If limit is -1, this is treated as no limit, and the entire hierarchy will be traversed.
	//
	// If returned walker is nil, then there are no more frames left to traverse. This method does not modify the provided walker, so it
	// can be cached.
	QueryNextFrameHierarchyPage(ctx context.Context, walker FrameHierarchyWalker, limit int) ([]fclient.FrameHierarchyFrame, *FrameHierarchyWalker, error)
}

type QueryMembershipAPI interface {
	QueryMembershipForSenderID(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID, res *QueryMembershipForUserResponse) error
	QueryMembershipForUser(ctx context.Context, req *QueryMembershipForUserRequest, res *QueryMembershipForUserResponse) error
	QueryMembershipsForFrame(ctx context.Context, req *QueryMembershipsForFrameRequest, res *QueryMembershipsForFrameResponse) error
	QueryFrameVersionForFrame(ctx context.Context, frameID string) (xtools.FrameVersion, error)

	// QueryMembershipAtEvent queries the memberships at the given events.
	// Returns a map from eventID to *types.HeaderedEvent of membership events.
	QueryMembershipAtEvent(
		ctx context.Context,
		frameID spec.FrameID,
		eventIDs []string,
		senderID spec.SenderID,
	) (map[string]*types.HeaderedEvent, error)
}

// API functions required by the syncapi
type SyncDataframeAPI interface {
	QueryLatestEventsAndStateAPI
	QueryBulkStateContentAPI
	QuerySenderIDAPI
	QueryMembershipAPI
	// QuerySharedUsers returns a list of users who share at least 1 frame in common with the given user.
	QuerySharedUsers(ctx context.Context, req *QuerySharedUsersRequest, res *QuerySharedUsersResponse) error
	// QueryEventsByID queries a list of events by event ID for one frame. If no frame is specified, it will try to determine
	// which frame to use by querying the first events frameID.
	QueryEventsByID(
		ctx context.Context,
		req *QueryEventsByIDRequest,
		res *QueryEventsByIDResponse,
	) error

	// Query the state after a list of events in a frame from the frame server.
	QueryStateAfterEvents(
		ctx context.Context,
		req *QueryStateAfterEventsRequest,
		res *QueryStateAfterEventsResponse,
	) error

	// Query a given amount (or less) of events prior to a given set of events.
	PerformBackfill(
		ctx context.Context,
		req *PerformBackfillRequest,
		res *PerformBackfillResponse,
	) error
}

type AppserviceDataframeAPI interface {
	QuerySenderIDAPI
	// QueryEventsByID queries a list of events by event ID for one frame. If no frame is specified, it will try to determine
	// which frame to use by querying the first events frameID.
	QueryEventsByID(
		ctx context.Context,
		req *QueryEventsByIDRequest,
		res *QueryEventsByIDResponse,
	) error
	// Query a list of membership events for a frame
	QueryMembershipsForFrame(
		ctx context.Context,
		req *QueryMembershipsForFrameRequest,
		res *QueryMembershipsForFrameResponse,
	) error
	// Get all known aliases for a frame ID
	GetAliasesForFrameID(
		ctx context.Context,
		req *GetAliasesForFrameIDRequest,
		res *GetAliasesForFrameIDResponse,
	) error
}

type ClientDataframeAPI interface {
	InputFrameEventsAPI
	QueryLatestEventsAndStateAPI
	QueryBulkStateContentAPI
	QueryEventsAPI
	QuerySenderIDAPI
	UserFramePrivateKeyCreator
	QueryFrameHierarchyAPI
	DefaultFrameVersionAPI
	QueryMembershipForUser(ctx context.Context, req *QueryMembershipForUserRequest, res *QueryMembershipForUserResponse) error
	QueryMembershipsForFrame(ctx context.Context, req *QueryMembershipsForFrameRequest, res *QueryMembershipsForFrameResponse) error
	QueryFramesForUser(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.FrameID, error)
	QueryStateAfterEvents(ctx context.Context, req *QueryStateAfterEventsRequest, res *QueryStateAfterEventsResponse) error
	// QueryKnownUsers returns a list of users that we know about from our joined frames.
	QueryKnownUsers(ctx context.Context, req *QueryKnownUsersRequest, res *QueryKnownUsersResponse) error
	QueryFrameVersionForFrame(ctx context.Context, frameID string) (xtools.FrameVersion, error)
	QueryPublishedFrames(ctx context.Context, req *QueryPublishedFramesRequest, res *QueryPublishedFramesResponse) error

	GetFrameIDForAlias(ctx context.Context, req *GetFrameIDForAliasRequest, res *GetFrameIDForAliasResponse) error
	GetAliasesForFrameID(ctx context.Context, req *GetAliasesForFrameIDRequest, res *GetAliasesForFrameIDResponse) error

	PerformCreateFrame(ctx context.Context, userID spec.UserID, frameID spec.FrameID, createRequest *PerformCreateFrameRequest) (string, *xutil.JSONResponse)
	// PerformFrameUpgrade upgrades a frame to a newer version
	PerformFrameUpgrade(ctx context.Context, frameID string, userID spec.UserID, frameVersion xtools.FrameVersion) (newFrameID string, err error)
	PerformAdminEvacuateFrame(ctx context.Context, frameID string) (affected []string, err error)
	PerformAdminEvacuateUser(ctx context.Context, userID string) (affected []string, err error)
	PerformAdminPurgeFrame(ctx context.Context, frameID string) error
	PerformAdminDownloadState(ctx context.Context, frameID, userID string, serverName spec.ServerName) error
	PerformPeek(ctx context.Context, req *PerformPeekRequest) (frameID string, err error)
	PerformUnpeek(ctx context.Context, frameID, userID, deviceID string) error
	PerformInvite(ctx context.Context, req *PerformInviteRequest) error
	PerformJoin(ctx context.Context, req *PerformJoinRequest) (frameID string, joinedVia spec.ServerName, err error)
	PerformLeave(ctx context.Context, req *PerformLeaveRequest, res *PerformLeaveResponse) error
	PerformPublish(ctx context.Context, req *PerformPublishRequest) error
	// PerformForget forgets a frames history for a specific user
	PerformForget(ctx context.Context, req *PerformForgetRequest, resp *PerformForgetResponse) error

	// Sets a frame alias, as provided sender, pointing to the provided frame ID.
	//
	// If err is nil, then the returned boolean indicates if the alias is already in use.
	// If true, then the alias has not been set to the provided frame, as it already in use.
	SetFrameAlias(ctx context.Context, senderID spec.SenderID, frameID spec.FrameID, alias string) (aliasAlreadyExists bool, err error)

	//RemoveFrameAlias(ctx context.Context, req *RemoveFrameAliasRequest, res *RemoveFrameAliasResponse) error
	// Removes a frame alias, as provided sender.
	//
	// Returns whether the alias was found, whether it was removed, and an error (if any occurred)
	RemoveFrameAlias(ctx context.Context, senderID spec.SenderID, alias string) (aliasFound bool, aliasRemoved bool, err error)

	SigningIdentityFor(ctx context.Context, frameID spec.FrameID, senderID spec.UserID) (fclient.SigningIdentity, error)
}

type UserDataframeAPI interface {
	QuerySenderIDAPI
	QueryLatestEventsAndStateAPI
	KeyserverDataframeAPI
	QueryCurrentState(ctx context.Context, req *QueryCurrentStateRequest, res *QueryCurrentStateResponse) error
	QueryMembershipsForFrame(ctx context.Context, req *QueryMembershipsForFrameRequest, res *QueryMembershipsForFrameResponse) error
	PerformAdminEvacuateUser(ctx context.Context, userID string) (affected []string, err error)
	PerformJoin(ctx context.Context, req *PerformJoinRequest) (frameID string, joinedVia spec.ServerName, err error)
	JoinedUserCount(ctx context.Context, frameID string) (int, error)
}

type FederationDataframeAPI interface {
	RestrictedJoinAPI
	InputFrameEventsAPI
	QueryLatestEventsAndStateAPI
	QueryBulkStateContentAPI
	QuerySenderIDAPI
	QueryFrameHierarchyAPI
	QueryMembershipAPI
	UserFramePrivateKeyCreator
	AssignFrameNID(ctx context.Context, frameID spec.FrameID, frameVersion xtools.FrameVersion) (frameNID types.FrameNID, err error)
	SigningIdentityFor(ctx context.Context, frameID spec.FrameID, senderID spec.UserID) (fclient.SigningIdentity, error)
	// QueryServerBannedFromFrame returns whether a server is banned from a frame by server ACLs.
	QueryServerBannedFromFrame(ctx context.Context, req *QueryServerBannedFromFrameRequest, res *QueryServerBannedFromFrameResponse) error
	GetFrameIDForAlias(ctx context.Context, req *GetFrameIDForAliasRequest, res *GetFrameIDForAliasResponse) error
	// QueryEventsByID queries a list of events by event ID for one frame. If no frame is specified, it will try to determine
	// which frame to use by querying the first events frameID.
	QueryEventsByID(ctx context.Context, req *QueryEventsByIDRequest, res *QueryEventsByIDResponse) error
	// Query to get state and auth chain for a (potentially hypothetical) event.
	// Takes lists of PrevEventIDs and AuthEventsIDs and uses them to calculate
	// the state and auth chain to return.
	QueryStateAndAuthChain(ctx context.Context, req *QueryStateAndAuthChainRequest, res *QueryStateAndAuthChainResponse) error
	QueryPublishedFrames(ctx context.Context, req *QueryPublishedFramesRequest, res *QueryPublishedFramesResponse) error
	// Query missing events for a frame from dataframe
	QueryMissingEvents(ctx context.Context, req *QueryMissingEventsRequest, res *QueryMissingEventsResponse) error
	// Query whether a server is allowed to see an event
	QueryServerAllowedToSeeEvent(ctx context.Context, serverName spec.ServerName, eventID string, frameID string) (allowed bool, err error)
	QueryFramesForUser(ctx context.Context, userID spec.UserID, desiredMembership string) ([]spec.FrameID, error)
	QueryRestrictedJoinAllowed(ctx context.Context, frameID spec.FrameID, senderID spec.SenderID) (string, error)
	PerformInboundPeek(ctx context.Context, req *PerformInboundPeekRequest, res *PerformInboundPeekResponse) error
	HandleInvite(ctx context.Context, event *types.HeaderedEvent) error

	PerformInvite(ctx context.Context, req *PerformInviteRequest) error
	// Query a given amount (or less) of events prior to a given set of events.
	PerformBackfill(ctx context.Context, req *PerformBackfillRequest, res *PerformBackfillResponse) error

	IsKnownFrame(ctx context.Context, frameID spec.FrameID) (bool, error)
	StateQuerier() xtools.StateQuerier
}

type KeyserverDataframeAPI interface {
	QueryLeftUsers(ctx context.Context, req *QueryLeftUsersRequest, res *QueryLeftUsersResponse) error
}
