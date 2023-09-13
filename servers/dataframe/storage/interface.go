package storage

import (
	"context"
	"crypto/ed25519"

	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/servers/dataframe/state"
	"github.com/withqb/coddy/servers/dataframe/storage/shared"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"
	"github.com/withqb/coddy/servers/dataframe/types"
)

type Database interface {
	UserFrameKeys
	// Do we support processing input events for more than one frame at a time?
	SupportsConcurrentFrameInputs() bool
	AssignFrameNID(ctx context.Context, frameID spec.FrameID, frameVersion xtools.FrameVersion) (frameNID types.FrameNID, err error)
	// FrameInfo returns frame information for the given frame ID, or nil if there is no frame.
	FrameInfo(ctx context.Context, frameID string) (*types.FrameInfo, error)
	FrameInfoByNID(ctx context.Context, frameNID types.FrameNID) (*types.FrameInfo, error)
	// Store the frame state at an event in the database
	AddState(
		ctx context.Context,
		frameNID types.FrameNID,
		stateBlockNIDs []types.StateBlockNID,
		state []types.StateEntry,
	) (types.StateSnapshotNID, error)

	MissingAuthPrevEvents(
		ctx context.Context, e xtools.PDU,
	) (missingAuth, missingPrev []string, err error)

	// Look up the state of a frame at each event for a list of string event IDs.
	// Returns an error if there is an error talking to the database.
	// The length of []types.StateAtEvent is guaranteed to equal the length of eventIDs if no error is returned.
	// Returns a types.MissingEventError if the frame state for the event IDs aren't in the database
	StateAtEventIDs(ctx context.Context, eventIDs []string) ([]types.StateAtEvent, error)
	// Look up the numeric IDs for a list of string event types.
	// Returns a map from string event type to numeric ID for the event type.
	EventTypeNIDs(ctx context.Context, eventTypes []string) (map[string]types.EventTypeNID, error)
	// Look up the numeric IDs for a list of string event state keys.
	// Returns a map from string state key to numeric ID for the state key.
	EventStateKeyNIDs(ctx context.Context, eventStateKeys []string) (map[string]types.EventStateKeyNID, error)
	// Look up the numeric state data IDs for each numeric state snapshot ID
	// The returned slice is sorted by numeric state snapshot ID.
	StateBlockNIDs(ctx context.Context, stateNIDs []types.StateSnapshotNID) ([]types.StateBlockNIDList, error)
	// Look up the state data for each numeric state data ID
	// The returned slice is sorted by numeric state data ID.
	StateEntries(ctx context.Context, stateBlockNIDs []types.StateBlockNID) ([]types.StateEntryList, error)
	// Look up the state data for the state key tuples for each numeric state block ID
	// This is used to fetch a subset of the frame state at a snapshot.
	// If a block doesn't contain any of the requested tuples then it can be discarded from the result.
	// The returned slice is sorted by numeric state block ID.
	StateEntriesForTuples(
		ctx context.Context,
		stateBlockNIDs []types.StateBlockNID,
		stateKeyTuples []types.StateKeyTuple,
	) ([]types.StateEntryList, error)
	// Look up the Events for a list of numeric event IDs.
	// Returns a sorted list of events.
	Events(ctx context.Context, frameVersion xtools.FrameVersion, eventNIDs []types.EventNID) ([]types.Event, error)
	// Look up snapshot NID for an event ID string
	SnapshotNIDFromEventID(ctx context.Context, eventID string) (types.StateSnapshotNID, error)
	BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error)
	// Stores a coddy frame event in the database. Returns the frame NID, the state snapshot or an error.
	StoreEvent(ctx context.Context, event xtools.PDU, frameInfo *types.FrameInfo, eventTypeNID types.EventTypeNID, eventStateKeyNID types.EventStateKeyNID, authEventNIDs []types.EventNID, isRejected bool) (types.EventNID, types.StateAtEvent, error)
	// Look up the state entries for a list of string event IDs
	// Returns an error if the there is an error talking to the database
	// Returns a types.MissingEventError if the event IDs aren't in the database.
	StateEntriesForEventIDs(ctx context.Context, eventIDs []string, excludeRejected bool) ([]types.StateEntry, error)
	// Look up the string event state keys for a list of numeric event state keys
	// Returns an error if there was a problem talking to the database.
	EventStateKeys(ctx context.Context, eventStateKeyNIDs []types.EventStateKeyNID) (map[types.EventStateKeyNID]string, error)
	// Look up the numeric IDs for a list of events.
	// Returns an error if there was a problem talking to the database.
	EventNIDs(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error)
	// Set the state at an event. FIXME TODO: "at"
	SetState(ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID) error
	// Lookup the event IDs for a batch of event numeric IDs.
	// Returns an error if the retrieval went wrong.
	EventIDs(ctx context.Context, eventNIDs []types.EventNID) (map[types.EventNID]string, error)
	// Opens and returns a frame updater, which locks the frame and opens a transaction.
	// The GetFrameUpdater must have Commit or Rollback called on it if this doesn't return an error.
	// If this returns an error then no further action is required.
	// IsEventRejected returns true if the event is known and rejected.
	IsEventRejected(ctx context.Context, frameNID types.FrameNID, eventID string) (rejected bool, err error)
	GetFrameUpdater(ctx context.Context, frameInfo *types.FrameInfo) (*shared.FrameUpdater, error)
	// Look up event references for the latest events in the frame and the current state snapshot.
	// Returns the latest events, the current state and the maximum depth of the latest events plus 1.
	// Returns an error if there was a problem talking to the database.
	LatestEventIDs(ctx context.Context, frameNID types.FrameNID) ([]string, types.StateSnapshotNID, int64, error)
	// Look up the active invites targeting a user in a frame and return the
	// numeric state key IDs for the user IDs who sent them along with the event IDs for the invites.
	// Returns an error if there was a problem talking to the database.
	GetInvitesForUser(ctx context.Context, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID) (senderUserIDs []types.EventStateKeyNID, eventIDs []string, inviteEventJSON []byte, err error)
	// Save a given frame alias with the frame ID it refers to.
	// Returns an error if there was a problem talking to the database.
	SetFrameAlias(ctx context.Context, alias string, frameID string, creatorUserID string) error
	// Look up the frame ID a given alias refers to.
	// Returns an error if there was a problem talking to the database.
	GetFrameIDForAlias(ctx context.Context, alias string) (string, error)
	// Look up all aliases referring to a given frame ID.
	// Returns an error if there was a problem talking to the database.
	GetAliasesForFrameID(ctx context.Context, frameID string) ([]string, error)
	// Get the user ID of the creator of an alias.
	// Returns an error if there was a problem talking to the database.
	GetCreatorIDForAlias(ctx context.Context, alias string) (string, error)
	// Remove a given frame alias.
	// Returns an error if there was a problem talking to the database.
	RemoveFrameAlias(ctx context.Context, alias string) error
	// Build a membership updater for the target user in a frame.
	MembershipUpdater(ctx context.Context, frameID, targetUserID string, targetLocal bool, frameVersion xtools.FrameVersion) (*shared.MembershipUpdater, error)
	// Lookup the membership of a given user in a given frame.
	// Returns the numeric ID of the latest membership event sent from this user
	// in this frame, along a boolean set to true if the user is still in this frame,
	// false if not.
	// Returns an error if there was a problem talking to the database.
	GetMembership(ctx context.Context, frameNID types.FrameNID, requestSenderID spec.SenderID) (membershipEventNID types.EventNID, stillInFrame, isFrameForgotten bool, err error)
	// Lookup the membership event numeric IDs for all user that are or have
	// been members of a given frame. Only lookup events of "join" membership if
	// joinOnly is set to true.
	// Returns an error if there was a problem talking to the database.
	GetMembershipEventNIDsForFrame(ctx context.Context, frameNID types.FrameNID, joinOnly bool, localOnly bool) ([]types.EventNID, error)
	// EventsFromIDs looks up the Events for a list of event IDs. Does not error if event was
	// not found.
	// Returns an error if the retrieval went wrong.
	EventsFromIDs(ctx context.Context, frameInfo *types.FrameInfo, eventIDs []string) ([]types.Event, error)
	// PerformPublish publishes or unpublishes a frame from the frame directory. Returns a database error, if any.
	PublishFrame(ctx context.Context, frameID, appserviceID, networkID string, publish bool) error
	// Returns a list of frame IDs for frames which are published.
	GetPublishedFrames(ctx context.Context, networkID string, includeAllNetworks bool) ([]string, error)
	// Returns whether a given frame is published or not.
	GetPublishedFrame(ctx context.Context, frameID string) (bool, error)

	// TODO: factor out - from currentstateserver

	// GetStateEvent returns the state event of a given type for a given frame with a given state key
	// If no event could be found, returns nil
	// If there was an issue during the retrieval, returns an error
	GetStateEvent(ctx context.Context, frameID, evType, stateKey string) (*types.HeaderedEvent, error)
	GetStateEventsWithEventType(ctx context.Context, frameID, evType string) ([]*types.HeaderedEvent, error)
	// GetFramesByMembership returns a list of frame IDs matching the provided membership and user ID (as state_key).
	GetFramesByMembership(ctx context.Context, userID spec.UserID, membership string) ([]string, error)
	// GetBulkStateContent returns all state events which match a given frame ID and a given state key tuple. Both must be satisfied for a match.
	// If a tuple has the StateKey of '*' and allowWildcards=true then all state events with the EventType should be returned.
	GetBulkStateContent(ctx context.Context, frameIDs []string, tuples []xtools.StateKeyTuple, allowWildcards bool) ([]tables.StrippedEvent, error)
	// JoinedUsersSetInFrames returns how many times each of the given users appears across the given frames.
	JoinedUsersSetInFrames(ctx context.Context, frameIDs, userIDs []string, localOnly bool) (map[string]int, error)
	// GetLocalServerInFrame returns true if we think we're in a given frame or false otherwise.
	GetLocalServerInFrame(ctx context.Context, frameNID types.FrameNID) (bool, error)
	// GetServerInFrame returns true if we think a server is in a given frame or false otherwise.
	GetServerInFrame(ctx context.Context, frameNID types.FrameNID, serverName spec.ServerName) (bool, error)
	// GetKnownUsers searches all users that userID knows about.
	GetKnownUsers(ctx context.Context, userID, searchString string, limit int) ([]string, error)
	// GetKnownFrames returns a list of all frames we know about.
	GetKnownFrames(ctx context.Context) ([]string, error)
	// ForgetFrame sets a flag in the membership table, that the user wishes to forget a specific frame
	ForgetFrame(ctx context.Context, userID, frameID string, forget bool) error

	GetHistoryVisibilityState(ctx context.Context, frameInfo *types.FrameInfo, eventID string, domain string) ([]xtools.PDU, error)
	GetLeftUsers(ctx context.Context, userIDs []string) ([]string, error)
	PurgeFrame(ctx context.Context, frameID string) error
	UpgradeFrame(ctx context.Context, oldFrameID, newFrameID, eventSender string) error

	// GetMembershipForHistoryVisibility queries the membership events for the given eventIDs.
	// Returns a map from (input) eventID -> membership event. If no membership event is found, returns an empty event, resulting in
	// a membership of "leave" when calculating history visibility.
	GetMembershipForHistoryVisibility(
		ctx context.Context, userNID types.EventStateKeyNID, info *types.FrameInfo, eventIDs ...string,
	) (map[string]*types.HeaderedEvent, error)
	GetOrCreateFrameInfo(ctx context.Context, event xtools.PDU) (*types.FrameInfo, error)
	GetFrameVersion(ctx context.Context, frameID string) (xtools.FrameVersion, error)
	GetOrCreateEventTypeNID(ctx context.Context, eventType string) (eventTypeNID types.EventTypeNID, err error)
	GetOrCreateEventStateKeyNID(ctx context.Context, eventStateKey *string) (types.EventStateKeyNID, error)
	MaybeRedactEvent(
		ctx context.Context, frameInfo *types.FrameInfo, eventNID types.EventNID, event xtools.PDU, plResolver state.PowerLevelResolver, querier api.QuerySenderIDAPI,
	) (xtools.PDU, xtools.PDU, error)
}

type UserFrameKeys interface {
	// InsertUserFramePrivatePublicKey inserts the given private key as well as the public key for it. This should be used
	// when creating keys locally.
	InsertUserFramePrivatePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID, key ed25519.PrivateKey) (result ed25519.PrivateKey, err error)
	// InsertUserFramePublicKey inserts the given public key, this should be used for users NOT local to this server
	InsertUserFramePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID, key ed25519.PublicKey) (result ed25519.PublicKey, err error)
	// SelectUserFramePrivateKey selects the private key for the given user and frame combination
	SelectUserFramePrivateKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (key ed25519.PrivateKey, err error)
	// SelectUserFramePublicKey selects the public key for the given user and frame combination
	SelectUserFramePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (key ed25519.PublicKey, err error)
	// SelectUserIDsForPublicKeys selects all userIDs for the requested senderKeys. Returns a map from frameID -> map from publicKey to userID.
	// If a senderKey can't be found, it is omitted in the result.
	// TODO: Why is the result map indexed by string not public key?
	// TODO: Shouldn't the input & result map be changed to be indexed by string instead of the FrameID struct?
	SelectUserIDsForPublicKeys(ctx context.Context, publicKeys map[spec.FrameID][]ed25519.PublicKey) (map[spec.FrameID]map[string]string, error)
}

type FrameDatabase interface {
	EventDatabase
	UserFrameKeys
	AssignFrameNID(ctx context.Context, frameID spec.FrameID, frameVersion xtools.FrameVersion) (frameNID types.FrameNID, err error)
	// FrameInfo returns frame information for the given frame ID, or nil if there is no frame.
	FrameInfo(ctx context.Context, frameID string) (*types.FrameInfo, error)
	FrameInfoByNID(ctx context.Context, frameNID types.FrameNID) (*types.FrameInfo, error)
	// IsEventRejected returns true if the event is known and rejected.
	IsEventRejected(ctx context.Context, frameNID types.FrameNID, eventID string) (rejected bool, err error)
	MissingAuthPrevEvents(ctx context.Context, e xtools.PDU) (missingAuth, missingPrev []string, err error)
	UpgradeFrame(ctx context.Context, oldFrameID, newFrameID, eventSender string) error
	GetFrameUpdater(ctx context.Context, frameInfo *types.FrameInfo) (*shared.FrameUpdater, error)
	GetMembershipEventNIDsForFrame(ctx context.Context, frameNID types.FrameNID, joinOnly bool, localOnly bool) ([]types.EventNID, error)
	StateBlockNIDs(ctx context.Context, stateNIDs []types.StateSnapshotNID) ([]types.StateBlockNIDList, error)
	StateEntries(ctx context.Context, stateBlockNIDs []types.StateBlockNID) ([]types.StateEntryList, error)
	BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error)
	StateEntriesForTuples(ctx context.Context, stateBlockNIDs []types.StateBlockNID, stateKeyTuples []types.StateKeyTuple) ([]types.StateEntryList, error)
	AddState(ctx context.Context, frameNID types.FrameNID, stateBlockNIDs []types.StateBlockNID, state []types.StateEntry) (types.StateSnapshotNID, error)
	LatestEventIDs(ctx context.Context, frameNID types.FrameNID) ([]string, types.StateSnapshotNID, int64, error)
	GetOrCreateFrameInfo(ctx context.Context, event xtools.PDU) (*types.FrameInfo, error)
	GetOrCreateEventTypeNID(ctx context.Context, eventType string) (eventTypeNID types.EventTypeNID, err error)
	GetOrCreateEventStateKeyNID(ctx context.Context, eventStateKey *string) (types.EventStateKeyNID, error)
	GetStateEvent(ctx context.Context, frameID, evType, stateKey string) (*types.HeaderedEvent, error)
}

type EventDatabase interface {
	EventTypeNIDs(ctx context.Context, eventTypes []string) (map[string]types.EventTypeNID, error)
	EventStateKeys(ctx context.Context, eventStateKeyNIDs []types.EventStateKeyNID) (map[types.EventStateKeyNID]string, error)
	EventStateKeyNIDs(ctx context.Context, eventStateKeys []string) (map[string]types.EventStateKeyNID, error)
	StateEntriesForEventIDs(ctx context.Context, eventIDs []string, excludeRejected bool) ([]types.StateEntry, error)
	EventNIDs(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error)
	SetState(ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID) error
	StateAtEventIDs(ctx context.Context, eventIDs []string) ([]types.StateAtEvent, error)
	SnapshotNIDFromEventID(ctx context.Context, eventID string) (types.StateSnapshotNID, error)
	EventIDs(ctx context.Context, eventNIDs []types.EventNID) (map[types.EventNID]string, error)
	EventsFromIDs(ctx context.Context, frameInfo *types.FrameInfo, eventIDs []string) ([]types.Event, error)
	Events(ctx context.Context, frameVersion xtools.FrameVersion, eventNIDs []types.EventNID) ([]types.Event, error)
	// MaybeRedactEvent returns the redaction event and the redacted event if this call resulted in a redaction, else an error
	// (nil if there was nothing to do)
	MaybeRedactEvent(
		ctx context.Context, frameInfo *types.FrameInfo, eventNID types.EventNID, event xtools.PDU, plResolver state.PowerLevelResolver, querier api.QuerySenderIDAPI,
	) (xtools.PDU, xtools.PDU, error)
	StoreEvent(ctx context.Context, event xtools.PDU, frameInfo *types.FrameInfo, eventTypeNID types.EventTypeNID, eventStateKeyNID types.EventStateKeyNID, authEventNIDs []types.EventNID, isRejected bool) (types.EventNID, types.StateAtEvent, error)
}
