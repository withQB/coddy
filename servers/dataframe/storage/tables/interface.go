package tables

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"

	"github.com/tidwall/gjson"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/servers/dataframe/types"
)

var OptimisationNotSupportedError = errors.New("optimisation not supported")

type EventJSONPair struct {
	EventNID  types.EventNID
	EventJSON []byte
}

type EventJSON interface {
	// Insert the event JSON. On conflict, replace the event JSON with the new value (for redactions).
	InsertEventJSON(ctx context.Context, tx *sql.Tx, eventNID types.EventNID, eventJSON []byte) error
	BulkSelectEventJSON(ctx context.Context, tx *sql.Tx, eventNIDs []types.EventNID) ([]EventJSONPair, error)
}

type EventTypes interface {
	InsertEventTypeNID(ctx context.Context, tx *sql.Tx, eventType string) (types.EventTypeNID, error)
	SelectEventTypeNID(ctx context.Context, tx *sql.Tx, eventType string) (types.EventTypeNID, error)
	BulkSelectEventTypeNID(ctx context.Context, txn *sql.Tx, eventTypes []string) (map[string]types.EventTypeNID, error)
}

type EventStateKeys interface {
	InsertEventStateKeyNID(ctx context.Context, txn *sql.Tx, eventStateKey string) (types.EventStateKeyNID, error)
	SelectEventStateKeyNID(ctx context.Context, txn *sql.Tx, eventStateKey string) (types.EventStateKeyNID, error)
	BulkSelectEventStateKeyNID(ctx context.Context, txn *sql.Tx, eventStateKeys []string) (map[string]types.EventStateKeyNID, error)
	BulkSelectEventStateKey(ctx context.Context, txn *sql.Tx, eventStateKeyNIDs []types.EventStateKeyNID) (map[types.EventStateKeyNID]string, error)
}

type Events interface {
	InsertEvent(
		ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, eventTypeNID types.EventTypeNID,
		eventStateKeyNID types.EventStateKeyNID, eventID string,
		authEventNIDs []types.EventNID, depth int64, isRejected bool,
	) (types.EventNID, types.StateSnapshotNID, error)
	SelectEvent(ctx context.Context, txn *sql.Tx, eventID string) (types.EventNID, types.StateSnapshotNID, error)
	BulkSelectSnapshotsFromEventIDs(ctx context.Context, txn *sql.Tx, eventIDs []string) (map[types.StateSnapshotNID][]string, error)
	// bulkSelectStateEventByID lookups a list of state events by event ID.
	// If any of the requested events are missing from the database it returns a types.MissingEventError
	BulkSelectStateEventByID(ctx context.Context, txn *sql.Tx, eventIDs []string, excludeRejected bool) ([]types.StateEntry, error)
	BulkSelectStateEventByNID(ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID, stateKeyTuples []types.StateKeyTuple) ([]types.StateEntry, error)
	// BulkSelectStateAtEventByID lookups the state at a list of events by event ID.
	// If any of the requested events are missing from the database it returns a types.MissingEventError.
	// If we do not have the state for any of the requested events it returns a types.MissingEventError.
	BulkSelectStateAtEventByID(ctx context.Context, txn *sql.Tx, eventIDs []string) ([]types.StateAtEvent, error)
	UpdateEventState(ctx context.Context, txn *sql.Tx, eventNID types.EventNID, stateNID types.StateSnapshotNID) error
	SelectEventSentToOutput(ctx context.Context, txn *sql.Tx, eventNID types.EventNID) (sentToOutput bool, err error)
	UpdateEventSentToOutput(ctx context.Context, txn *sql.Tx, eventNID types.EventNID) error
	SelectEventID(ctx context.Context, txn *sql.Tx, eventNID types.EventNID) (eventID string, err error)
	BulkSelectStateAtEventAndReference(ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID) ([]types.StateAtEventAndReference, error)
	// BulkSelectEventID returns a map from numeric event ID to string event ID.
	BulkSelectEventID(ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID) (map[types.EventNID]string, error)
	// BulkSelectEventNIDs returns a map from string event ID to numeric event ID.
	// If an event ID is not in the database then it is omitted from the map.
	BulkSelectEventNID(ctx context.Context, txn *sql.Tx, eventIDs []string) (map[string]types.EventMetadata, error)
	BulkSelectUnsentEventNID(ctx context.Context, txn *sql.Tx, eventIDs []string) (map[string]types.EventMetadata, error)
	SelectMaxEventDepth(ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID) (int64, error)
	SelectFrameNIDsForEventNIDs(ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID) (frameNIDs map[types.EventNID]types.FrameNID, err error)
	SelectEventRejected(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, eventID string) (rejected bool, err error)
}

type Frames interface {
	InsertFrameNID(ctx context.Context, txn *sql.Tx, frameID string, frameVersion xtools.FrameVersion) (types.FrameNID, error)
	SelectFrameNID(ctx context.Context, txn *sql.Tx, frameID string) (types.FrameNID, error)
	SelectFrameNIDForUpdate(ctx context.Context, txn *sql.Tx, frameID string) (types.FrameNID, error)
	SelectLatestEventNIDs(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID) ([]types.EventNID, types.StateSnapshotNID, error)
	SelectLatestEventsNIDsForUpdate(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID) ([]types.EventNID, types.EventNID, types.StateSnapshotNID, error)
	UpdateLatestEventNIDs(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, eventNIDs []types.EventNID, lastEventSentNID types.EventNID, stateSnapshotNID types.StateSnapshotNID) error
	SelectFrameVersionsForFrameNIDs(ctx context.Context, txn *sql.Tx, frameNID []types.FrameNID) (map[types.FrameNID]xtools.FrameVersion, error)
	SelectFrameInfo(ctx context.Context, txn *sql.Tx, frameID string) (*types.FrameInfo, error)
	SelectFrameIDsWithEvents(ctx context.Context, txn *sql.Tx) ([]string, error)
	BulkSelectFrameIDs(ctx context.Context, txn *sql.Tx, frameNIDs []types.FrameNID) ([]string, error)
	BulkSelectFrameNIDs(ctx context.Context, txn *sql.Tx, frameIDs []string) ([]types.FrameNID, error)
}

type StateSnapshot interface {
	InsertState(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, stateBlockNIDs types.StateBlockNIDs) (stateNID types.StateSnapshotNID, err error)
	BulkSelectStateBlockNIDs(ctx context.Context, txn *sql.Tx, stateNIDs []types.StateSnapshotNID) ([]types.StateBlockNIDList, error)
	// BulkSelectStateForHistoryVisibility is a PostgreSQL-only optimisation for finding
	// which users are in a frame faster than having to load the entire frame state. In the
	// case of SQLite, this will return tables.OptimisationNotSupportedError.
	BulkSelectStateForHistoryVisibility(ctx context.Context, txn *sql.Tx, stateSnapshotNID types.StateSnapshotNID, domain string) ([]types.EventNID, error)

	BulkSelectMembershipForHistoryVisibility(
		ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameInfo *types.FrameInfo, eventIDs ...string,
	) (map[string]*types.HeaderedEvent, error)
}

type StateBlock interface {
	BulkInsertStateData(ctx context.Context, txn *sql.Tx, entries types.StateEntries) (types.StateBlockNID, error)
	BulkSelectStateBlockEntries(ctx context.Context, txn *sql.Tx, stateBlockNIDs types.StateBlockNIDs) ([][]types.EventNID, error)
	//BulkSelectFilteredStateBlockEntries(ctx context.Context, stateBlockNIDs []types.StateBlockNID, stateKeyTuples []types.StateKeyTuple) ([]types.StateEntryList, error)
}

type FrameAliases interface {
	InsertFrameAlias(ctx context.Context, txn *sql.Tx, alias string, frameID string, creatorUserID string) (err error)
	SelectFrameIDFromAlias(ctx context.Context, txn *sql.Tx, alias string) (frameID string, err error)
	SelectAliasesFromFrameID(ctx context.Context, txn *sql.Tx, frameID string) ([]string, error)
	SelectCreatorIDFromAlias(ctx context.Context, txn *sql.Tx, alias string) (creatorID string, err error)
	DeleteFrameAlias(ctx context.Context, txn *sql.Tx, alias string) (err error)
}

type PreviousEvents interface {
	InsertPreviousEvent(ctx context.Context, txn *sql.Tx, previousEventID string, eventNID types.EventNID) error
	// Check if the event reference exists
	// Returns sql.ErrNoRows if the event reference doesn't exist.
	SelectPreviousEventExists(ctx context.Context, txn *sql.Tx, eventID string) error
}

type Invites interface {
	InsertInviteEvent(ctx context.Context, txn *sql.Tx, inviteEventID string, frameNID types.FrameNID, targetUserNID, senderUserNID types.EventStateKeyNID, inviteEventJSON []byte) (bool, error)
	UpdateInviteRetired(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID) ([]string, error)
	// SelectInviteActiveForUserInFrame returns a list of sender state key NIDs and invite event IDs matching those nids.
	SelectInviteActiveForUserInFrame(ctx context.Context, txn *sql.Tx, targetUserNID types.EventStateKeyNID, frameNID types.FrameNID) ([]types.EventStateKeyNID, []string, []byte, error)
}

type MembershipState int64

const (
	MembershipStateLeaveOrBan MembershipState = 1
	MembershipStateInvite     MembershipState = 2
	MembershipStateJoin       MembershipState = 3
	MembershipStateKnock      MembershipState = 4
)

type Membership interface {
	InsertMembership(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID, localTarget bool) error
	SelectMembershipForUpdate(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID) (MembershipState, error)
	SelectMembershipFromFrameAndTarget(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID) (types.EventNID, MembershipState, bool, error)
	SelectMembershipsFromFrame(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, localOnly bool) (eventNIDs []types.EventNID, err error)
	SelectMembershipsFromFrameAndMembership(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, membership MembershipState, localOnly bool) (eventNIDs []types.EventNID, err error)
	UpdateMembership(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership MembershipState, eventNID types.EventNID, forgotten bool) (bool, error)
	SelectFramesWithMembership(ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, membershipState MembershipState) ([]types.FrameNID, error)
	// SelectJoinedUsersSetForFrames returns how many times each of the given users appears across the given frames.
	SelectJoinedUsersSetForFrames(ctx context.Context, txn *sql.Tx, frameNIDs []types.FrameNID, userNIDs []types.EventStateKeyNID, localOnly bool) (map[types.EventStateKeyNID]int, error)
	SelectKnownUsers(ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, searchString string, limit int) ([]string, error)
	UpdateForgetMembership(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID, forget bool) error
	SelectLocalServerInFrame(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID) (bool, error)
	SelectServerInFrame(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, serverName spec.ServerName) (bool, error)
	DeleteMembership(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, targetUserNID types.EventStateKeyNID) error
	SelectJoinedUsers(ctx context.Context, txn *sql.Tx, targetUserNIDs []types.EventStateKeyNID) ([]types.EventStateKeyNID, error)
}

type Published interface {
	UpsertFramePublished(ctx context.Context, txn *sql.Tx, frameID, appserviceID, networkID string, published bool) (err error)
	SelectPublishedFromFrameID(ctx context.Context, txn *sql.Tx, frameID string) (published bool, err error)
	SelectAllPublishedFrames(ctx context.Context, txn *sql.Tx, networkdID string, published, includeAllNetworks bool) ([]string, error)
}

type RedactionInfo struct {
	// whether this redaction is validated (we have both events)
	Validated bool
	// the ID of the event being redacted
	RedactsEventID string
	// the ID of the redaction event
	RedactionEventID string
}

type Redactions interface {
	InsertRedaction(ctx context.Context, txn *sql.Tx, info RedactionInfo) error
	// SelectRedactionInfoByRedactionEventID returns the redaction info for the given redaction event ID, or nil if there is no match.
	SelectRedactionInfoByRedactionEventID(ctx context.Context, txn *sql.Tx, redactionEventID string) (*RedactionInfo, error)
	// SelectRedactionInfoByEventBeingRedacted returns the redaction info for the given redacted event ID, or nil if there is no match.
	SelectRedactionInfoByEventBeingRedacted(ctx context.Context, txn *sql.Tx, eventID string) (*RedactionInfo, error)
	// Mark this redaction event as having been validated. This means we have both sides of the redaction and have
	// successfully redacted the event JSON.
	MarkRedactionValidated(ctx context.Context, txn *sql.Tx, redactionEventID string, validated bool) error
}

type Purge interface {
	PurgeFrame(
		ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, frameID string,
	) error
}

type UserFrameKeys interface {
	// InsertUserFramePrivatePublicKey inserts the given private key as well as the public key for it. This should be used
	// when creating keys locally.
	InsertUserFramePrivatePublicKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID, key ed25519.PrivateKey) (ed25519.PrivateKey, error)
	// InsertUserFramePublicKey inserts the given public key, this should be used for users NOT local to this server
	InsertUserFramePublicKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID, key ed25519.PublicKey) (ed25519.PublicKey, error)
	// SelectUserFramePrivateKey selects the private key for the given user and frame combination
	SelectUserFramePrivateKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID) (ed25519.PrivateKey, error)
	// SelectUserFramePublicKey selects the public key for the given user and frame combination
	SelectUserFramePublicKey(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID, frameNID types.FrameNID) (ed25519.PublicKey, error)
	// BulkSelectUserNIDs selects all userIDs for the requested senderKeys. Returns a map from publicKey -> types.UserFrameKeyPair.
	// If a senderKey can't be found, it is omitted in the result.
	BulkSelectUserNIDs(ctx context.Context, txn *sql.Tx, senderKeys map[types.FrameNID][]ed25519.PublicKey) (map[string]types.UserFrameKeyPair, error)
	// SelectAllPublicKeysForUser returns all known public keys for a user. Returns a map from frame NID -> public key
	SelectAllPublicKeysForUser(ctx context.Context, txn *sql.Tx, userNID types.EventStateKeyNID) (map[types.FrameNID]ed25519.PublicKey, error)
}

// StrippedEvent represents a stripped event for returning extracted content values.
type StrippedEvent struct {
	FrameID       string
	EventType    string
	StateKey     string
	ContentValue string
}

// ExtractContentValue from the given state event. For example, given an m.frame.name event with:
// content: { name: "Foo" }
// this returns "Foo".
func ExtractContentValue(ev *types.HeaderedEvent) string {
	content := ev.Content()
	key := ""
	switch ev.Type() {
	case spec.MFrameCreate:
		key = "creator"
	case spec.MFrameCanonicalAlias:
		key = "alias"
	case spec.MFrameHistoryVisibility:
		key = "history_visibility"
	case spec.MFrameJoinRules:
		key = "join_rule"
	case spec.MFrameMember:
		key = "membership"
	case spec.MFrameName:
		key = "name"
	case "m.frame.avatar":
		key = "url"
	case "m.frame.topic":
		key = "topic"
	case "m.frame.guest_access":
		key = "guest_access"
	}
	result := gjson.GetBytes(content, key)
	if !result.Exists() {
		return ""
	}
	// this returns the empty string if this is not a string type
	return result.Str
}
