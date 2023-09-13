package storage

import (
	"context"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/api"
	rstypes "github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/syncapi/storage/shared"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
	userapi "github.com/withqb/coddy/services/userapi/api"
)

type DatabaseTransaction interface {
	sqlutil.Transaction
	SharedUsers

	MaxStreamPositionForPDUs(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForReceipts(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForInvites(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForAccountData(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForSendToDeviceMessages(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForNotificationData(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForPresence(ctx context.Context) (types.StreamPosition, error)
	MaxStreamPositionForRelations(ctx context.Context) (types.StreamPosition, error)

	CurrentState(ctx context.Context, frameID string, stateFilterPart *synctypes.StateFilter, excludeEventIDs []string) ([]*rstypes.HeaderedEvent, error)
	GetStateDeltasForFullStateSync(ctx context.Context, device *userapi.Device, r types.Range, userID string, stateFilter *synctypes.StateFilter, rsAPI api.SyncDataframeAPI) ([]types.StateDelta, []string, error)
	GetStateDeltas(ctx context.Context, device *userapi.Device, r types.Range, userID string, stateFilter *synctypes.StateFilter, rsAPI api.SyncDataframeAPI) ([]types.StateDelta, []string, error)
	FrameIDsWithMembership(ctx context.Context, userID string, membership string) ([]string, error)
	MembershipCount(ctx context.Context, frameID, membership string, pos types.StreamPosition) (int, error)
	GetFrameSummary(ctx context.Context, frameID, userID string) (summary *types.Summary, err error)
	RecentEvents(ctx context.Context, frameIDs []string, r types.Range, eventFilter *synctypes.FrameEventFilter, chronologicalOrder bool, onlySyncEvents bool) (map[string]types.RecentEvents, error)
	GetBackwardTopologyPos(ctx context.Context, events []*rstypes.HeaderedEvent) (types.TopologyToken, error)
	PositionInTopology(ctx context.Context, eventID string) (pos types.StreamPosition, spos types.StreamPosition, err error)
	InviteEventsInRange(ctx context.Context, targetUserID string, r types.Range) (map[string]*rstypes.HeaderedEvent, map[string]*rstypes.HeaderedEvent, types.StreamPosition, error)
	PeeksInRange(ctx context.Context, userID, deviceID string, r types.Range) (peeks []types.Peek, err error)
	FrameReceiptsAfter(ctx context.Context, frameIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error)
	// AllJoinedUsersInFrames returns a map of frame ID to a list of all joined user IDs.
	AllJoinedUsersInFrames(ctx context.Context) (map[string][]string, error)
	// AllJoinedUsersInFrame returns a map of frame ID to a list of all joined user IDs for a given frame.
	AllJoinedUsersInFrame(ctx context.Context, frameIDs []string) (map[string][]string, error)
	// AllPeekingDevicesInFrames returns a map of frame ID to a list of all peeking devices.
	AllPeekingDevicesInFrames(ctx context.Context) (map[string][]types.PeekingDevice, error)
	// Events lookups a list of event by their event ID.
	// Returns a list of events matching the requested IDs found in the database.
	// If an event is not found in the database then it will be omitted from the list.
	// Returns an error if there was a problem talking with the database.
	// Does not include any transaction IDs in the returned events.
	Events(ctx context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error)
	// GetStateEvent returns the Coddy state event of a given type for a given frame with a given state key
	// If no event could be found, returns nil
	// If there was an issue during the retrieval, returns an error
	GetStateEvent(ctx context.Context, frameID, evType, stateKey string) (*rstypes.HeaderedEvent, error)
	// GetStateEventsForFrame fetches the state events for a given frame.
	// Returns an empty slice if no state events could be found for this frame.
	// Returns an error if there was an issue with the retrieval.
	GetStateEventsForFrame(ctx context.Context, frameID string, stateFilterPart *synctypes.StateFilter) (stateEvents []*rstypes.HeaderedEvent, err error)
	// GetAccountDataInRange returns all account data for a given user inserted or
	// updated between two given positions
	// Returns a map following the format data[frameID] = []dataTypes
	// If no data is retrieved, returns an empty map
	// If there was an issue with the retrieval, returns an error
	GetAccountDataInRange(ctx context.Context, userID string, r types.Range, accountDataFilterPart *synctypes.EventFilter) (map[string][]string, types.StreamPosition, error)
	// GetEventsInTopologicalRange retrieves all of the events on a given ordering using the given extremities and limit.
	// If backwardsOrdering is true, the most recent event must be first, else last.
	// Returns the filtered StreamEvents on success. Returns **unfiltered** StreamEvents and ErrNoEventsForFilter if
	// the provided filter removed all events, this can be used to still calculate the start/end position. (e.g for `/messages`)
	GetEventsInTopologicalRange(ctx context.Context, from, to *types.TopologyToken, frameID string, filter *synctypes.FrameEventFilter, backwardOrdering bool) (events []types.StreamEvent, start, end types.TopologyToken, err error)
	// EventPositionInTopology returns the depth and stream position of the given event.
	EventPositionInTopology(ctx context.Context, eventID string) (types.TopologyToken, error)
	// BackwardExtremitiesForFrame returns a map of backwards extremity event ID to a list of its prev_events.
	BackwardExtremitiesForFrame(ctx context.Context, frameID string) (backwardExtremities map[string][]string, err error)
	// StreamEventsToEvents converts streamEvent to Event. If device is non-nil and
	// matches the streamevent.transactionID device then the transaction ID gets
	// added to the unsigned section of the output event.
	StreamEventsToEvents(ctx context.Context, device *userapi.Device, in []types.StreamEvent, rsAPI api.SyncDataframeAPI) []*rstypes.HeaderedEvent
	// SendToDeviceUpdatesForSync returns a list of send-to-device updates. It returns the
	// relevant events within the given ranges for the supplied user ID and device ID.
	SendToDeviceUpdatesForSync(ctx context.Context, userID, deviceID string, from, to types.StreamPosition) (pos types.StreamPosition, events []types.SendToDeviceEvent, err error)
	// GetFrameReceipts gets all receipts for a given frameID
	GetFrameReceipts(ctx context.Context, frameIDs []string, streamPos types.StreamPosition) ([]types.OutputReceiptEvent, error)
	SelectContextEvent(ctx context.Context, frameID, eventID string) (int, rstypes.HeaderedEvent, error)
	SelectContextBeforeEvent(ctx context.Context, id int, frameID string, filter *synctypes.FrameEventFilter) ([]*rstypes.HeaderedEvent, error)
	SelectContextAfterEvent(ctx context.Context, id int, frameID string, filter *synctypes.FrameEventFilter) (int, []*rstypes.HeaderedEvent, error)
	StreamToTopologicalPosition(ctx context.Context, frameID string, streamPos types.StreamPosition, backwardOrdering bool) (types.TopologyToken, error)
	IgnoresForUser(ctx context.Context, userID string) (*types.IgnoredUsers, error)
	// SelectMembershipForUser returns the membership of the user before and including the given position. If no membership can be found
	// returns "leave", the topological position and no error. If an error occurs, other than sql.ErrNoRows, returns that and an empty
	// string as the membership.
	SelectMembershipForUser(ctx context.Context, frameID, userID string, pos int64) (membership string, topologicalPos int, err error)
	// getUserUnreadNotificationCountsForFrames returns the unread notifications for the given frames
	GetUserUnreadNotificationCountsForFrames(ctx context.Context, userID string, frameIDs map[string]string) (map[string]*eventutil.NotificationData, error)
	GetPresences(ctx context.Context, userID []string) ([]*types.PresenceInternal, error)
	PresenceAfter(ctx context.Context, after types.StreamPosition, filter synctypes.EventFilter) (map[string]*types.PresenceInternal, error)
	RelationsFor(ctx context.Context, frameID, eventID, relType, eventType string, from, to types.StreamPosition, backwards bool, limit int) (events []types.StreamEvent, prevBatch, nextBatch string, err error)
}

type Database interface {
	Presence
	Notifications

	NewDatabaseSnapshot(ctx context.Context) (*shared.DatabaseTransaction, error)
	NewDatabaseTransaction(ctx context.Context) (*shared.DatabaseTransaction, error)

	// Events lookups a list of event by their event ID.
	// Returns a list of events matching the requested IDs found in the database.
	// If an event is not found in the database then it will be omitted from the list.
	// Returns an error if there was a problem talking with the database.
	// Does not include any transaction IDs in the returned events.
	Events(ctx context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error)
	// WriteEvent into the database. It is not safe to call this function from multiple goroutines, as it would create races
	// when generating the sync stream position for this event. Returns the sync stream position for the inserted event.
	// Returns an error if there was a problem inserting this event.
	WriteEvent(ctx context.Context, ev *rstypes.HeaderedEvent, addStateEvents []*rstypes.HeaderedEvent,
		addStateEventIDs []string, removeStateEventIDs []string, transactionID *api.TransactionID, excludeFromSync bool,
		historyVisibility xtools.HistoryVisibility,
	) (types.StreamPosition, error)
	// PurgeFrameState completely purges frame state from the sync API. This is done when
	// receiving an output event that completely resets the state.
	PurgeFrameState(ctx context.Context, frameID string) error
	// PurgeFrame entirely eliminates a frame from the sync API, timeline, state and all.
	PurgeFrame(ctx context.Context, frameID string) error
	// UpsertAccountData keeps track of new or updated account data, by saving the type
	// of the new/updated data, and the user ID and frame ID the data is related to (empty)
	// frame ID means the data isn't specific to any frame)
	// If no data with the given type, user ID and frame ID exists in the database,
	// creates a new row, else update the existing one
	// Returns an error if there was an issue with the upsert
	UpsertAccountData(ctx context.Context, userID, frameID, dataType string) (types.StreamPosition, error)
	// AddInviteEvent stores a new invite event for a user.
	// If the invite was successfully stored this returns the stream ID it was stored at.
	// Returns an error if there was a problem communicating with the database.
	AddInviteEvent(ctx context.Context, inviteEvent *rstypes.HeaderedEvent) (types.StreamPosition, error)
	// RetireInviteEvent removes an old invite event from the database. Returns the new position of the retired invite.
	// Returns an error if there was a problem communicating with the database.
	RetireInviteEvent(ctx context.Context, inviteEventID string) (types.StreamPosition, error)
	// AddPeek adds a new peek to our DB for a given frame by a given user's device.
	// Returns an error if there was a problem communicating with the database.
	AddPeek(ctx context.Context, FrameID, UserID, DeviceID string) (types.StreamPosition, error)
	// DeletePeek removes an existing peek from the database for a given frame by a user's device.
	// Returns an error if there was a problem communicating with the database.
	DeletePeek(ctx context.Context, frameID, userID, deviceID string) (sp types.StreamPosition, err error)
	// DeletePeek deletes all peeks for a given frame by a given user
	// Returns an error if there was a problem communicating with the database.
	DeletePeeks(ctx context.Context, FrameID, UserID string) (types.StreamPosition, error)
	// StoreNewSendForDeviceMessage stores a new send-to-device event for a user's device.
	StoreNewSendForDeviceMessage(ctx context.Context, userID, deviceID string, event xtools.SendToDeviceEvent) (types.StreamPosition, error)
	// CleanSendToDeviceUpdates removes all send-to-device messages BEFORE the specified
	// from position, preventing the send-to-device table from growing indefinitely.
	CleanSendToDeviceUpdates(ctx context.Context, userID, deviceID string, before types.StreamPosition) (err error)
	// GetFilter looks up the filter associated with a given local user and filter ID
	// and populates the target filter. Otherwise returns an error if no such filter exists
	// or if there was an error talking to the database.
	GetFilter(ctx context.Context, target *synctypes.Filter, localpart string, filterID string) error
	// PutFilter puts the passed filter into the database.
	// Returns the filterID as a string. Otherwise returns an error if something
	// goes wrong.
	PutFilter(ctx context.Context, localpart string, filter *synctypes.Filter) (string, error)
	// RedactEvent wipes an event in the database and sets the unsigned.redacted_because key to the redaction event
	RedactEvent(ctx context.Context, redactedEventID string, redactedBecause *rstypes.HeaderedEvent, querier api.QuerySenderIDAPI) error
	// StoreReceipt stores new receipt events
	StoreReceipt(ctx context.Context, frameId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error)
	UpdateIgnoresForUser(ctx context.Context, userID string, ignores *types.IgnoredUsers) error
	ReIndex(ctx context.Context, limit, afterID int64) (map[int64]rstypes.HeaderedEvent, error)
	UpdateRelations(ctx context.Context, event *rstypes.HeaderedEvent) error
	RedactRelations(ctx context.Context, frameID, redactedEventID string) error
	SelectMemberships(
		ctx context.Context,
		frameID string, pos types.TopologyToken,
		membership, notMembership *string,
	) (eventIDs []string, err error)
}

type Presence interface {
	GetPresences(ctx context.Context, userIDs []string) ([]*types.PresenceInternal, error)
	UpdatePresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, lastActiveTS spec.Timestamp, fromSync bool) (types.StreamPosition, error)
}

type SharedUsers interface {
	// SharedUsers returns a subset of otherUserIDs that share a frame with userID.
	SharedUsers(ctx context.Context, userID string, otherUserIDs []string) ([]string, error)
}

type Notifications interface {
	// UpsertFrameUnreadNotificationCounts updates the notification statistics about a (user, frame) key.
	UpsertFrameUnreadNotificationCounts(ctx context.Context, userID, frameID string, notificationCount, highlightCount int) (types.StreamPosition, error)
}
