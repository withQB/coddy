package shared

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/state"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"
	"github.com/withqb/coddy/servers/dataframe/types"
)

// Ideally, when we have both events we should redact the event JSON and forget about the redaction, but we currently
// don't because the redaction code is brand new. When we are more certain that redactions don't misbehave or are
// vulnerable to attacks from remote servers (e.g a server bypassing event auth rules shouldn't redact our data)
// then we should flip this to true. This will mean redactions /actually delete information irretrievably/ which
// will be necessary for compliance with the law. Note that downstream components (syncapi) WILL delete information
// in their database on receipt of a redaction. Also note that we still modify the event JSON to set the field
// unsigned.redacted_because - we just don't clear out the content fields yet.
const redactionsArePermanent = true

type Database struct {
	DB *sql.DB
	EventDatabase
	Cache              caching.DataFrameCaches
	Writer             sqlutil.Writer
	FramesTable         tables.Frames
	StateSnapshotTable tables.StateSnapshot
	StateBlockTable    tables.StateBlock
	FrameAliasesTable   tables.FrameAliases
	InvitesTable       tables.Invites
	MembershipTable    tables.Membership
	PublishedTable     tables.Published
	Purge              tables.Purge
	UserFrameKeyTable   tables.UserFrameKeys
	GetFrameUpdaterFn   func(ctx context.Context, frameInfo *types.FrameInfo) (*FrameUpdater, error)
}

// EventDatabase contains all tables needed to work with events
type EventDatabase struct {
	DB                  *sql.DB
	Cache               caching.DataFrameCaches
	Writer              sqlutil.Writer
	EventsTable         tables.Events
	EventJSONTable      tables.EventJSON
	EventTypesTable     tables.EventTypes
	EventStateKeysTable tables.EventStateKeys
	PrevEventsTable     tables.PreviousEvents
	RedactionsTable     tables.Redactions
}

func (d *Database) SupportsConcurrentFrameInputs() bool {
	return true
}

func (d *Database) GetMembershipForHistoryVisibility(
	ctx context.Context, userNID types.EventStateKeyNID, frameInfo *types.FrameInfo, eventIDs ...string,
) (map[string]*types.HeaderedEvent, error) {
	return d.StateSnapshotTable.BulkSelectMembershipForHistoryVisibility(ctx, nil, userNID, frameInfo, eventIDs...)
}

func (d *EventDatabase) EventTypeNIDs(
	ctx context.Context, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	return d.eventTypeNIDs(ctx, nil, eventTypes)
}

func (d *EventDatabase) eventTypeNIDs(
	ctx context.Context, txn *sql.Tx, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	result := make(map[string]types.EventTypeNID)
	// first try the cache
	fetchEventTypes := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		eventTypeNID, ok := d.Cache.GetEventTypeKey(eventType)
		if ok {
			result[eventType] = eventTypeNID
			continue
		}
		fetchEventTypes = append(fetchEventTypes, eventType)
	}
	if len(fetchEventTypes) > 0 {
		nids, err := d.EventTypesTable.BulkSelectEventTypeNID(ctx, txn, fetchEventTypes)
		if err != nil {
			return nil, err
		}
		for eventType, nid := range nids {
			result[eventType] = nid
			d.Cache.StoreEventTypeKey(nid, eventType)
		}
	}
	return result, nil
}

func (d *EventDatabase) EventStateKeys(
	ctx context.Context, eventStateKeyNIDs []types.EventStateKeyNID,
) (map[types.EventStateKeyNID]string, error) {
	result := make(map[types.EventStateKeyNID]string, len(eventStateKeyNIDs))
	fetch := make([]types.EventStateKeyNID, 0, len(eventStateKeyNIDs))
	for _, nid := range eventStateKeyNIDs {
		if key, ok := d.Cache.GetEventStateKey(nid); ok {
			result[nid] = key
		} else {
			fetch = append(fetch, nid)
		}
	}
	if len(fetch) > 0 {
		fromDB, err := d.EventStateKeysTable.BulkSelectEventStateKey(ctx, nil, fetch)
		if err != nil {
			return nil, err
		}
		for nid, key := range fromDB {
			result[nid] = key
			d.Cache.StoreEventStateKey(nid, key)
		}
	}
	return result, nil
}

func (d *EventDatabase) EventStateKeyNIDs(
	ctx context.Context, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	return d.eventStateKeyNIDs(ctx, nil, eventStateKeys)
}

func (d *EventDatabase) eventStateKeyNIDs(
	ctx context.Context, txn *sql.Tx, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	result := make(map[string]types.EventStateKeyNID)
	eventStateKeys = xutil.UniqueStrings(eventStateKeys)
	// first ask the cache about these keys
	fetchEventStateKeys := make([]string, 0, len(eventStateKeys))
	for _, eventStateKey := range eventStateKeys {
		eventStateKeyNID, ok := d.Cache.GetEventStateKeyNID(eventStateKey)
		if ok {
			result[eventStateKey] = eventStateKeyNID
			continue
		}
		fetchEventStateKeys = append(fetchEventStateKeys, eventStateKey)
	}

	if len(fetchEventStateKeys) > 0 {
		nids, err := d.EventStateKeysTable.BulkSelectEventStateKeyNID(ctx, txn, fetchEventStateKeys)
		if err != nil {
			return nil, err
		}
		for eventStateKey, nid := range nids {
			result[eventStateKey] = nid
			d.Cache.StoreEventStateKey(nid, eventStateKey)
		}
	}

	// We received some nids, but are still missing some, work out which and create them
	if len(eventStateKeys) > len(result) {
		var nid types.EventStateKeyNID
		var err error
		err = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
			for _, eventStateKey := range eventStateKeys {
				if _, ok := result[eventStateKey]; ok {
					continue
				}

				nid, err = d.assignStateKeyNID(ctx, txn, eventStateKey)
				if err != nil {
					return err
				}
				result[eventStateKey] = nid
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (d *EventDatabase) StateEntriesForEventIDs(
	ctx context.Context, eventIDs []string, excludeRejected bool,
) ([]types.StateEntry, error) {
	return d.EventsTable.BulkSelectStateEventByID(ctx, nil, eventIDs, excludeRejected)
}

func (d *Database) StateEntriesForTuples(
	ctx context.Context,
	stateBlockNIDs []types.StateBlockNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntryList, error) {
	return d.stateEntriesForTuples(ctx, nil, stateBlockNIDs, stateKeyTuples)
}

func (d *Database) stateEntriesForTuples(
	ctx context.Context, txn *sql.Tx,
	stateBlockNIDs []types.StateBlockNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntryList, error) {
	entries, err := d.StateBlockTable.BulkSelectStateBlockEntries(
		ctx, txn, stateBlockNIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("d.StateBlockTable.BulkSelectStateBlockEntries: %w", err)
	}
	lists := []types.StateEntryList{}
	for i, entry := range entries {
		entries, err := d.EventsTable.BulkSelectStateEventByNID(ctx, txn, entry, stateKeyTuples)
		if err != nil {
			return nil, fmt.Errorf("d.EventsTable.BulkSelectStateEventByNID: %w", err)
		}
		lists = append(lists, types.StateEntryList{
			StateBlockNID: stateBlockNIDs[i],
			StateEntries:  entries,
		})
	}
	return lists, nil
}

func (d *Database) FrameInfoByNID(ctx context.Context, frameNID types.FrameNID) (*types.FrameInfo, error) {
	frameIDs, err := d.FramesTable.BulkSelectFrameIDs(ctx, nil, []types.FrameNID{frameNID})
	if err != nil {
		return nil, err
	}
	if len(frameIDs) == 0 {
		return nil, fmt.Errorf("frame does not exist")
	}
	return d.frameInfo(ctx, nil, frameIDs[0])
}

func (d *Database) FrameInfo(ctx context.Context, frameID string) (*types.FrameInfo, error) {
	return d.frameInfo(ctx, nil, frameID)
}

func (d *Database) frameInfo(ctx context.Context, txn *sql.Tx, frameID string) (*types.FrameInfo, error) {
	frameInfo, err := d.FramesTable.SelectFrameInfo(ctx, txn, frameID)
	if err != nil {
		return nil, err
	}
	if frameInfo != nil {
		d.Cache.StoreDataFrameFrameID(frameInfo.FrameNID, frameID)
		d.Cache.StoreFrameVersion(frameID, frameInfo.FrameVersion)
	}
	return frameInfo, err
}

func (d *Database) AddState(
	ctx context.Context,
	frameNID types.FrameNID,
	stateBlockNIDs []types.StateBlockNID,
	state []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	return d.addState(ctx, nil, frameNID, stateBlockNIDs, state)
}

func (d *Database) addState(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID,
	stateBlockNIDs []types.StateBlockNID,
	state []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	if len(stateBlockNIDs) > 0 && len(state) > 0 {
		// Check to see if the event already appears in any of the existing state
		// blocks. If it does then we should not add it again, as this will just
		// result in excess state blocks and snapshots.
		// TDO: Investigate why this is happening - probably input_events.go!
		blocks, berr := d.StateBlockTable.BulkSelectStateBlockEntries(ctx, txn, stateBlockNIDs)
		if berr != nil {
			return 0, fmt.Errorf("d.StateBlockTable.BulkSelectStateBlockEntries: %w", berr)
		}
		var found bool
		for i := len(state) - 1; i >= 0; i-- {
			found = false
		blocksLoop:
			for _, events := range blocks {
				for _, event := range events {
					if state[i].EventNID == event {
						found = true
						break blocksLoop
					}
				}
			}
			if found {
				state = append(state[:i], state[i+1:]...)
			}
		}
	}
	err = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		if len(state) > 0 {
			// If there's any state left to add then let's add new blocks.
			var stateBlockNID types.StateBlockNID
			stateBlockNID, err = d.StateBlockTable.BulkInsertStateData(ctx, txn, state)
			if err != nil {
				return fmt.Errorf("d.StateBlockTable.BulkInsertStateData: %w", err)
			}
			stateBlockNIDs = append(stateBlockNIDs[:len(stateBlockNIDs):len(stateBlockNIDs)], stateBlockNID)
		}
		stateNID, err = d.StateSnapshotTable.InsertState(ctx, txn, frameNID, stateBlockNIDs)
		if err != nil {
			return fmt.Errorf("d.StateSnapshotTable.InsertState: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("d.Writer.Do: %w", err)
	}
	return
}

func (d *EventDatabase) EventNIDs(
	ctx context.Context, eventIDs []string,
) (map[string]types.EventMetadata, error) {
	return d.eventNIDs(ctx, nil, eventIDs, NoFilter)
}

type UnsentFilter bool

const (
	NoFilter         UnsentFilter = false
	FilterUnsentOnly UnsentFilter = true
)

func (d *EventDatabase) eventNIDs(
	ctx context.Context, txn *sql.Tx, eventIDs []string, filter UnsentFilter,
) (map[string]types.EventMetadata, error) {
	switch filter {
	case FilterUnsentOnly:
		return d.EventsTable.BulkSelectUnsentEventNID(ctx, txn, eventIDs)
	case NoFilter:
		return d.EventsTable.BulkSelectEventNID(ctx, txn, eventIDs)
	default:
		panic("impossible case")
	}
}

func (d *EventDatabase) SetState(
	ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.EventsTable.UpdateEventState(ctx, txn, eventNID, stateNID)
	})
}

func (d *EventDatabase) StateAtEventIDs(
	ctx context.Context, eventIDs []string,
) ([]types.StateAtEvent, error) {
	return d.EventsTable.BulkSelectStateAtEventByID(ctx, nil, eventIDs)
}

func (d *EventDatabase) SnapshotNIDFromEventID(
	ctx context.Context, eventID string,
) (types.StateSnapshotNID, error) {
	return d.snapshotNIDFromEventID(ctx, nil, eventID)
}

func (d *EventDatabase) snapshotNIDFromEventID(
	ctx context.Context, txn *sql.Tx, eventID string,
) (types.StateSnapshotNID, error) {
	_, stateNID, err := d.EventsTable.SelectEvent(ctx, txn, eventID)
	if err != nil {
		return 0, err
	}
	if stateNID == 0 {
		return 0, sql.ErrNoRows // effectively there's no state entry
	}
	return stateNID, err
}

func (d *EventDatabase) EventIDs(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID]string, error) {
	return d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
}

func (d *EventDatabase) EventsFromIDs(ctx context.Context, frameInfo *types.FrameInfo, eventIDs []string) ([]types.Event, error) {
	return d.eventsFromIDs(ctx, nil, frameInfo, eventIDs, NoFilter)
}

func (d *EventDatabase) eventsFromIDs(ctx context.Context, txn *sql.Tx, frameInfo *types.FrameInfo, eventIDs []string, filter UnsentFilter) ([]types.Event, error) {
	nidMap, err := d.eventNIDs(ctx, txn, eventIDs, filter)
	if err != nil {
		return nil, err
	}

	var nids []types.EventNID
	for _, nid := range nidMap {
		nids = append(nids, nid.EventNID)
	}

	if frameInfo == nil {
		return nil, types.ErrorInvalidFrameInfo
	}
	return d.events(ctx, txn, frameInfo.FrameVersion, nids)
}

func (d *Database) LatestEventIDs(ctx context.Context, frameNID types.FrameNID) (references []string, currentStateSnapshotNID types.StateSnapshotNID, depth int64, err error) {
	var eventNIDs []types.EventNID
	eventNIDs, currentStateSnapshotNID, err = d.FramesTable.SelectLatestEventNIDs(ctx, nil, frameNID)
	if err != nil {
		return
	}
	eventNIDMap, err := d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
	if err != nil {
		return
	}
	depth, err = d.EventsTable.SelectMaxEventDepth(ctx, nil, eventNIDs)
	if err != nil {
		return
	}
	for _, eventID := range eventNIDMap {
		references = append(references, eventID)
	}
	return
}

func (d *Database) StateBlockNIDs(
	ctx context.Context, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	return d.stateBlockNIDs(ctx, nil, stateNIDs)
}

func (d *Database) stateBlockNIDs(
	ctx context.Context, txn *sql.Tx, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	return d.StateSnapshotTable.BulkSelectStateBlockNIDs(ctx, txn, stateNIDs)
}

func (d *Database) StateEntries(
	ctx context.Context, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntryList, error) {
	return d.stateEntries(ctx, nil, stateBlockNIDs)
}

func (d *Database) stateEntries(
	ctx context.Context, txn *sql.Tx, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntryList, error) {
	entries, err := d.StateBlockTable.BulkSelectStateBlockEntries(
		ctx, txn, stateBlockNIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("d.StateBlockTable.BulkSelectStateBlockEntries: %w", err)
	}
	lists := make([]types.StateEntryList, 0, len(entries))
	for i, entry := range entries {
		eventNIDs, err := d.EventsTable.BulkSelectStateEventByNID(ctx, txn, entry, nil)
		if err != nil {
			return nil, fmt.Errorf("d.EventsTable.BulkSelectStateEventByNID: %w", err)
		}
		lists = append(lists, types.StateEntryList{
			StateBlockNID: stateBlockNIDs[i],
			StateEntries:  eventNIDs,
		})
	}
	return lists, nil
}

func (d *Database) SetFrameAlias(ctx context.Context, alias string, frameID string, creatorUserID string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FrameAliasesTable.InsertFrameAlias(ctx, txn, alias, frameID, creatorUserID)
	})
}

func (d *Database) GetFrameIDForAlias(ctx context.Context, alias string) (string, error) {
	return d.FrameAliasesTable.SelectFrameIDFromAlias(ctx, nil, alias)
}

func (d *Database) GetAliasesForFrameID(ctx context.Context, frameID string) ([]string, error) {
	return d.FrameAliasesTable.SelectAliasesFromFrameID(ctx, nil, frameID)
}

func (d *Database) GetCreatorIDForAlias(
	ctx context.Context, alias string,
) (string, error) {
	return d.FrameAliasesTable.SelectCreatorIDFromAlias(ctx, nil, alias)
}

func (d *Database) RemoveFrameAlias(ctx context.Context, alias string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FrameAliasesTable.DeleteFrameAlias(ctx, txn, alias)
	})
}

func (d *Database) GetMembership(ctx context.Context, frameNID types.FrameNID, requestSenderID spec.SenderID) (membershipEventNID types.EventNID, stillInFrame, isFrameforgotten bool, err error) {
	var requestSenderUserNID types.EventStateKeyNID
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		requestSenderUserNID, err = d.assignStateKeyNID(ctx, txn, string(requestSenderID))
		return err
	})
	if err != nil {
		return 0, false, false, fmt.Errorf("d.assignStateKeyNID: %w", err)
	}

	senderMembershipEventNID, senderMembership, isFrameforgotten, err :=
		d.MembershipTable.SelectMembershipFromFrameAndTarget(
			ctx, nil, frameNID, requestSenderUserNID,
		)
	if err == sql.ErrNoRows {
		// The user has never been a member of that frame
		return 0, false, false, nil
	} else if err != nil {
		return
	}

	return senderMembershipEventNID, senderMembership == tables.MembershipStateJoin, isFrameforgotten, nil
}

func (d *Database) GetMembershipEventNIDsForFrame(
	ctx context.Context, frameNID types.FrameNID, joinOnly bool, localOnly bool,
) ([]types.EventNID, error) {
	return d.getMembershipEventNIDsForFrame(ctx, nil, frameNID, joinOnly, localOnly)
}

func (d *Database) getMembershipEventNIDsForFrame(
	ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, joinOnly bool, localOnly bool,
) ([]types.EventNID, error) {
	if joinOnly {
		return d.MembershipTable.SelectMembershipsFromFrameAndMembership(
			ctx, txn, frameNID, tables.MembershipStateJoin, localOnly,
		)
	}

	return d.MembershipTable.SelectMembershipsFromFrame(ctx, txn, frameNID, localOnly)
}

func (d *Database) GetInvitesForUser(
	ctx context.Context,
	frameNID types.FrameNID,
	targetUserNID types.EventStateKeyNID,
) (senderUserIDs []types.EventStateKeyNID, eventIDs []string, inviteEventJSON []byte, err error) {
	return d.InvitesTable.SelectInviteActiveForUserInFrame(ctx, nil, targetUserNID, frameNID)
}

func (d *EventDatabase) Events(ctx context.Context, frameVersion xtools.FrameVersion, eventNIDs []types.EventNID) ([]types.Event, error) {
	return d.events(ctx, nil, frameVersion, eventNIDs)
}

func (d *EventDatabase) events(
	ctx context.Context, txn *sql.Tx, frameVersion xtools.FrameVersion, inputEventNIDs types.EventNIDs,
) ([]types.Event, error) {
	sort.Sort(inputEventNIDs)
	events := make(map[types.EventNID]xtools.PDU, len(inputEventNIDs))
	eventNIDs := make([]types.EventNID, 0, len(inputEventNIDs))
	for _, nid := range inputEventNIDs {
		if event, ok := d.Cache.GetDataFrameEvent(nid); ok && event != nil {
			events[nid] = event
		} else {
			eventNIDs = append(eventNIDs, nid)
		}
	}
	// If we don't need to get any events from the database, short circuit now
	if len(eventNIDs) == 0 {
		results := make([]types.Event, 0, len(inputEventNIDs))
		for _, nid := range inputEventNIDs {
			event, ok := events[nid]
			if !ok || event == nil {
				return nil, fmt.Errorf("event %d missing", nid)
			}
			results = append(results, types.Event{
				EventNID: nid,
				PDU:      event,
			})
		}
		if !redactionsArePermanent {
			d.applyRedactions(results)
		}
		return results, nil
	}
	eventJSONs, err := d.EventJSONTable.BulkSelectEventJSON(ctx, txn, eventNIDs)
	if err != nil {
		return nil, err
	}
	eventIDs, err := d.EventsTable.BulkSelectEventID(ctx, txn, eventNIDs)
	if err != nil {
		eventIDs = map[types.EventNID]string{}
	}

	verImpl, err := xtools.GetFrameVersion(frameVersion)
	if err != nil {
		return nil, err
	}

	for _, eventJSON := range eventJSONs {
		redacted := gjson.GetBytes(eventJSON.EventJSON, "unsigned.redacted_because").Exists()
		events[eventJSON.EventNID], err = verImpl.NewEventFromTrustedJSONWithEventID(
			eventIDs[eventJSON.EventNID], eventJSON.EventJSON, redacted,
		)
		if err != nil {
			return nil, err
		}
		if event := events[eventJSON.EventNID]; event != nil {
			d.Cache.StoreDataFrameEvent(eventJSON.EventNID, &types.HeaderedEvent{PDU: event})
		}
	}
	results := make([]types.Event, 0, len(inputEventNIDs))
	for _, nid := range inputEventNIDs {
		event, ok := events[nid]
		if !ok || event == nil {
			return nil, fmt.Errorf("event %d missing", nid)
		}
		results = append(results, types.Event{
			EventNID: nid,
			PDU:      event,
		})
	}
	if !redactionsArePermanent {
		d.applyRedactions(results)
	}
	return results, nil
}

func (d *Database) BulkSelectSnapshotsFromEventIDs(
	ctx context.Context, eventIDs []string,
) (map[types.StateSnapshotNID][]string, error) {
	return d.EventsTable.BulkSelectSnapshotsFromEventIDs(ctx, nil, eventIDs)
}

func (d *Database) MembershipUpdater(
	ctx context.Context, frameID, targetUserID string,
	targetLocal bool, frameVersion xtools.FrameVersion,
) (*MembershipUpdater, error) {
	txn, err := d.DB.Begin()
	if err != nil {
		return nil, err
	}
	var updater *MembershipUpdater
	_ = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		updater, err = NewMembershipUpdater(ctx, d, txn, frameID, targetUserID, targetLocal, frameVersion)
		return err
	})
	return updater, err
}

func (d *Database) GetFrameUpdater(
	ctx context.Context, frameInfo *types.FrameInfo,
) (*FrameUpdater, error) {
	if d.GetFrameUpdaterFn != nil {
		return d.GetFrameUpdaterFn(ctx, frameInfo)
	}
	txn, err := d.DB.Begin()
	if err != nil {
		return nil, err
	}
	var updater *FrameUpdater
	_ = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		updater, err = NewFrameUpdater(ctx, d, txn, frameInfo)
		return err
	})
	return updater, err
}

func (d *Database) IsEventRejected(ctx context.Context, frameNID types.FrameNID, eventID string) (bool, error) {
	return d.EventsTable.SelectEventRejected(ctx, nil, frameNID, eventID)
}

func (d *Database) AssignFrameNID(ctx context.Context, frameID spec.FrameID, frameVersion xtools.FrameVersion) (frameNID types.FrameNID, err error) {
	// This should already be checked, let's check it anyway.
	_, err = xtools.GetFrameVersion(frameVersion)
	if err != nil {
		return 0, err
	}
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameNID, err = d.assignFrameNID(ctx, txn, frameID.String(), frameVersion)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	// Not setting caches, as assignFrameNID already does this
	return frameNID, err
}

// GetOrCreateFrameInfo gets or creates a new FrameInfo, which is only safe to use with functions only needing a frameVersion or frameNID.
func (d *Database) GetOrCreateFrameInfo(ctx context.Context, event xtools.PDU) (frameInfo *types.FrameInfo, err error) {
	// Get the default frame version. If the client doesn't supply a frame_version
	// then we will use our configured default to create the frame.
	// post-coddy-client-r0-createframe
	// Note that the below logic depends on the m.frame.create event being the
	// first event that is persisted to the database when creating or joining a
	// frame.
	var frameVersion xtools.FrameVersion
	if frameVersion, err = extractFrameVersionFromCreateEvent(event); err != nil {
		return nil, fmt.Errorf("extractFrameVersionFromCreateEvent: %w", err)
	}

	frameNID, nidOK := d.Cache.GetDataFrameFrameNID(event.FrameID())
	cachedFrameVersion, versionOK := d.Cache.GetFrameVersion(event.FrameID())
	// if we found both, the frameNID and version in our cache, no need to query the database
	if nidOK && versionOK {
		return &types.FrameInfo{
			FrameNID:     frameNID,
			FrameVersion: cachedFrameVersion,
		}, nil
	}

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameNID, err = d.assignFrameNID(ctx, txn, event.FrameID(), frameVersion)
		if err != nil {
			return err
		}
		return nil
	})
	if frameVersion != "" {
		d.Cache.StoreFrameVersion(event.FrameID(), frameVersion)
	}
	return &types.FrameInfo{
		FrameVersion: frameVersion,
		FrameNID:     frameNID,
	}, err
}

func (d *Database) GetFrameVersion(ctx context.Context, frameID string) (xtools.FrameVersion, error) {
	cachedFrameVersion, versionOK := d.Cache.GetFrameVersion(frameID)
	if versionOK {
		return cachedFrameVersion, nil
	}

	frameInfo, err := d.FrameInfo(ctx, frameID)
	if err != nil {
		return "", err
	}
	if frameInfo == nil {
		return "", nil
	}
	return frameInfo.FrameVersion, nil
}

func (d *Database) GetOrCreateEventTypeNID(ctx context.Context, eventType string) (eventTypeNID types.EventTypeNID, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if eventTypeNID, err = d.assignEventTypeNID(ctx, txn, eventType); err != nil {
			return fmt.Errorf("d.assignEventTypeNID: %w", err)
		}
		return nil
	})
	return eventTypeNID, err
}

func (d *Database) GetOrCreateEventStateKeyNID(ctx context.Context, eventStateKey *string) (eventStateKeyNID types.EventStateKeyNID, err error) {
	if eventStateKey == nil {
		return 0, nil
	}

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if eventStateKeyNID, err = d.assignStateKeyNID(ctx, txn, *eventStateKey); err != nil {
			return fmt.Errorf("d.assignStateKeyNID: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return eventStateKeyNID, nil
}

func (d *EventDatabase) StoreEvent(
	ctx context.Context, event xtools.PDU,
	frameInfo *types.FrameInfo, eventTypeNID types.EventTypeNID, eventStateKeyNID types.EventStateKeyNID,
	authEventNIDs []types.EventNID, isRejected bool,
) (types.EventNID, types.StateAtEvent, error) {
	var (
		eventNID types.EventNID
		stateNID types.StateSnapshotNID
		err      error
	)

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if eventNID, stateNID, err = d.EventsTable.InsertEvent(
			ctx,
			txn,
			frameInfo.FrameNID,
			eventTypeNID,
			eventStateKeyNID,
			event.EventID(),
			authEventNIDs,
			event.Depth(),
			isRejected,
		); err != nil {
			if err == sql.ErrNoRows {
				// We've already inserted the event so select the numeric event ID
				eventNID, stateNID, err = d.EventsTable.SelectEvent(ctx, txn, event.EventID())
			} else if err != nil {
				return fmt.Errorf("d.EventsTable.InsertEvent: %w", err)
			}
			if err != nil {
				return fmt.Errorf("d.EventsTable.SelectEvent: %w", err)
			}
		}

		if err = d.EventJSONTable.InsertEventJSON(ctx, txn, eventNID, event.JSON()); err != nil {
			return fmt.Errorf("d.EventJSONTable.InsertEventJSON: %w", err)
		}

		if prevEvents := event.PrevEventIDs(); len(prevEvents) > 0 {
			// Create an updater - NB: on sqlite this WILL create a txn as we are directly calling the shared DB form of
			// GetLatestEventsForUpdate - not via the SQLiteDatabase form which has `nil` txns. This
			// function only does SELECTs though so the created txn (at this point) is just a read txn like
			// any other so this is fine. If we ever update GetLatestEventsForUpdate or NewLatestEventsUpdater
			// to do writes however then this will need to go inside `Writer.Do`.

			// The following is a copy of FrameUpdater.StorePreviousEvents
			for _, eventID := range prevEvents {
				if err = d.PrevEventsTable.InsertPreviousEvent(ctx, txn, eventID, eventNID); err != nil {
					return fmt.Errorf("u.d.PrevEventsTable.InsertPreviousEvent: %w", err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return 0, types.StateAtEvent{}, fmt.Errorf("d.Writer.Do: %w", err)
	}

	// We should attempt to update the previous events table with any
	// references that this new event makes. We do this using a latest
	// events updater because it somewhat works as a mutex, ensuring
	// that there's a row-level lock on the latest frame events (well,
	// on Postgres at least).

	return eventNID, types.StateAtEvent{
		BeforeStateSnapshotNID: stateNID,
		StateEntry: types.StateEntry{
			StateKeyTuple: types.StateKeyTuple{
				EventTypeNID:     eventTypeNID,
				EventStateKeyNID: eventStateKeyNID,
			},
			EventNID: eventNID,
		},
	}, err
}

func (d *Database) PublishFrame(ctx context.Context, frameID, appserviceID, networkID string, publish bool) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.PublishedTable.UpsertFramePublished(ctx, txn, frameID, appserviceID, networkID, publish)
	})
}

func (d *Database) GetPublishedFrame(ctx context.Context, frameID string) (bool, error) {
	return d.PublishedTable.SelectPublishedFromFrameID(ctx, nil, frameID)
}

func (d *Database) GetPublishedFrames(ctx context.Context, networkID string, includeAllNetworks bool) ([]string, error) {
	return d.PublishedTable.SelectAllPublishedFrames(ctx, nil, networkID, true, includeAllNetworks)
}

func (d *Database) MissingAuthPrevEvents(
	ctx context.Context, e xtools.PDU,
) (missingAuth, missingPrev []string, err error) {
	authEventNIDs, err := d.EventNIDs(ctx, e.AuthEventIDs())
	if err != nil {
		return nil, nil, fmt.Errorf("d.EventNIDs: %w", err)
	}
	for _, authEventID := range e.AuthEventIDs() {
		if _, ok := authEventNIDs[authEventID]; !ok {
			missingAuth = append(missingAuth, authEventID)
		}
	}

	for _, prevEventID := range e.PrevEventIDs() {
		state, err := d.StateAtEventIDs(ctx, []string{prevEventID})
		if err != nil || len(state) == 0 || (!state[0].IsCreate() && state[0].BeforeStateSnapshotNID == 0) {
			missingPrev = append(missingPrev, prevEventID)
		}
	}

	return
}

func (d *Database) assignFrameNID(
	ctx context.Context, txn *sql.Tx, frameID string, frameVersion xtools.FrameVersion,
) (types.FrameNID, error) {
	frameNID, ok := d.Cache.GetDataFrameFrameNID(frameID)
	if ok {
		return frameNID, nil
	}
	// Check if we already have a numeric ID in the database.
	frameNID, err := d.FramesTable.SelectFrameNID(ctx, txn, frameID)
	if err == sql.ErrNoRows {
		// We don't have a numeric ID so insert one into the database.
		frameNID, err = d.FramesTable.InsertFrameNID(ctx, txn, frameID, frameVersion)
		if err == sql.ErrNoRows {
			// We raced with another insert so run the select again.
			frameNID, err = d.FramesTable.SelectFrameNID(ctx, txn, frameID)
		}
	}
	if err != nil {
		return 0, err
	}
	d.Cache.StoreDataFrameFrameID(frameNID, frameID)
	d.Cache.StoreFrameVersion(frameID, frameVersion)
	return frameNID, nil
}

func (d *Database) assignEventTypeNID(
	ctx context.Context, txn *sql.Tx, eventType string,
) (types.EventTypeNID, error) {
	eventTypeNID, ok := d.Cache.GetEventTypeKey(eventType)
	if ok {
		return eventTypeNID, nil
	}
	// Check if we already have a numeric ID in the database.
	eventTypeNID, err := d.EventTypesTable.SelectEventTypeNID(ctx, txn, eventType)
	if err == sql.ErrNoRows {
		// We don't have a numeric ID so insert one into the database.
		eventTypeNID, err = d.EventTypesTable.InsertEventTypeNID(ctx, txn, eventType)
		if err == sql.ErrNoRows {
			// We raced with another insert so run the select again.
			eventTypeNID, err = d.EventTypesTable.SelectEventTypeNID(ctx, txn, eventType)
		}
	}
	if err != nil {
		return 0, err
	}
	d.Cache.StoreEventTypeKey(eventTypeNID, eventType)
	return eventTypeNID, nil
}

func (d *EventDatabase) assignStateKeyNID(
	ctx context.Context, txn *sql.Tx, eventStateKey string,
) (types.EventStateKeyNID, error) {
	eventStateKeyNID, ok := d.Cache.GetEventStateKeyNID(eventStateKey)
	if ok {
		return eventStateKeyNID, nil
	}
	// Check if we already have a numeric ID in the database.
	eventStateKeyNID, err := d.EventStateKeysTable.SelectEventStateKeyNID(ctx, txn, eventStateKey)
	if err == sql.ErrNoRows {
		// We don't have a numeric ID so insert one into the database.
		eventStateKeyNID, err = d.EventStateKeysTable.InsertEventStateKeyNID(ctx, txn, eventStateKey)
		if err == sql.ErrNoRows {
			// We raced with another insert so run the select again.
			eventStateKeyNID, err = d.EventStateKeysTable.SelectEventStateKeyNID(ctx, txn, eventStateKey)
		}
	}
	d.Cache.StoreEventStateKey(eventStateKeyNID, eventStateKey)
	return eventStateKeyNID, err
}

func extractFrameVersionFromCreateEvent(event xtools.PDU) (
	xtools.FrameVersion, error,
) {
	var err error
	var frameVersion xtools.FrameVersion
	// Look for m.frame.create events.
	if event.Type() != spec.MFrameCreate {
		return xtools.FrameVersion(""), nil
	}
	frameVersion = xtools.FrameVersionV1
	var createContent xtools.CreateContent
	// The m.frame.create event contains an optional "frame_version" key in
	// the event content, so we need to unmarshal that first.
	if err = json.Unmarshal(event.Content(), &createContent); err != nil {
		return xtools.FrameVersion(""), err
	}
	// A frame version was specified in the event content?
	if createContent.FrameVersion != nil {
		frameVersion = xtools.FrameVersion(*createContent.FrameVersion)
	}
	return frameVersion, err
}

// nolint:gocyclo
// MaybeRedactEvent manages the redacted status of events. There's two cases to consider in order to comply with the spec:
// "servers should not apply or send redactions to clients until both the redaction event and original event have been seen, and are valid."
// These cases are:
//   - This is a redaction event, redact the event it references if we know about it.
//   - This is a normal event which may have been previously redacted.
//
// In the first case, check if we have the referenced event then apply the redaction, else store it
// in the redactions table with validated=FALSE. In the second case, check if there is a redaction for it:
// if there is then apply the redactions and set validated=TRUE.
//
// When an event is redacted, the redacted event JSON is modified to add an `unsigned.redacted_because` field. We use this field
// when loading events to determine whether to apply redactions. This keeps the hot-path of reading events quick as we don't need
// to cross-reference with other tables when loading.
//
// Returns the redaction event and the redacted event if this call resulted in a redaction.
func (d *EventDatabase) MaybeRedactEvent(
	ctx context.Context, frameInfo *types.FrameInfo, eventNID types.EventNID, event xtools.PDU, plResolver state.PowerLevelResolver,
	querier api.QuerySenderIDAPI,
) (xtools.PDU, xtools.PDU, error) {
	var (
		redactionEvent, redactedEvent *types.Event
		err                           error
		validated                     bool
		ignoreRedaction               bool
	)

	wErr := d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		isRedactionEvent := event.Type() == spec.MFrameRedaction && event.StateKey() == nil
		if isRedactionEvent {
			// an event which redacts itself should be ignored
			if event.EventID() == event.Redacts() {
				return nil
			}

			err = d.RedactionsTable.InsertRedaction(ctx, txn, tables.RedactionInfo{
				Validated:        false,
				RedactionEventID: event.EventID(),
				RedactsEventID:   event.Redacts(),
			})
			if err != nil {
				return fmt.Errorf("d.RedactionsTable.InsertRedaction: %w", err)
			}
		}

		redactionEvent, redactedEvent, validated, err = d.loadRedactionPair(ctx, txn, frameInfo, eventNID, event)
		switch {
		case err != nil:
			return fmt.Errorf("d.loadRedactionPair: %w", err)
		case validated || redactedEvent == nil || redactionEvent == nil:
			// we've seen this redaction before or there is nothing to redact
			return nil
		case redactedEvent.FrameID() != redactionEvent.FrameID():
			// redactions across frames aren't allowed
			ignoreRedaction = true
			return nil
		}

		var validFrameID *spec.FrameID
		validFrameID, err = spec.NewFrameID(redactedEvent.FrameID())
		if err != nil {
			return err
		}
		sender1Domain := ""
		sender1, err1 := querier.QueryUserIDForSender(ctx, *validFrameID, redactedEvent.SenderID())
		if err1 == nil {
			sender1Domain = string(sender1.Domain())
		}
		sender2Domain := ""
		sender2, err2 := querier.QueryUserIDForSender(ctx, *validFrameID, redactionEvent.SenderID())
		if err2 == nil {
			sender2Domain = string(sender2.Domain())
		}
		var powerlevels *xtools.PowerLevelContent
		powerlevels, err = plResolver.Resolve(ctx, redactionEvent.EventID())
		if err != nil {
			return err
		}

		switch {
		case powerlevels.UserLevel(redactionEvent.SenderID()) >= powerlevels.Redact:
			// 1. The power level of the redaction event’s sender is greater than or equal to the redact level.
		case sender1Domain != "" && sender2Domain != "" && sender1Domain == sender2Domain:
			// 2. The domain of the redaction event’s sender matches that of the original event’s sender.
		default:
			ignoreRedaction = true
			return nil
		}

		// mark the event as redacted
		if redactionsArePermanent {
			redactedEvent.Redact()
		}

		err = redactedEvent.SetUnsignedField("redacted_because", redactionEvent)
		if err != nil {
			return fmt.Errorf("redactedEvent.SetUnsignedField: %w", err)
		}
		// NOTSPEC: sytest relies on this unspecced field existing :(
		err = redactedEvent.SetUnsignedField("redacted_by", redactionEvent.EventID())
		if err != nil {
			return fmt.Errorf("redactedEvent.SetUnsignedField: %w", err)
		}
		// overwrite the eventJSON table
		err = d.EventJSONTable.InsertEventJSON(ctx, txn, redactedEvent.EventNID, redactedEvent.JSON())
		if err != nil {
			return fmt.Errorf("d.EventJSONTable.InsertEventJSON: %w", err)
		}

		err = d.RedactionsTable.MarkRedactionValidated(ctx, txn, redactionEvent.EventID(), true)
		if err != nil {
			return fmt.Errorf("d.RedactionsTable.MarkRedactionValidated: %w", err)
		}

		// We remove the entry from the cache, as if we just "StoreDataFrameEvent", we can't be
		// certain that the cached entry actually is updated, since ristretto is eventual-persistent.
		d.Cache.InvalidateDataFrameEvent(redactedEvent.EventNID)

		return nil
	})
	if wErr != nil {
		return nil, nil, err
	}
	if ignoreRedaction || redactionEvent == nil || redactedEvent == nil {
		return nil, nil, nil
	}
	return redactionEvent.PDU, redactedEvent.PDU, nil
}

// loadRedactionPair returns both the redaction event and the redacted event, else nil.
func (d *EventDatabase) loadRedactionPair(
	ctx context.Context, txn *sql.Tx, frameInfo *types.FrameInfo, eventNID types.EventNID, event xtools.PDU,
) (*types.Event, *types.Event, bool, error) {
	var redactionEvent, redactedEvent *types.Event
	var info *tables.RedactionInfo
	var err error
	isRedactionEvent := event.Type() == spec.MFrameRedaction && event.StateKey() == nil

	var eventBeingRedacted string
	if isRedactionEvent {
		eventBeingRedacted = event.Redacts()
		redactionEvent = &types.Event{
			EventNID: eventNID,
			PDU:      event,
		}
	} else {
		eventBeingRedacted = event.EventID() // maybe, we'll see if we have info
		redactedEvent = &types.Event{
			EventNID: eventNID,
			PDU:      event,
		}
	}

	info, err = d.RedactionsTable.SelectRedactionInfoByEventBeingRedacted(ctx, txn, eventBeingRedacted)
	if err != nil {
		return nil, nil, false, err
	}
	if info == nil {
		// this event hasn't been redacted or we don't have the redaction for it yet
		return nil, nil, false, nil
	}

	if isRedactionEvent {
		redactedEvent = d.loadEvent(ctx, frameInfo, info.RedactsEventID)
	} else {
		redactionEvent = d.loadEvent(ctx, frameInfo, info.RedactionEventID)
	}

	return redactionEvent, redactedEvent, info.Validated, nil
}

// applyRedactions will redact events that have an `unsigned.redacted_because` field.
func (d *EventDatabase) applyRedactions(events []types.Event) {
	for i := range events {
		if result := gjson.GetBytes(events[i].Unsigned(), "redacted_because"); result.Exists() {
			events[i].Redact()
		}
	}
}

// loadEvent loads a single event or returns nil on any problems/missing event
func (d *EventDatabase) loadEvent(ctx context.Context, frameInfo *types.FrameInfo, eventID string) *types.Event {
	nids, err := d.EventNIDs(ctx, []string{eventID})
	if err != nil {
		return nil
	}
	if len(nids) == 0 {
		return nil
	}
	if frameInfo == nil {
		return nil
	}
	evs, err := d.Events(ctx, frameInfo.FrameVersion, []types.EventNID{nids[eventID].EventNID})
	if err != nil {
		return nil
	}
	if len(evs) != 1 {
		return nil
	}
	return &evs[0]
}

func (d *Database) GetHistoryVisibilityState(ctx context.Context, frameInfo *types.FrameInfo, eventID string, domain string) ([]xtools.PDU, error) {
	eventStates, err := d.EventsTable.BulkSelectStateAtEventByID(ctx, nil, []string{eventID})
	if err != nil {
		return nil, err
	}
	stateSnapshotNID := eventStates[0].BeforeStateSnapshotNID
	if stateSnapshotNID == 0 {
		return nil, nil
	}
	eventNIDs, err := d.StateSnapshotTable.BulkSelectStateForHistoryVisibility(ctx, nil, stateSnapshotNID, domain)
	if err != nil {
		return nil, err
	}
	eventIDs, _ := d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
	if err != nil {
		eventIDs = map[types.EventNID]string{}
	}
	verImpl, err := xtools.GetFrameVersion(frameInfo.FrameVersion)
	if err != nil {
		return nil, err
	}
	events := make([]xtools.PDU, 0, len(eventNIDs))
	for _, eventNID := range eventNIDs {
		data, err := d.EventJSONTable.BulkSelectEventJSON(ctx, nil, []types.EventNID{eventNID})
		if err != nil {
			return nil, err
		}
		ev, err := verImpl.NewEventFromTrustedJSONWithEventID(eventIDs[eventNID], data[0].EventJSON, false)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, nil
}

// GetStateEvent returns the current state event of a given type for a given frame with a given state key
// If no event could be found, returns nil
// If there was an issue during the retrieval, returns an error
func (d *Database) GetStateEvent(ctx context.Context, frameID, evType, stateKey string) (*types.HeaderedEvent, error) {
	frameInfo, err := d.frameInfo(ctx, nil, frameID)
	if err != nil {
		return nil, err
	}
	if frameInfo == nil {
		return nil, fmt.Errorf("frame %s doesn't exist", frameID)
	}
	// e.g invited frames
	if frameInfo.IsStub() {
		return nil, nil
	}
	eventTypeNID, err := d.GetOrCreateEventTypeNID(ctx, evType)
	if err == sql.ErrNoRows {
		// No frames have an event of this type, otherwise we'd have an event type NID
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stateKeyNID, err := d.GetOrCreateEventStateKeyNID(ctx, &stateKey)
	if err == sql.ErrNoRows {
		// No frames have a state event with this state key, otherwise we'd have an state key NID
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := d.loadStateAtSnapshot(ctx, frameInfo.StateSnapshotNID())
	if err != nil {
		return nil, err
	}
	var eventNIDs []types.EventNID
	for _, e := range entries {
		if e.EventTypeNID == eventTypeNID && e.EventStateKeyNID == stateKeyNID {
			eventNIDs = append(eventNIDs, e.EventNID)
		}
	}
	verImpl, err := xtools.GetFrameVersion(frameInfo.FrameVersion)
	if err != nil {
		return nil, err
	}
	eventIDs, _ := d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
	if err != nil {
		eventIDs = map[types.EventNID]string{}
	}
	// return the event requested
	for _, e := range entries {
		if e.EventTypeNID == eventTypeNID && e.EventStateKeyNID == stateKeyNID {
			cachedEvent, ok := d.Cache.GetDataFrameEvent(e.EventNID)
			if ok {
				return &types.HeaderedEvent{PDU: cachedEvent}, nil
			}
			data, err := d.EventJSONTable.BulkSelectEventJSON(ctx, nil, []types.EventNID{e.EventNID})
			if err != nil {
				return nil, err
			}
			if len(data) == 0 {
				return nil, fmt.Errorf("GetStateEvent: no json for event nid %d", e.EventNID)
			}
			ev, err := verImpl.NewEventFromTrustedJSONWithEventID(eventIDs[e.EventNID], data[0].EventJSON, false)
			if err != nil {
				return nil, err
			}
			return &types.HeaderedEvent{PDU: ev}, nil
		}
	}

	return nil, nil
}

// Same as GetStateEvent but returns all matching state events with this event type. Returns no error
// if there are no events with this event type.
func (d *Database) GetStateEventsWithEventType(ctx context.Context, frameID, evType string) ([]*types.HeaderedEvent, error) {
	frameInfo, err := d.frameInfo(ctx, nil, frameID)
	if err != nil {
		return nil, err
	}
	if frameInfo == nil {
		return nil, fmt.Errorf("frame %s doesn't exist", frameID)
	}
	// e.g invited frames
	if frameInfo.IsStub() {
		return nil, nil
	}
	eventTypeNID, err := d.EventTypesTable.SelectEventTypeNID(ctx, nil, evType)
	if err == sql.ErrNoRows {
		// No frames have an event of this type, otherwise we'd have an event type NID
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := d.loadStateAtSnapshot(ctx, frameInfo.StateSnapshotNID())
	if err != nil {
		return nil, err
	}
	var eventNIDs []types.EventNID
	for _, e := range entries {
		if e.EventTypeNID == eventTypeNID {
			eventNIDs = append(eventNIDs, e.EventNID)
		}
	}
	eventIDs, _ := d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
	if err != nil {
		eventIDs = map[types.EventNID]string{}
	}
	// return the events requested
	eventPairs, err := d.EventJSONTable.BulkSelectEventJSON(ctx, nil, eventNIDs)
	if err != nil {
		return nil, err
	}
	if len(eventPairs) == 0 {
		return nil, nil
	}
	verImpl, err := xtools.GetFrameVersion(frameInfo.FrameVersion)
	if err != nil {
		return nil, err
	}
	var result []*types.HeaderedEvent
	for _, pair := range eventPairs {
		ev, err := verImpl.NewEventFromTrustedJSONWithEventID(eventIDs[pair.EventNID], pair.EventJSON, false)
		if err != nil {
			return nil, err
		}
		result = append(result, &types.HeaderedEvent{PDU: ev})
	}

	return result, nil
}

// GetFramesByMembership returns a list of frame IDs matching the provided membership and user ID (as state_key).
func (d *Database) GetFramesByMembership(ctx context.Context, userID spec.UserID, membership string) ([]string, error) {
	var membershipState tables.MembershipState
	switch membership {
	case "join":
		membershipState = tables.MembershipStateJoin
	case "invite":
		membershipState = tables.MembershipStateInvite
	case "leave":
		membershipState = tables.MembershipStateLeaveOrBan
	case "ban":
		membershipState = tables.MembershipStateLeaveOrBan
	default:
		return nil, fmt.Errorf("GetFramesByMembership: invalid membership %s", membership)
	}

	// Convert provided user ID to NID
	userNID, err := d.EventStateKeysTable.SelectEventStateKeyNID(ctx, nil, userID.String())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, fmt.Errorf("SelectEventStateKeyNID: cannot map user ID to state key NIDs: %w", err)
		}
	}

	// Use this NID to fetch all associated frame keys (for pseudo ID frames)
	frameKeyMap, err := d.UserFrameKeyTable.SelectAllPublicKeysForUser(ctx, nil, userNID)
	if err != nil {
		if err == sql.ErrNoRows {
			frameKeyMap = map[types.FrameNID]ed25519.PublicKey{}
		} else {
			return nil, fmt.Errorf("SelectAllPublicKeysForUser: could not select user frame public keys for user: %w", err)
		}
	}

	var eventStateKeyNIDs []types.EventStateKeyNID

	// If there are frame keys (i.e. this user is in pseudo ID frames), then gather the appropriate NIDs
	if len(frameKeyMap) != 0 {
		// Convert keys to string representation
		userFrameKeys := make([]string, len(frameKeyMap))
		i := 0
		for _, key := range frameKeyMap {
			userFrameKeys[i] = spec.Base64Bytes(key).Encode()
			i += 1
		}

		// Convert the string representation to its NID
		pseudoIDStateKeys, sqlErr := d.EventStateKeysTable.BulkSelectEventStateKeyNID(ctx, nil, userFrameKeys)
		if sqlErr != nil {
			if sqlErr == sql.ErrNoRows {
				pseudoIDStateKeys = map[string]types.EventStateKeyNID{}
			} else {
				return nil, fmt.Errorf("BulkSelectEventStateKeyNID: could not select state keys for public frame keys: %w", err)
			}
		}

		// Collect all NIDs together
		eventStateKeyNIDs = make([]types.EventStateKeyNID, len(pseudoIDStateKeys)+1)
		eventStateKeyNIDs[0] = userNID
		i = 1
		for _, nid := range pseudoIDStateKeys {
			eventStateKeyNIDs[i] = nid
			i += 1
		}
	} else {
		// If there are no frame keys (so no pseudo ID frames), we only need to care about the user ID NID.
		eventStateKeyNIDs = []types.EventStateKeyNID{userNID}
	}

	// Fetch frames that match membership for each NID
	frameNIDs := []types.FrameNID{}
	for _, nid := range eventStateKeyNIDs {
		var frameNIDsChunk []types.FrameNID
		frameNIDsChunk, err = d.MembershipTable.SelectFramesWithMembership(ctx, nil, nid, membershipState)
		if err != nil {
			return nil, fmt.Errorf("GetFramesByMembership: failed to SelectFramesWithMembership: %w", err)
		}
		frameNIDs = append(frameNIDs, frameNIDsChunk...)
	}

	frameIDs, err := d.FramesTable.BulkSelectFrameIDs(ctx, nil, frameNIDs)
	if err != nil {
		return nil, fmt.Errorf("GetFramesByMembership: failed to lookup frame nids: %w", err)
	}
	if len(frameIDs) != len(frameNIDs) {
		return nil, fmt.Errorf("GetFramesByMembership: missing frame IDs, got %d want %d", len(frameIDs), len(frameNIDs))
	}
	return frameIDs, nil
}

// GetBulkStateContent returns all state events which match a given frame ID and a given state key tuple. Both must be satisfied for a match.
// If a tuple has the StateKey of '*' and allowWildcards=true then all state events with the EventType should be returned.
func (d *Database) GetBulkStateContent(ctx context.Context, frameIDs []string, tuples []xtools.StateKeyTuple, allowWildcards bool) ([]tables.StrippedEvent, error) {
	eventTypes := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		eventTypes = append(eventTypes, tuple.EventType)
	}
	// we don't bother failing the request if we get asked for event types we don't know about, as all that would result in is no matches which
	// isn't a failure.
	eventTypeNIDMap, err := d.eventTypeNIDs(ctx, nil, eventTypes)
	if err != nil {
		return nil, fmt.Errorf("GetBulkStateContent: failed to map event type nids: %w", err)
	}
	typeNIDSet := make(map[types.EventTypeNID]bool)
	for _, nid := range eventTypeNIDMap {
		typeNIDSet[nid] = true
	}

	allowWildcard := make(map[types.EventTypeNID]bool)
	eventStateKeys := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		if allowWildcards && tuple.StateKey == "*" {
			allowWildcard[eventTypeNIDMap[tuple.EventType]] = true
			continue
		}
		eventStateKeys = append(eventStateKeys, tuple.StateKey)

	}

	eventStateKeyNIDMap, err := d.eventStateKeyNIDs(ctx, nil, eventStateKeys)
	if err != nil {
		return nil, fmt.Errorf("GetBulkStateContent: failed to map state key nids: %w", err)
	}
	stateKeyNIDSet := make(map[types.EventStateKeyNID]bool)
	for _, nid := range eventStateKeyNIDMap {
		stateKeyNIDSet[nid] = true
	}

	var eventNIDs []types.EventNID
	eventNIDToVer := make(map[types.EventNID]xtools.FrameVersion)
	// TDO: This feels like this is going to be really slow...
	for _, frameID := range frameIDs {
		frameInfo, err2 := d.frameInfo(ctx, nil, frameID)
		if err2 != nil {
			return nil, fmt.Errorf("GetBulkStateContent: failed to load frame info for frame %s : %w", frameID, err2)
		}
		// for unknown frames or frames which we don't have the current state, skip them.
		if frameInfo == nil || frameInfo.IsStub() {
			continue
		}
		entries, err2 := d.loadStateAtSnapshot(ctx, frameInfo.StateSnapshotNID())
		if err2 != nil {
			return nil, fmt.Errorf("GetBulkStateContent: failed to load state for frame %s : %w", frameID, err2)
		}
		for _, entry := range entries {
			if typeNIDSet[entry.EventTypeNID] {
				if allowWildcard[entry.EventTypeNID] || stateKeyNIDSet[entry.EventStateKeyNID] {
					eventNIDs = append(eventNIDs, entry.EventNID)
					eventNIDToVer[entry.EventNID] = frameInfo.FrameVersion
				}
			}
		}
	}
	eventIDs, _ := d.EventsTable.BulkSelectEventID(ctx, nil, eventNIDs)
	if err != nil {
		eventIDs = map[types.EventNID]string{}
	}
	events, err := d.EventJSONTable.BulkSelectEventJSON(ctx, nil, eventNIDs)
	if err != nil {
		return nil, fmt.Errorf("GetBulkStateContent: failed to load event JSON for event nids: %w", err)
	}
	result := make([]tables.StrippedEvent, len(events))
	for i := range events {
		frameVer := eventNIDToVer[events[i].EventNID]
		verImpl, err := xtools.GetFrameVersion(frameVer)
		if err != nil {
			return nil, err
		}
		ev, err := verImpl.NewEventFromTrustedJSONWithEventID(eventIDs[events[i].EventNID], events[i].EventJSON, false)
		if err != nil {
			return nil, fmt.Errorf("GetBulkStateContent: failed to load event JSON for event NID %v : %w", events[i].EventNID, err)
		}
		result[i] = tables.StrippedEvent{
			EventType:    ev.Type(),
			FrameID:       ev.FrameID(),
			StateKey:     *ev.StateKey(),
			ContentValue: tables.ExtractContentValue(&types.HeaderedEvent{PDU: ev}),
		}
	}

	return result, nil
}

// JoinedUsersSetInFrames returns a map of how many times the given users appear in the specified frames.
func (d *Database) JoinedUsersSetInFrames(ctx context.Context, frameIDs, userIDs []string, localOnly bool) (map[string]int, error) {
	frameNIDs, err := d.FramesTable.BulkSelectFrameNIDs(ctx, nil, frameIDs)
	if err != nil {
		return nil, err
	}
	userNIDsMap, err := d.eventStateKeyNIDs(ctx, nil, userIDs)
	if err != nil {
		return nil, err
	}
	userNIDs := make([]types.EventStateKeyNID, 0, len(userNIDsMap))
	nidToUserID := make(map[types.EventStateKeyNID]string, len(userNIDsMap))
	for id, nid := range userNIDsMap {
		userNIDs = append(userNIDs, nid)
		nidToUserID[nid] = id
	}
	userNIDToCount, err := d.MembershipTable.SelectJoinedUsersSetForFrames(ctx, nil, frameNIDs, userNIDs, localOnly)
	if err != nil {
		return nil, err
	}
	stateKeyNIDs := make([]types.EventStateKeyNID, len(userNIDToCount))
	i := 0
	for nid := range userNIDToCount {
		stateKeyNIDs[i] = nid
		i++
	}
	// If we didn't have any userIDs to look up, get the UserIDs for the returned userNIDToCount now
	if len(userIDs) == 0 {
		nidToUserID, err = d.EventStateKeys(ctx, stateKeyNIDs)
		if err != nil {
			return nil, err
		}
	}
	result := make(map[string]int, len(userNIDToCount))
	for nid, count := range userNIDToCount {
		result[nidToUserID[nid]] = count
	}
	return result, nil
}

// GetLeftUsers calculates users we (the server) don't share a frame with anymore.
func (d *Database) GetLeftUsers(ctx context.Context, userIDs []string) ([]string, error) {
	// Get the userNID for all users with a stale device list
	stateKeyNIDMap, err := d.EventStateKeyNIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	userNIDs := make([]types.EventStateKeyNID, 0, len(stateKeyNIDMap))
	userNIDtoUserID := make(map[types.EventStateKeyNID]string, len(stateKeyNIDMap))
	// Create a map from userNID -> userID
	for userID, nid := range stateKeyNIDMap {
		userNIDs = append(userNIDs, nid)
		userNIDtoUserID[nid] = userID
	}

	// Get all users whose membership is still join, knock or invite.
	stillJoinedUsersNIDs, err := d.MembershipTable.SelectJoinedUsers(ctx, nil, userNIDs)
	if err != nil {
		return nil, err
	}

	// Remove joined users from the "user with stale devices" list, which contains left AND joined users
	for _, joinedUser := range stillJoinedUsersNIDs {
		delete(userNIDtoUserID, joinedUser)
	}

	// The users still in our userNIDtoUserID map are the users we don't share a frame with anymore,
	// and the return value we are looking for.
	leftUsers := make([]string, 0, len(userNIDtoUserID))
	for _, userID := range userNIDtoUserID {
		leftUsers = append(leftUsers, userID)
	}

	return leftUsers, nil
}

// GetLocalServerInFrame returns true if we think we're in a given frame or false otherwise.
func (d *Database) GetLocalServerInFrame(ctx context.Context, frameNID types.FrameNID) (bool, error) {
	return d.MembershipTable.SelectLocalServerInFrame(ctx, nil, frameNID)
}

// GetServerInFrame returns true if we think a server is in a given frame or false otherwise.
func (d *Database) GetServerInFrame(ctx context.Context, frameNID types.FrameNID, serverName spec.ServerName) (bool, error) {
	return d.MembershipTable.SelectServerInFrame(ctx, nil, frameNID, serverName)
}

// GetKnownUsers searches all users that userID knows about.
func (d *Database) GetKnownUsers(ctx context.Context, userID, searchString string, limit int) ([]string, error) {
	stateKeyNID, err := d.EventStateKeysTable.SelectEventStateKeyNID(ctx, nil, userID)
	if err != nil {
		return nil, err
	}
	return d.MembershipTable.SelectKnownUsers(ctx, nil, stateKeyNID, searchString, limit)
}

// GetKnownFrames returns a list of all frames we know about.
func (d *Database) GetKnownFrames(ctx context.Context) ([]string, error) {
	return d.FramesTable.SelectFrameIDsWithEvents(ctx, nil)
}

// ForgetFrame sets a users frame to forgotten
func (d *Database) ForgetFrame(ctx context.Context, userID, frameID string, forget bool) error {
	frameNIDs, err := d.FramesTable.BulkSelectFrameNIDs(ctx, nil, []string{frameID})
	if err != nil {
		return err
	}
	if len(frameNIDs) > 1 {
		return fmt.Errorf("expected one frame, got %d", len(frameNIDs))
	}
	stateKeyNID, err := d.EventStateKeysTable.SelectEventStateKeyNID(ctx, nil, userID)
	if err != nil {
		return err
	}

	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.MembershipTable.UpdateForgetMembership(ctx, nil, frameNIDs[0], stateKeyNID, forget)
	})
}

// PurgeFrame removes all information about a given frame from the dataframe.
// For large frames this operation may take a considerable amount of time.
func (d *Database) PurgeFrame(ctx context.Context, frameID string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameNID, err := d.FramesTable.SelectFrameNIDForUpdate(ctx, txn, frameID)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("frame %s does not exist", frameID)
			}
			return fmt.Errorf("failed to lock the frame: %w", err)
		}
		return d.Purge.PurgeFrame(ctx, txn, frameNID, frameID)
	})
}

func (d *Database) UpgradeFrame(ctx context.Context, oldFrameID, newFrameID, eventSender string) error {

	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		published, err := d.PublishedTable.SelectPublishedFromFrameID(ctx, txn, oldFrameID)
		if err != nil {
			return fmt.Errorf("failed to get published frame: %w", err)
		}
		if published {
			// un-publish old frame
			if err = d.PublishedTable.UpsertFramePublished(ctx, txn, oldFrameID, "", "", false); err != nil {
				return fmt.Errorf("failed to unpublish frame: %w", err)
			}
			// publish new frame
			if err = d.PublishedTable.UpsertFramePublished(ctx, txn, newFrameID, "", "", true); err != nil {
				return fmt.Errorf("failed to publish frame: %w", err)
			}
		}

		// Migrate any existing frame aliases
		aliases, err := d.FrameAliasesTable.SelectAliasesFromFrameID(ctx, txn, oldFrameID)
		if err != nil {
			return fmt.Errorf("failed to get frame aliases: %w", err)
		}

		for _, alias := range aliases {
			if err = d.FrameAliasesTable.DeleteFrameAlias(ctx, txn, alias); err != nil {
				return fmt.Errorf("failed to remove frame alias: %w", err)
			}
			if err = d.FrameAliasesTable.InsertFrameAlias(ctx, txn, alias, newFrameID, eventSender); err != nil {
				return fmt.Errorf("failed to set frame alias: %w", err)
			}
		}
		return nil
	})
}

// InsertUserFramePrivatePublicKey inserts a new user frame key for the given user and frame.
// Returns the newly inserted private key or an existing private key. If there is
// an error talking to the database, returns that error.
func (d *Database) InsertUserFramePrivatePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID, key ed25519.PrivateKey) (result ed25519.PrivateKey, err error) {
	uID := userID.String()
	stateKeyNIDMap, sErr := d.eventStateKeyNIDs(ctx, nil, []string{uID})
	if sErr != nil {
		return nil, sErr
	}
	stateKeyNID := stateKeyNIDMap[uID]

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameInfo, rErr := d.frameInfo(ctx, txn, frameID.String())
		if rErr != nil {
			return rErr
		}
		if frameInfo == nil {
			return eventutil.ErrFrameNoExists{}
		}

		var iErr error
		result, iErr = d.UserFrameKeyTable.InsertUserFramePrivatePublicKey(ctx, txn, stateKeyNID, frameInfo.FrameNID, key)
		return iErr
	})
	return result, err
}

// InsertUserFramePublicKey inserts a new user frame key for the given user and frame.
// Returns the newly inserted public key or an existing public key. If there is
// an error talking to the database, returns that error.
func (d *Database) InsertUserFramePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID, key ed25519.PublicKey) (result ed25519.PublicKey, err error) {
	uID := userID.String()
	stateKeyNIDMap, sErr := d.eventStateKeyNIDs(ctx, nil, []string{uID})
	if sErr != nil {
		return nil, sErr
	}
	stateKeyNID := stateKeyNIDMap[uID]

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameInfo, rErr := d.frameInfo(ctx, txn, frameID.String())
		if rErr != nil {
			return rErr
		}
		if frameInfo == nil {
			return eventutil.ErrFrameNoExists{}
		}

		var iErr error
		result, iErr = d.UserFrameKeyTable.InsertUserFramePublicKey(ctx, txn, stateKeyNID, frameInfo.FrameNID, key)
		return iErr
	})
	return result, err
}

// SelectUserFramePrivateKey queries the users frame private key.
// If no key exists, returns no key and no error. Otherwise returns
// the key and a database error, if any.
// TDO: Cache this?
func (d *Database) SelectUserFramePrivateKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (key ed25519.PrivateKey, err error) {
	uID := userID.String()
	stateKeyNIDMap, sErr := d.eventStateKeyNIDs(ctx, nil, []string{uID})
	if sErr != nil {
		return nil, sErr
	}
	stateKeyNID := stateKeyNIDMap[uID]

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameInfo, rErr := d.frameInfo(ctx, txn, frameID.String())
		if rErr != nil {
			return rErr
		}
		if frameInfo == nil {
			return eventutil.ErrFrameNoExists{}
		}

		key, sErr = d.UserFrameKeyTable.SelectUserFramePrivateKey(ctx, txn, stateKeyNID, frameInfo.FrameNID)
		if !errors.Is(sErr, sql.ErrNoRows) {
			return sErr
		}
		return nil
	})
	return
}

// SelectUserFramePublicKey queries the users frame public key.
// If no key exists, returns no key and no error. Otherwise returns
// the key and a database error, if any.
func (d *Database) SelectUserFramePublicKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (key ed25519.PublicKey, err error) {
	uID := userID.String()
	stateKeyNIDMap, sErr := d.eventStateKeyNIDs(ctx, nil, []string{uID})
	if sErr != nil {
		return nil, sErr
	}
	stateKeyNID := stateKeyNIDMap[uID]

	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		frameInfo, rErr := d.frameInfo(ctx, txn, frameID.String())
		if rErr != nil {
			return rErr
		}
		if frameInfo == nil {
			return nil
		}

		key, sErr = d.UserFrameKeyTable.SelectUserFramePublicKey(ctx, txn, stateKeyNID, frameInfo.FrameNID)
		if !errors.Is(sErr, sql.ErrNoRows) {
			return sErr
		}
		return nil
	})
	return
}

// SelectUserIDsForPublicKeys returns a map from frameID -> map from senderKey -> userID
func (d *Database) SelectUserIDsForPublicKeys(ctx context.Context, publicKeys map[spec.FrameID][]ed25519.PublicKey) (result map[spec.FrameID]map[string]string, err error) {
	result = make(map[spec.FrameID]map[string]string, len(publicKeys))

	// map all frameIDs to frameNIDs
	query := make(map[types.FrameNID][]ed25519.PublicKey)
	frames := make(map[types.FrameNID]spec.FrameID)
	for frameID, keys := range publicKeys {
		frameNID, ok := d.Cache.GetDataFrameFrameNID(frameID.String())
		if !ok {
			frameInfo, rErr := d.frameInfo(ctx, nil, frameID.String())
			if rErr != nil {
				return nil, rErr
			}
			if frameInfo == nil {
				logrus.Warnf("missing frame info for %s, there will be missing users in the response", frameID.String())
				continue
			}
			frameNID = frameInfo.FrameNID
		}

		query[frameNID] = keys
		frames[frameNID] = frameID
	}

	// get the user frame key pars
	userFrameKeyPairMap, sErr := d.UserFrameKeyTable.BulkSelectUserNIDs(ctx, nil, query)
	if sErr != nil {
		return nil, sErr
	}
	nids := make([]types.EventStateKeyNID, 0, len(userFrameKeyPairMap))
	for _, nid := range userFrameKeyPairMap {
		nids = append(nids, nid.EventStateKeyNID)
	}
	// get the userIDs
	nidMap, seErr := d.EventStateKeys(ctx, nids)
	if seErr != nil {
		return nil, seErr
	}

	// build the result map (frameID -> map publicKey -> userID)
	for publicKey, userFrameKeyPair := range userFrameKeyPairMap {
		userID := nidMap[userFrameKeyPair.EventStateKeyNID]
		frameID := frames[userFrameKeyPair.FrameNID]
		resMap, exists := result[frameID]
		if !exists {
			resMap = map[string]string{}
		}
		resMap[publicKey] = userID
		result[frameID] = resMap
	}
	return result, err
}

// FXME TDO: Remove all this - horrible dupe with dataframe/state. Can't use the original impl because of circular loops
// it should live in this package!

func (d *Database) loadStateAtSnapshot(
	ctx context.Context, stateNID types.StateSnapshotNID,
) ([]types.StateEntry, error) {
	stateBlockNIDLists, err := d.StateBlockNIDs(ctx, []types.StateSnapshotNID{stateNID})
	if err != nil {
		return nil, err
	}
	// We've asked for exactly one snapshot from the db so we should have exactly one entry in the result.
	stateBlockNIDList := stateBlockNIDLists[0]

	stateEntryLists, err := d.StateEntries(ctx, stateBlockNIDList.StateBlockNIDs)
	if err != nil {
		return nil, err
	}
	stateEntriesMap := stateEntryListMap(stateEntryLists)

	// Combine all the state entries for this snapshot.
	// The order of state block NIDs in the list tells us the order to combine them in.
	var fullState []types.StateEntry
	for _, stateBlockNID := range stateBlockNIDList.StateBlockNIDs {
		entries, ok := stateEntriesMap.lookup(stateBlockNID)
		if !ok {
			// This should only get hit if the database is corrupt.
			// It should be impossible for an event to reference a NID that doesn't exist
			panic(fmt.Errorf("corrupt DB: Missing state block numeric ID %d", stateBlockNID))
		}
		fullState = append(fullState, entries...)
	}

	// Stable sort so that the most recent entry for each state key stays
	// remains later in the list than the older entries for the same state key.
	sort.Stable(stateEntryByStateKeySorter(fullState))
	// Unique returns the last entry and hence the most recent entry for each state key.
	fullState = fullState[:xutil.Unique(stateEntryByStateKeySorter(fullState))]
	return fullState, nil
}

type stateEntryListMap []types.StateEntryList

func (m stateEntryListMap) lookup(stateBlockNID types.StateBlockNID) (stateEntries []types.StateEntry, ok bool) {
	list := []types.StateEntryList(m)
	i := sort.Search(len(list), func(i int) bool {
		return list[i].StateBlockNID >= stateBlockNID
	})
	if i < len(list) && list[i].StateBlockNID == stateBlockNID {
		ok = true
		stateEntries = list[i].StateEntries
	}
	return
}

type stateEntryByStateKeySorter []types.StateEntry

func (s stateEntryByStateKeySorter) Len() int { return len(s) }
func (s stateEntryByStateKeySorter) Less(i, j int) bool {
	return s[i].StateKeyTuple.LessThan(s[j].StateKeyTuple)
}
func (s stateEntryByStateKeySorter) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
