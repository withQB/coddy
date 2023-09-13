package shared

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/withqb/xtools"

	"github.com/withqb/coddy/servers/dataframe/types"
)

type FrameUpdater struct {
	transaction
	d                       *Database
	frameInfo                *types.FrameInfo
	latestEvents            []types.StateAtEventAndReference
	lastEventIDSent         string
	currentStateSnapshotNID types.StateSnapshotNID
	frameExists              bool
}

func rollback(txn *sql.Tx) {
	if txn == nil {
		return
	}
	txn.Rollback() // nolint: errcheck
}

func NewFrameUpdater(ctx context.Context, d *Database, txn *sql.Tx, frameInfo *types.FrameInfo) (*FrameUpdater, error) {
	// If the frameInfo is nil then that means that the frame doesn't exist
	// yet, so we can't do `SelectLatestEventsNIDsForUpdate` because that
	// would involve locking a row on the table that doesn't exist. Instead
	// we will just run with a normal database transaction. It'll either
	// succeed, processing a create event which creates the frame, or it won't.
	if frameInfo == nil {
		return &FrameUpdater{
			transaction{ctx, txn}, d, nil, nil, "", 0, false,
		}, nil
	}

	eventNIDs, lastEventNIDSent, currentStateSnapshotNID, err :=
		d.FramesTable.SelectLatestEventsNIDsForUpdate(ctx, txn, frameInfo.FrameNID)
	if err != nil {
		rollback(txn)
		return nil, err
	}
	stateAndRefs, err := d.EventsTable.BulkSelectStateAtEventAndReference(ctx, txn, eventNIDs)
	if err != nil {
		rollback(txn)
		return nil, err
	}
	var lastEventIDSent string
	if lastEventNIDSent != 0 {
		lastEventIDSent, err = d.EventsTable.SelectEventID(ctx, txn, lastEventNIDSent)
		if err != nil {
			rollback(txn)
			return nil, err
		}
	}
	return &FrameUpdater{
		transaction{ctx, txn}, d, frameInfo, stateAndRefs, lastEventIDSent, currentStateSnapshotNID, true,
	}, nil
}

// FrameExists returns true if the frame exists and false otherwise.
func (u *FrameUpdater) FrameExists() bool {
	return u.frameExists
}

// Implements sqlutil.Transaction
func (u *FrameUpdater) Commit() error {
	if u.txn == nil { // SQLite mode probably
		return nil
	}
	return u.txn.Commit()
}

// Implements sqlutil.Transaction
func (u *FrameUpdater) Rollback() error {
	if u.txn == nil { // SQLite mode probably
		return nil
	}
	return u.txn.Rollback()
}

// FrameVersion implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) FrameVersion() (version xtools.FrameVersion) {
	return u.frameInfo.FrameVersion
}

// LatestEvents implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) LatestEvents() []types.StateAtEventAndReference {
	return u.latestEvents
}

// LastEventIDSent implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) LastEventIDSent() string {
	return u.lastEventIDSent
}

// CurrentStateSnapshotNID implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) CurrentStateSnapshotNID() types.StateSnapshotNID {
	return u.currentStateSnapshotNID
}

func (u *FrameUpdater) Events(ctx context.Context, _ xtools.FrameVersion, eventNIDs []types.EventNID) ([]types.Event, error) {
	if u.frameInfo == nil {
		return nil, types.ErrorInvalidFrameInfo
	}
	return u.d.events(ctx, u.txn, u.frameInfo.FrameVersion, eventNIDs)
}

func (u *FrameUpdater) SnapshotNIDFromEventID(
	ctx context.Context, eventID string,
) (types.StateSnapshotNID, error) {
	return u.d.snapshotNIDFromEventID(ctx, u.txn, eventID)
}

func (u *FrameUpdater) StateBlockNIDs(
	ctx context.Context, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	return u.d.stateBlockNIDs(ctx, u.txn, stateNIDs)
}

func (u *FrameUpdater) StateEntries(
	ctx context.Context, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntryList, error) {
	return u.d.stateEntries(ctx, u.txn, stateBlockNIDs)
}

func (u *FrameUpdater) StateEntriesForTuples(
	ctx context.Context,
	stateBlockNIDs []types.StateBlockNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntryList, error) {
	return u.d.stateEntriesForTuples(ctx, u.txn, stateBlockNIDs, stateKeyTuples)
}

func (u *FrameUpdater) AddState(
	ctx context.Context,
	frameNID types.FrameNID,
	stateBlockNIDs []types.StateBlockNID,
	state []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	return u.d.addState(ctx, u.txn, frameNID, stateBlockNIDs, state)
}

func (u *FrameUpdater) SetState(
	ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID,
) error {
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		return u.d.EventsTable.UpdateEventState(ctx, txn, eventNID, stateNID)
	})
}

func (u *FrameUpdater) EventTypeNIDs(
	ctx context.Context, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	return u.d.eventTypeNIDs(ctx, u.txn, eventTypes)
}

func (u *FrameUpdater) EventStateKeyNIDs(
	ctx context.Context, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	return u.d.eventStateKeyNIDs(ctx, u.txn, eventStateKeys)
}

func (u *FrameUpdater) FrameInfo(ctx context.Context, frameID string) (*types.FrameInfo, error) {
	return u.d.frameInfo(ctx, u.txn, frameID)
}

func (u *FrameUpdater) EventIDs(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID]string, error) {
	return u.d.EventsTable.BulkSelectEventID(ctx, u.txn, eventNIDs)
}

func (u *FrameUpdater) BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error) {
	return u.d.EventsTable.BulkSelectSnapshotsFromEventIDs(ctx, u.txn, eventIDs)
}

func (u *FrameUpdater) StateAtEventIDs(
	ctx context.Context, eventIDs []string,
) ([]types.StateAtEvent, error) {
	return u.d.EventsTable.BulkSelectStateAtEventByID(ctx, u.txn, eventIDs)
}

func (u *FrameUpdater) EventsFromIDs(ctx context.Context, frameInfo *types.FrameInfo, eventIDs []string) ([]types.Event, error) {
	return u.d.eventsFromIDs(ctx, u.txn, u.frameInfo, eventIDs, NoFilter)
}

// IsReferenced implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) IsReferenced(eventID string) (bool, error) {
	err := u.d.PrevEventsTable.SelectPreviousEventExists(u.ctx, u.txn, eventID)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, fmt.Errorf("u.d.PrevEventsTable.SelectPreviousEventExists: %w", err)
}

// SetLatestEvents implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) SetLatestEvents(
	frameNID types.FrameNID, latest []types.StateAtEventAndReference, lastEventNIDSent types.EventNID,
	currentStateSnapshotNID types.StateSnapshotNID,
) error {
	switch {
	case len(latest) == 0:
		return fmt.Errorf("cannot set latest events with no latest event references")
	case currentStateSnapshotNID == 0:
		return fmt.Errorf("cannot set latest events with invalid state snapshot NID")
	case lastEventNIDSent == 0:
		return fmt.Errorf("cannot set latest events with invalid latest event NID")
	}
	eventNIDs := make([]types.EventNID, len(latest))
	for i := range latest {
		eventNIDs[i] = latest[i].EventNID
	}
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		if err := u.d.FramesTable.UpdateLatestEventNIDs(u.ctx, txn, frameNID, eventNIDs, lastEventNIDSent, currentStateSnapshotNID); err != nil {
			return fmt.Errorf("u.d.FramesTable.updateLatestEventNIDs: %w", err)
		}

		// Since it's entirely possible that this types.FrameInfo came from the
		// cache, we should make sure to update that entry so that the next run
		// works from live data.
		if u.frameInfo != nil {
			u.frameInfo.SetStateSnapshotNID(currentStateSnapshotNID)
			u.frameInfo.SetIsStub(false)
		}
		return nil
	})
}

// HasEventBeenSent implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) HasEventBeenSent(eventNID types.EventNID) (bool, error) {
	return u.d.EventsTable.SelectEventSentToOutput(u.ctx, u.txn, eventNID)
}

// MarkEventAsSent implements types.FrameRecentEventsUpdater
func (u *FrameUpdater) MarkEventAsSent(eventNID types.EventNID) error {
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		return u.d.EventsTable.UpdateEventSentToOutput(u.ctx, txn, eventNID)
	})
}

func (u *FrameUpdater) MembershipUpdater(targetUserNID types.EventStateKeyNID, targetLocal bool) (*MembershipUpdater, error) {
	return u.d.membershipUpdaterTxn(u.ctx, u.txn, u.frameInfo.FrameNID, targetUserNID, targetLocal)
}
