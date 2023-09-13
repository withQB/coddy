package sqlite3

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/types"
)

const purgeEventJSONSQL = "" +
	"DELETE FROM dataframe_event_json WHERE event_nid IN (" +
	"	SELECT event_nid FROM dataframe_events WHERE frame_nid = $1" +
	")"

const purgeEventsSQL = "" +
	"DELETE FROM dataframe_events WHERE frame_nid = $1"

const purgeInvitesSQL = "" +
	"DELETE FROM dataframe_invites WHERE frame_nid = $1"

const purgeMembershipsSQL = "" +
	"DELETE FROM dataframe_membership WHERE frame_nid = $1"

const purgePreviousEventsSQL = "" +
	"DELETE FROM dataframe_previous_events WHERE event_nids IN(" +
	"	SELECT event_nid FROM dataframe_events WHERE frame_nid = $1" +
	")"

const purgePublishedSQL = "" +
	"DELETE FROM dataframe_published WHERE frame_id = $1"

const purgeRedactionsSQL = "" +
	"DELETE FROM dataframe_redactions WHERE redaction_event_id IN(" +
	"	SELECT event_id FROM dataframe_events WHERE frame_nid = $1" +
	")"

const purgeFrameAliasesSQL = "" +
	"DELETE FROM dataframe_frame_aliases WHERE frame_id = $1"

const purgeFrameSQL = "" +
	"DELETE FROM dataframe_frames WHERE frame_nid = $1"

const purgeStateSnapshotEntriesSQL = "" +
	"DELETE FROM dataframe_state_snapshots WHERE frame_nid = $1"

type purgeStatements struct {
	purgeEventJSONStmt            *sql.Stmt
	purgeEventsStmt               *sql.Stmt
	purgeInvitesStmt              *sql.Stmt
	purgeMembershipsStmt          *sql.Stmt
	purgePreviousEventsStmt       *sql.Stmt
	purgePublishedStmt            *sql.Stmt
	purgeRedactionStmt            *sql.Stmt
	purgeFrameAliasesStmt          *sql.Stmt
	purgeFrameStmt                 *sql.Stmt
	purgeStateSnapshotEntriesStmt *sql.Stmt
	stateSnapshot                 *stateSnapshotStatements
}

func PreparePurgeStatements(db *sql.DB, stateSnapshot *stateSnapshotStatements) (*purgeStatements, error) {
	s := &purgeStatements{stateSnapshot: stateSnapshot}
	return s, sqlutil.StatementList{
		{&s.purgeEventJSONStmt, purgeEventJSONSQL},
		{&s.purgeEventsStmt, purgeEventsSQL},
		{&s.purgeInvitesStmt, purgeInvitesSQL},
		{&s.purgeMembershipsStmt, purgeMembershipsSQL},
		{&s.purgePublishedStmt, purgePublishedSQL},
		{&s.purgePreviousEventsStmt, purgePreviousEventsSQL},
		{&s.purgeRedactionStmt, purgeRedactionsSQL},
		{&s.purgeFrameAliasesStmt, purgeFrameAliasesSQL},
		{&s.purgeFrameStmt, purgeFrameSQL},
		//{&s.purgeStateBlockEntriesStmt, purgeStateBlockEntriesSQL},
		{&s.purgeStateSnapshotEntriesStmt, purgeStateSnapshotEntriesSQL},
	}.Prepare(db)
}

func (s *purgeStatements) PurgeFrame(
	ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, frameID string,
) error {

	// purge by frameID
	purgeByFrameID := []*sql.Stmt{
		s.purgeFrameAliasesStmt,
		s.purgePublishedStmt,
	}
	for _, stmt := range purgeByFrameID {
		_, err := sqlutil.TxStmt(txn, stmt).ExecContext(ctx, frameID)
		if err != nil {
			return err
		}
	}

	// purge by frameNID
	if err := s.purgeStateBlocks(ctx, txn, frameNID); err != nil {
		return err
	}

	purgeByFrameNID := []*sql.Stmt{
		s.purgeStateSnapshotEntriesStmt,
		s.purgeInvitesStmt,
		s.purgeMembershipsStmt,
		s.purgePreviousEventsStmt,
		s.purgeEventJSONStmt,
		s.purgeRedactionStmt,
		s.purgeEventsStmt,
		s.purgeFrameStmt,
	}
	for _, stmt := range purgeByFrameNID {
		_, err := sqlutil.TxStmt(txn, stmt).ExecContext(ctx, frameNID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *purgeStatements) purgeStateBlocks(
	ctx context.Context, txn *sql.Tx, frameNID types.FrameNID,
) error {
	// Get all stateBlockNIDs
	stateBlockNIDs, err := s.stateSnapshot.selectStateBlockNIDsForFrameNID(ctx, txn, frameNID)
	if err != nil {
		return err
	}
	params := make([]interface{}, len(stateBlockNIDs))
	seenNIDs := make(map[types.StateBlockNID]struct{}, len(stateBlockNIDs))
	// dedupe NIDs
	for k, v := range stateBlockNIDs {
		if _, ok := seenNIDs[v]; ok {
			continue
		}
		params[k] = v
		seenNIDs[v] = struct{}{}
	}

	query := "DELETE FROM dataframe_state_block WHERE state_block_nid IN($1)"
	return sqlutil.RunLimitedVariablesExec(ctx, query, txn, params, sqlutil.SQLite3MaxVariables)
}
