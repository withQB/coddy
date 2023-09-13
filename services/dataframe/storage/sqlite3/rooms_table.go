// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Coddy.org Foundation C.I.C.
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

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/storage/tables"
	"github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/xtools"
)

const framesSchema = `
  CREATE TABLE IF NOT EXISTS dataframe_frames (
    frame_nid INTEGER PRIMARY KEY AUTOINCREMENT,
    frame_id TEXT NOT NULL UNIQUE,
    latest_event_nids TEXT NOT NULL DEFAULT '[]',
    last_event_sent_nid INTEGER NOT NULL DEFAULT 0,
    state_snapshot_nid INTEGER NOT NULL DEFAULT 0,
    frame_version TEXT NOT NULL
  );
`

// Same as insertEventTypeNIDSQL
const insertFrameNIDSQL = `
	INSERT INTO dataframe_frames (frame_id, frame_version) VALUES ($1, $2)
	  ON CONFLICT DO NOTHING
	  RETURNING frame_nid;
`

const selectFrameNIDSQL = "" +
	"SELECT frame_nid FROM dataframe_frames WHERE frame_id = $1"

const selectLatestEventNIDsSQL = "" +
	"SELECT latest_event_nids, state_snapshot_nid FROM dataframe_frames WHERE frame_nid = $1"

const selectLatestEventNIDsForUpdateSQL = "" +
	"SELECT latest_event_nids, last_event_sent_nid, state_snapshot_nid FROM dataframe_frames WHERE frame_nid = $1"

const updateLatestEventNIDsSQL = "" +
	"UPDATE dataframe_frames SET latest_event_nids = $1, last_event_sent_nid = $2, state_snapshot_nid = $3 WHERE frame_nid = $4"

const selectFrameVersionsForFrameNIDsSQL = "" +
	"SELECT frame_nid, frame_version FROM dataframe_frames WHERE frame_nid IN ($1)"

const selectFrameInfoSQL = "" +
	"SELECT frame_version, frame_nid, state_snapshot_nid, latest_event_nids FROM dataframe_frames WHERE frame_id = $1"

const selectFrameIDsSQL = "" +
	"SELECT frame_id FROM dataframe_frames WHERE latest_event_nids != '[]'"

const bulkSelectFrameIDsSQL = "" +
	"SELECT frame_id FROM dataframe_frames WHERE frame_nid IN ($1)"

const bulkSelectFrameNIDsSQL = "" +
	"SELECT frame_nid FROM dataframe_frames WHERE frame_id IN ($1)"

const selectFrameNIDForUpdateSQL = "" +
	"SELECT frame_nid FROM dataframe_frames WHERE frame_id = $1"

type frameStatements struct {
	db                                 *sql.DB
	insertFrameNIDStmt                  *sql.Stmt
	selectFrameNIDStmt                  *sql.Stmt
	selectFrameNIDForUpdateStmt         *sql.Stmt
	selectLatestEventNIDsStmt          *sql.Stmt
	selectLatestEventNIDsForUpdateStmt *sql.Stmt
	updateLatestEventNIDsStmt          *sql.Stmt
	//selectFrameVersionForFrameNIDStmt    *sql.Stmt
	selectFrameInfoStmt *sql.Stmt
	selectFrameIDsStmt  *sql.Stmt
}

func CreateFramesTable(db *sql.DB) error {
	_, err := db.Exec(framesSchema)
	return err
}

func PrepareFramesTable(db *sql.DB) (tables.Frames, error) {
	s := &frameStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertFrameNIDStmt, insertFrameNIDSQL},
		{&s.selectFrameNIDStmt, selectFrameNIDSQL},
		{&s.selectLatestEventNIDsStmt, selectLatestEventNIDsSQL},
		{&s.selectLatestEventNIDsForUpdateStmt, selectLatestEventNIDsForUpdateSQL},
		{&s.updateLatestEventNIDsStmt, updateLatestEventNIDsSQL},
		//{&s.selectFrameVersionForFrameNIDsStmt, selectFrameVersionForFrameNIDsSQL},
		{&s.selectFrameInfoStmt, selectFrameInfoSQL},
		{&s.selectFrameIDsStmt, selectFrameIDsSQL},
		{&s.selectFrameNIDForUpdateStmt, selectFrameNIDForUpdateSQL},
	}.Prepare(db)
}

func (s *frameStatements) SelectFrameIDsWithEvents(ctx context.Context, txn *sql.Tx) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectFrameIDsStmt)
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectFrameIDsStmt: rows.close() failed")
	var frameIDs []string
	var frameID string
	for rows.Next() {
		if err = rows.Scan(&frameID); err != nil {
			return nil, err
		}
		frameIDs = append(frameIDs, frameID)
	}
	return frameIDs, nil
}

func (s *frameStatements) SelectFrameInfo(ctx context.Context, txn *sql.Tx, frameID string) (*types.FrameInfo, error) {
	var info types.FrameInfo
	var latestNIDsJSON string
	var stateSnapshotNID types.StateSnapshotNID
	stmt := sqlutil.TxStmt(txn, s.selectFrameInfoStmt)
	err := stmt.QueryRowContext(ctx, frameID).Scan(
		&info.FrameVersion, &info.FrameNID, &stateSnapshotNID, &latestNIDsJSON,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var latestNIDs []int64
	if err = json.Unmarshal([]byte(latestNIDsJSON), &latestNIDs); err != nil {
		return nil, err
	}
	info.SetStateSnapshotNID(stateSnapshotNID)
	info.SetIsStub(len(latestNIDs) == 0)
	return &info, err
}

func (s *frameStatements) InsertFrameNID(
	ctx context.Context, txn *sql.Tx,
	frameID string, frameVersion xtools.FrameVersion,
) (frameNID types.FrameNID, err error) {
	insertStmt := sqlutil.TxStmt(txn, s.insertFrameNIDStmt)
	if err = insertStmt.QueryRowContext(ctx, frameID, frameVersion).Scan(&frameNID); err != nil {
		return 0, fmt.Errorf("resultStmt.QueryRowContext.Scan: %w", err)
	}
	return
}

func (s *frameStatements) SelectFrameNID(
	ctx context.Context, txn *sql.Tx, frameID string,
) (types.FrameNID, error) {
	var frameNID int64
	stmt := sqlutil.TxStmt(txn, s.selectFrameNIDStmt)
	err := stmt.QueryRowContext(ctx, frameID).Scan(&frameNID)
	return types.FrameNID(frameNID), err
}

func (s *frameStatements) SelectFrameNIDForUpdate(
	ctx context.Context, txn *sql.Tx, frameID string,
) (types.FrameNID, error) {
	var frameNID int64
	stmt := sqlutil.TxStmt(txn, s.selectFrameNIDForUpdateStmt)
	err := stmt.QueryRowContext(ctx, frameID).Scan(&frameNID)
	return types.FrameNID(frameNID), err
}

func (s *frameStatements) SelectLatestEventNIDs(
	ctx context.Context, txn *sql.Tx, frameNID types.FrameNID,
) ([]types.EventNID, types.StateSnapshotNID, error) {
	var eventNIDs []types.EventNID
	var nidsJSON string
	var stateSnapshotNID int64
	stmt := sqlutil.TxStmt(txn, s.selectLatestEventNIDsStmt)
	err := stmt.QueryRowContext(ctx, int64(frameNID)).Scan(&nidsJSON, &stateSnapshotNID)
	if err != nil {
		return nil, 0, err
	}
	if err := json.Unmarshal([]byte(nidsJSON), &eventNIDs); err != nil {
		return nil, 0, err
	}
	return eventNIDs, types.StateSnapshotNID(stateSnapshotNID), nil
}

func (s *frameStatements) SelectLatestEventsNIDsForUpdate(
	ctx context.Context, txn *sql.Tx, frameNID types.FrameNID,
) ([]types.EventNID, types.EventNID, types.StateSnapshotNID, error) {
	var eventNIDs []types.EventNID
	var nidsJSON string
	var lastEventSentNID int64
	var stateSnapshotNID int64
	stmt := sqlutil.TxStmt(txn, s.selectLatestEventNIDsForUpdateStmt)
	err := stmt.QueryRowContext(ctx, int64(frameNID)).Scan(&nidsJSON, &lastEventSentNID, &stateSnapshotNID)
	if err != nil {
		return nil, 0, 0, err
	}
	if err := json.Unmarshal([]byte(nidsJSON), &eventNIDs); err != nil {
		return nil, 0, 0, err
	}
	return eventNIDs, types.EventNID(lastEventSentNID), types.StateSnapshotNID(stateSnapshotNID), nil
}

func (s *frameStatements) UpdateLatestEventNIDs(
	ctx context.Context,
	txn *sql.Tx,
	frameNID types.FrameNID,
	eventNIDs []types.EventNID,
	lastEventSentNID types.EventNID,
	stateSnapshotNID types.StateSnapshotNID,
) error {
	stmt := sqlutil.TxStmt(txn, s.updateLatestEventNIDsStmt)
	_, err := stmt.ExecContext(
		ctx,
		eventNIDsAsArray(eventNIDs),
		int64(lastEventSentNID),
		int64(stateSnapshotNID),
		frameNID,
	)
	return err
}

func (s *frameStatements) SelectFrameVersionsForFrameNIDs(
	ctx context.Context, txn *sql.Tx, frameNIDs []types.FrameNID,
) (map[types.FrameNID]xtools.FrameVersion, error) {
	sqlStr := strings.Replace(selectFrameVersionsForFrameNIDsSQL, "($1)", sqlutil.QueryVariadic(len(frameNIDs)), 1)
	sqlPrep, err := s.db.Prepare(sqlStr)
	if err != nil {
		return nil, err
	}
	defer sqlPrep.Close() // nolint:errcheck
	sqlStmt := sqlutil.TxStmt(txn, sqlPrep)
	iFrameNIDs := make([]interface{}, len(frameNIDs))
	for i, v := range frameNIDs {
		iFrameNIDs[i] = v
	}
	rows, err := sqlStmt.QueryContext(ctx, iFrameNIDs...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectFrameVersionsForFrameNIDsStmt: rows.close() failed")
	result := make(map[types.FrameNID]xtools.FrameVersion)
	var frameNID types.FrameNID
	var frameVersion xtools.FrameVersion
	for rows.Next() {
		if err = rows.Scan(&frameNID, &frameVersion); err != nil {
			return nil, err
		}
		result[frameNID] = frameVersion
	}
	return result, nil
}

func (s *frameStatements) BulkSelectFrameIDs(ctx context.Context, txn *sql.Tx, frameNIDs []types.FrameNID) ([]string, error) {
	iFrameNIDs := make([]interface{}, len(frameNIDs))
	for i, v := range frameNIDs {
		iFrameNIDs[i] = v
	}
	sqlQuery := strings.Replace(bulkSelectFrameIDsSQL, "($1)", sqlutil.QueryVariadic(len(frameNIDs)), 1)
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, sqlQuery, iFrameNIDs...)
	} else {
		rows, err = s.db.QueryContext(ctx, sqlQuery, iFrameNIDs...)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectFrameIDsStmt: rows.close() failed")
	var frameIDs []string
	var frameID string
	for rows.Next() {
		if err = rows.Scan(&frameID); err != nil {
			return nil, err
		}
		frameIDs = append(frameIDs, frameID)
	}
	return frameIDs, nil
}

func (s *frameStatements) BulkSelectFrameNIDs(ctx context.Context, txn *sql.Tx, frameIDs []string) ([]types.FrameNID, error) {
	iFrameIDs := make([]interface{}, len(frameIDs))
	for i, v := range frameIDs {
		iFrameIDs[i] = v
	}
	sqlQuery := strings.Replace(bulkSelectFrameNIDsSQL, "($1)", sqlutil.QueryVariadic(len(frameIDs)), 1)
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, sqlQuery, iFrameIDs...)
	} else {
		rows, err = s.db.QueryContext(ctx, sqlQuery, iFrameIDs...)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "bulkSelectFrameNIDsStmt: rows.close() failed")
	var frameNIDs []types.FrameNID
	var frameNID types.FrameNID
	for rows.Next() {
		if err = rows.Scan(&frameNID); err != nil {
			return nil, err
		}
		frameNIDs = append(frameNIDs, frameNID)
	}
	return frameNIDs, nil
}
