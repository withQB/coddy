// Copyright 2018 New Vector Ltd
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

	"github.com/withqb/coddy/apis/syncapi/storage/tables"
	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
)

const backwardExtremitiesSchema = `
-- Stores output frame events received from the dataframe.
CREATE TABLE IF NOT EXISTS syncapi_backward_extremities (
	-- The 'frame_id' key for the event.
	frame_id TEXT NOT NULL,
	-- The event ID for the last known event. This is the backwards extremity.
	event_id TEXT NOT NULL,
	-- The prev_events for the last known event. This is used to update extremities.
	prev_event_id TEXT NOT NULL,
	PRIMARY KEY(frame_id, event_id, prev_event_id)
);
`

const insertBackwardExtremitySQL = "" +
	"INSERT INTO syncapi_backward_extremities (frame_id, event_id, prev_event_id)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT (frame_id, event_id, prev_event_id) DO NOTHING"

const selectBackwardExtremitiesForFrameSQL = "" +
	"SELECT event_id, prev_event_id FROM syncapi_backward_extremities WHERE frame_id = $1"

const deleteBackwardExtremitySQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE frame_id = $1 AND prev_event_id = $2"

const purgeBackwardExtremitiesSQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE frame_id = $1"

type backwardExtremitiesStatements struct {
	db                                   *sql.DB
	insertBackwardExtremityStmt          *sql.Stmt
	selectBackwardExtremitiesForFrameStmt *sql.Stmt
	deleteBackwardExtremityStmt          *sql.Stmt
	purgeBackwardExtremitiesStmt         *sql.Stmt
}

func NewSqliteBackwardsExtremitiesTable(db *sql.DB) (tables.BackwardsExtremities, error) {
	s := &backwardExtremitiesStatements{
		db: db,
	}
	_, err := db.Exec(backwardExtremitiesSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertBackwardExtremityStmt, insertBackwardExtremitySQL},
		{&s.selectBackwardExtremitiesForFrameStmt, selectBackwardExtremitiesForFrameSQL},
		{&s.deleteBackwardExtremityStmt, deleteBackwardExtremitySQL},
		{&s.purgeBackwardExtremitiesStmt, purgeBackwardExtremitiesSQL},
	}.Prepare(db)
}

func (s *backwardExtremitiesStatements) InsertsBackwardExtremity(
	ctx context.Context, txn *sql.Tx, frameID, eventID string, prevEventID string,
) (err error) {
	_, err = sqlutil.TxStmt(txn, s.insertBackwardExtremityStmt).ExecContext(ctx, frameID, eventID, prevEventID)
	return err
}

func (s *backwardExtremitiesStatements) SelectBackwardExtremitiesForFrame(
	ctx context.Context, txn *sql.Tx, frameID string,
) (bwExtrems map[string][]string, err error) {
	rows, err := sqlutil.TxStmt(txn, s.selectBackwardExtremitiesForFrameStmt).QueryContext(ctx, frameID)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectBackwardExtremitiesForFrame: rows.close() failed")

	bwExtrems = make(map[string][]string)
	for rows.Next() {
		var eID string
		var prevEventID string
		if err = rows.Scan(&eID, &prevEventID); err != nil {
			return
		}
		bwExtrems[eID] = append(bwExtrems[eID], prevEventID)
	}

	return bwExtrems, rows.Err()
}

func (s *backwardExtremitiesStatements) DeleteBackwardExtremity(
	ctx context.Context, txn *sql.Tx, frameID, knownEventID string,
) (err error) {
	_, err = sqlutil.TxStmt(txn, s.deleteBackwardExtremityStmt).ExecContext(ctx, frameID, knownEventID)
	return err
}

func (s *backwardExtremitiesStatements) PurgeBackwardExtremities(
	ctx context.Context, txn *sql.Tx, frameID string,
) error {
	_, err := sqlutil.TxStmt(txn, s.purgeBackwardExtremitiesStmt).ExecContext(ctx, frameID)
	return err
}
