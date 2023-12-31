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

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/storage/tables"
	"github.com/withqb/coddy/services/dataframe/types"
)

const inviteSchema = `
	CREATE TABLE IF NOT EXISTS dataframe_invites (
		invite_event_id TEXT PRIMARY KEY,
		frame_nid INTEGER NOT NULL,
		target_nid INTEGER NOT NULL,
		sender_nid INTEGER NOT NULL DEFAULT 0,
		retired BOOLEAN NOT NULL DEFAULT FALSE,
		invite_event_json TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS dataframe_invites_active_idx ON dataframe_invites (target_nid, frame_nid)
		WHERE NOT retired;
`
const insertInviteEventSQL = "" +
	"INSERT INTO dataframe_invites (invite_event_id, frame_nid, target_nid," +
	" sender_nid, invite_event_json) VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT DO NOTHING"

const selectInviteActiveForUserInFrameSQL = "" +
	"SELECT invite_event_id, sender_nid, invite_event_json FROM dataframe_invites" +
	" WHERE target_nid = $1 AND frame_nid = $2" +
	" AND NOT retired"

// Retire every active invite for a user in a frame.
// Ideally we'd know which invite events were retired by a given update so we
// wouldn't need to remove every active invite.
// However the coddy protocol doesn't give us a way to reliably identify the
// invites that were retired, so we are forced to retire all of them.
const updateInviteRetiredSQL = `
	UPDATE dataframe_invites SET retired = TRUE WHERE frame_nid = $1 AND target_nid = $2 AND NOT retired
`

const selectInvitesAboutToRetireSQL = `
SELECT invite_event_id FROM dataframe_invites WHERE frame_nid = $1 AND target_nid = $2 AND NOT retired
`

type inviteStatements struct {
	db                                  *sql.DB
	insertInviteEventStmt               *sql.Stmt
	selectInviteActiveForUserInFrameStmt *sql.Stmt
	updateInviteRetiredStmt             *sql.Stmt
	selectInvitesAboutToRetireStmt      *sql.Stmt
}

func CreateInvitesTable(db *sql.DB) error {
	_, err := db.Exec(inviteSchema)
	return err
}

func PrepareInvitesTable(db *sql.DB) (tables.Invites, error) {
	s := &inviteStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertInviteEventStmt, insertInviteEventSQL},
		{&s.selectInviteActiveForUserInFrameStmt, selectInviteActiveForUserInFrameSQL},
		{&s.updateInviteRetiredStmt, updateInviteRetiredSQL},
		{&s.selectInvitesAboutToRetireStmt, selectInvitesAboutToRetireSQL},
	}.Prepare(db)
}

func (s *inviteStatements) InsertInviteEvent(
	ctx context.Context, txn *sql.Tx,
	inviteEventID string, frameNID types.FrameNID,
	targetUserNID, senderUserNID types.EventStateKeyNID,
	inviteEventJSON []byte,
) (bool, error) {
	var count int64
	stmt := sqlutil.TxStmt(txn, s.insertInviteEventStmt)
	result, err := stmt.ExecContext(
		ctx, inviteEventID, frameNID, targetUserNID, senderUserNID, inviteEventJSON,
	)
	if err != nil {
		return false, err
	}
	count, err = result.RowsAffected()
	if err != nil {
		return false, err
	}
	return count != 0, err
}

func (s *inviteStatements) UpdateInviteRetired(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
) (eventIDs []string, err error) {
	// gather all the event IDs we will retire
	stmt := sqlutil.TxStmt(txn, s.selectInvitesAboutToRetireStmt)
	rows, err := stmt.QueryContext(ctx, frameNID, targetUserNID)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "UpdateInviteRetired: rows.close() failed")
	var inviteEventID string
	for rows.Next() {
		if err = rows.Scan(&inviteEventID); err != nil {
			return
		}
		eventIDs = append(eventIDs, inviteEventID)
	}
	// now retire the invites
	stmt = sqlutil.TxStmt(txn, s.updateInviteRetiredStmt)
	_, err = stmt.ExecContext(ctx, frameNID, targetUserNID)
	return
}

// selectInviteActiveForUserInFrame returns a list of sender state key NIDs
func (s *inviteStatements) SelectInviteActiveForUserInFrame(
	ctx context.Context, txn *sql.Tx,
	targetUserNID types.EventStateKeyNID, frameNID types.FrameNID,
) ([]types.EventStateKeyNID, []string, []byte, error) {
	stmt := sqlutil.TxStmt(txn, s.selectInviteActiveForUserInFrameStmt)
	rows, err := stmt.QueryContext(
		ctx, targetUserNID, frameNID,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectInviteActiveForUserInFrame: rows.close() failed")
	var result []types.EventStateKeyNID
	var eventIDs []string
	var eventID string
	var senderUserNID int64
	var eventJSON []byte
	for rows.Next() {
		if err := rows.Scan(&eventID, &senderUserNID, &eventJSON); err != nil {
			return nil, nil, nil, err
		}
		result = append(result, types.EventStateKeyNID(senderUserNID))
		eventIDs = append(eventIDs, eventID)
	}
	return result, eventIDs, eventJSON, nil
}
