// Copyright 2021 The Coddy.org Foundation C.I.C.
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

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/services/userapi/storage/tables"
)

const keyBackupTableSchema = `
CREATE TABLE IF NOT EXISTS userapi_key_backups (
    user_id TEXT NOT NULL,
    frame_id TEXT NOT NULL,
    session_id TEXT NOT NULL,

    version TEXT NOT NULL,
    first_message_index INTEGER NOT NULL,
    forwarded_count INTEGER NOT NULL,
    is_verified BOOLEAN NOT NULL,
    session_data TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS e2e_frame_keys_idx ON userapi_key_backups(user_id, frame_id, session_id, version);
CREATE INDEX IF NOT EXISTS e2e_frame_keys_versions_idx ON userapi_key_backups(user_id, version);
`

const insertBackupKeySQL = "" +
	"INSERT INTO userapi_key_backups(user_id, frame_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data) " +
	"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

const updateBackupKeySQL = "" +
	"UPDATE userapi_key_backups SET first_message_index=$1, forwarded_count=$2, is_verified=$3, session_data=$4 " +
	"WHERE user_id=$5 AND frame_id=$6 AND session_id=$7 AND version=$8"

const countKeysSQL = "" +
	"SELECT COUNT(*) FROM userapi_key_backups WHERE user_id = $1 AND version = $2"

const selectBackupKeysSQL = "" +
	"SELECT frame_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2"

const selectKeysByFrameIDSQL = "" +
	"SELECT frame_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND frame_id = $3"

const selectKeysByFrameIDAndSessionIDSQL = "" +
	"SELECT frame_id, session_id, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups " +
	"WHERE user_id = $1 AND version = $2 AND frame_id = $3 AND session_id = $4"

type keyBackupStatements struct {
	insertBackupKeyStmt                *sql.Stmt
	updateBackupKeyStmt                *sql.Stmt
	countKeysStmt                      *sql.Stmt
	selectKeysStmt                     *sql.Stmt
	selectKeysByFrameIDStmt             *sql.Stmt
	selectKeysByFrameIDAndSessionIDStmt *sql.Stmt
}

func NewSQLiteKeyBackupTable(db *sql.DB) (tables.KeyBackupTable, error) {
	s := &keyBackupStatements{}
	_, err := db.Exec(keyBackupTableSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertBackupKeyStmt, insertBackupKeySQL},
		{&s.updateBackupKeyStmt, updateBackupKeySQL},
		{&s.countKeysStmt, countKeysSQL},
		{&s.selectKeysStmt, selectBackupKeysSQL},
		{&s.selectKeysByFrameIDStmt, selectKeysByFrameIDSQL},
		{&s.selectKeysByFrameIDAndSessionIDStmt, selectKeysByFrameIDAndSessionIDSQL},
	}.Prepare(db)
}

func (s keyBackupStatements) CountKeys(
	ctx context.Context, txn *sql.Tx, userID, version string,
) (count int64, err error) {
	err = txn.Stmt(s.countKeysStmt).QueryRowContext(ctx, userID, version).Scan(&count)
	return
}

func (s *keyBackupStatements) InsertBackupKey(
	ctx context.Context, txn *sql.Tx, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	_, err = txn.Stmt(s.insertBackupKeyStmt).ExecContext(
		ctx, userID, key.FrameID, key.SessionID, version, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData),
	)
	return
}

func (s *keyBackupStatements) UpdateBackupKey(
	ctx context.Context, txn *sql.Tx, userID, version string, key api.InternalKeyBackupSession,
) (err error) {
	_, err = txn.Stmt(s.updateBackupKeyStmt).ExecContext(
		ctx, key.FirstMessageIndex, key.ForwardedCount, key.IsVerified, string(key.SessionData), userID, key.FrameID, key.SessionID, version,
	)
	return
}

func (s *keyBackupStatements) SelectKeys(
	ctx context.Context, txn *sql.Tx, userID, version string,
) (map[string]map[string]api.KeyBackupSession, error) {
	rows, err := txn.Stmt(s.selectKeysStmt).QueryContext(ctx, userID, version)
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

func (s *keyBackupStatements) SelectKeysByFrameID(
	ctx context.Context, txn *sql.Tx, userID, version, frameID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	rows, err := txn.Stmt(s.selectKeysByFrameIDStmt).QueryContext(ctx, userID, version, frameID)
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

func (s *keyBackupStatements) SelectKeysByFrameIDAndSessionID(
	ctx context.Context, txn *sql.Tx, userID, version, frameID, sessionID string,
) (map[string]map[string]api.KeyBackupSession, error) {
	rows, err := txn.Stmt(s.selectKeysByFrameIDAndSessionIDStmt).QueryContext(ctx, userID, version, frameID, sessionID)
	if err != nil {
		return nil, err
	}
	return unpackKeys(ctx, rows)
}

func unpackKeys(ctx context.Context, rows *sql.Rows) (map[string]map[string]api.KeyBackupSession, error) {
	result := make(map[string]map[string]api.KeyBackupSession)
	defer internal.CloseAndLogIfError(ctx, rows, "selectKeysStmt.Close failed")
	for rows.Next() {
		var key api.InternalKeyBackupSession
		// frame_id, session_id, first_message_index, forwarded_count, is_verified, session_data
		var sessionDataStr string
		if err := rows.Scan(&key.FrameID, &key.SessionID, &key.FirstMessageIndex, &key.ForwardedCount, &key.IsVerified, &sessionDataStr); err != nil {
			return nil, err
		}
		key.SessionData = json.RawMessage(sessionDataStr)
		frameData := result[key.FrameID]
		if frameData == nil {
			frameData = make(map[string]api.KeyBackupSession)
		}
		frameData[key.SessionID] = key.KeyBackupSession
		result[key.FrameID] = frameData
	}
	return result, nil
}
