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
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/federationapi/types"
	"github.com/withqb/xtools/spec"
)

const joinedHostsSchema = `
-- The joined_hosts table stores a list of m.frame.member event ids in the
-- current state for each frame where the membership is "join".
-- There will be an entry for every user that is joined to the frame.
CREATE TABLE IF NOT EXISTS federationsender_joined_hosts (
    -- The string ID of the frame.
    frame_id TEXT NOT NULL,
    -- The event ID of the m.frame.member join event.
    event_id TEXT NOT NULL,
    -- The domain part of the user ID the m.frame.member event is for.
    server_name TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS federatonsender_joined_hosts_event_id_idx
    ON federationsender_joined_hosts (event_id);

CREATE INDEX IF NOT EXISTS federatonsender_joined_hosts_frame_id_idx
    ON federationsender_joined_hosts (frame_id)
`

const insertJoinedHostsSQL = "" +
	"INSERT OR IGNORE INTO federationsender_joined_hosts (frame_id, event_id, server_name)" +
	" VALUES ($1, $2, $3)"

const deleteJoinedHostsSQL = "" +
	"DELETE FROM federationsender_joined_hosts WHERE event_id = $1"

const deleteJoinedHostsForFrameSQL = "" +
	"DELETE FROM federationsender_joined_hosts WHERE frame_id = $1"

const selectJoinedHostsSQL = "" +
	"SELECT event_id, server_name FROM federationsender_joined_hosts" +
	" WHERE frame_id = $1"

const selectAllJoinedHostsSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts"

const selectJoinedHostsForFramesSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts WHERE frame_id IN ($1)"

const selectJoinedHostsForFramesExcludingBlacklistedSQL = "" +
	"SELECT DISTINCT server_name FROM federationsender_joined_hosts j WHERE frame_id IN ($1) AND NOT EXISTS (" +
	"  SELECT server_name FROM federationsender_blacklist WHERE j.server_name = server_name" +
	");"

type joinedHostsStatements struct {
	db                           *sql.DB
	insertJoinedHostsStmt        *sql.Stmt
	deleteJoinedHostsStmt        *sql.Stmt
	deleteJoinedHostsForFrameStmt *sql.Stmt
	selectJoinedHostsStmt        *sql.Stmt
	selectAllJoinedHostsStmt     *sql.Stmt
	// selectJoinedHostsForFramesStmt *sql.Stmt - prepared at runtime due to variadic
	// selectJoinedHostsForFramesExcludingBlacklistedStmt *sql.Stmt - prepared at runtime due to variadic
}

func NewSQLiteJoinedHostsTable(db *sql.DB) (s *joinedHostsStatements, err error) {
	s = &joinedHostsStatements{
		db: db,
	}
	_, err = db.Exec(joinedHostsSchema)
	if err != nil {
		return
	}

	return s, sqlutil.StatementList{
		{&s.insertJoinedHostsStmt, insertJoinedHostsSQL},
		{&s.deleteJoinedHostsStmt, deleteJoinedHostsSQL},
		{&s.deleteJoinedHostsForFrameStmt, deleteJoinedHostsForFrameSQL},
		{&s.selectJoinedHostsStmt, selectJoinedHostsSQL},
		{&s.selectAllJoinedHostsStmt, selectAllJoinedHostsSQL},
	}.Prepare(db)
}

func (s *joinedHostsStatements) InsertJoinedHosts(
	ctx context.Context,
	txn *sql.Tx,
	frameID, eventID string,
	serverName spec.ServerName,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertJoinedHostsStmt)
	_, err := stmt.ExecContext(ctx, frameID, eventID, serverName)
	return err
}

func (s *joinedHostsStatements) DeleteJoinedHosts(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) error {
	for _, eventID := range eventIDs {
		stmt := sqlutil.TxStmt(txn, s.deleteJoinedHostsStmt)
		if _, err := stmt.ExecContext(ctx, eventID); err != nil {
			return err
		}
	}
	return nil
}

func (s *joinedHostsStatements) DeleteJoinedHostsForFrame(
	ctx context.Context, txn *sql.Tx, frameID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteJoinedHostsForFrameStmt)
	_, err := stmt.ExecContext(ctx, frameID)
	return err
}

func (s *joinedHostsStatements) SelectJoinedHostsWithTx(
	ctx context.Context, txn *sql.Tx, frameID string,
) ([]types.JoinedHost, error) {
	stmt := sqlutil.TxStmt(txn, s.selectJoinedHostsStmt)
	return joinedHostsFromStmt(ctx, stmt, frameID)
}

func (s *joinedHostsStatements) SelectJoinedHosts(
	ctx context.Context, frameID string,
) ([]types.JoinedHost, error) {
	return joinedHostsFromStmt(ctx, s.selectJoinedHostsStmt, frameID)
}

func (s *joinedHostsStatements) SelectAllJoinedHosts(
	ctx context.Context,
) ([]spec.ServerName, error) {
	rows, err := s.selectAllJoinedHostsStmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAllJoinedHosts: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, rows.Err()
}

func (s *joinedHostsStatements) SelectJoinedHostsForFrames(
	ctx context.Context, frameIDs []string, excludingBlacklisted bool,
) ([]spec.ServerName, error) {
	iFrameIDs := make([]interface{}, len(frameIDs))
	for i := range frameIDs {
		iFrameIDs[i] = frameIDs[i]
	}
	query := selectJoinedHostsForFramesSQL
	if excludingBlacklisted {
		query = selectJoinedHostsForFramesExcludingBlacklistedSQL
	}
	sql := strings.Replace(query, "($1)", sqlutil.QueryVariadic(len(iFrameIDs)), 1)
	rows, err := s.db.QueryContext(ctx, sql, iFrameIDs...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedHostsForFramesStmt: rows.close() failed")

	var result []spec.ServerName
	for rows.Next() {
		var serverName string
		if err = rows.Scan(&serverName); err != nil {
			return nil, err
		}
		result = append(result, spec.ServerName(serverName))
	}

	return result, rows.Err()
}

func joinedHostsFromStmt(
	ctx context.Context, stmt *sql.Stmt, frameID string,
) ([]types.JoinedHost, error) {
	rows, err := stmt.QueryContext(ctx, frameID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "joinedHostsFromStmt: rows.close() failed")

	var result []types.JoinedHost
	for rows.Next() {
		var eventID, serverName string
		if err = rows.Scan(&eventID, &serverName); err != nil {
			return nil, err
		}
		result = append(result, types.JoinedHost{
			MemberEventID: eventID,
			ServerName:    spec.ServerName(serverName),
		})
	}

	return result, nil
}
