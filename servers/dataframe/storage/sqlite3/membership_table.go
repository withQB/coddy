// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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
	"fmt"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/storage/sqlite3/deltas"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/spec"
)

const membershipSchema = `
	CREATE TABLE IF NOT EXISTS dataframe_membership (
		frame_nid INTEGER NOT NULL,
		target_nid INTEGER NOT NULL,
		sender_nid INTEGER NOT NULL DEFAULT 0,
		membership_nid INTEGER NOT NULL DEFAULT 1,
		event_nid INTEGER NOT NULL DEFAULT 0,
		target_local BOOLEAN NOT NULL DEFAULT false,
		forgotten BOOLEAN NOT NULL DEFAULT false,
		UNIQUE (frame_nid, target_nid)
	);
`

var selectJoinedUsersSetForFramesAndUserSQL = "" +
	"SELECT target_nid, COUNT(frame_nid) FROM dataframe_membership" +
	" WHERE (target_local OR $1 = false)" +
	" AND frame_nid IN ($2) AND target_nid IN ($3)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

var selectJoinedUsersSetForFramesSQL = "" +
	"SELECT target_nid, COUNT(frame_nid) FROM dataframe_membership" +
	" WHERE (target_local OR $1 = false)" +
	" AND frame_nid IN ($2)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

// Insert a row in to membership table so that it can be locked by the
// SELECT FOR UPDATE
const insertMembershipSQL = "" +
	"INSERT INTO dataframe_membership (frame_nid, target_nid, target_local)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectMembershipFromFrameAndTargetSQL = "" +
	"SELECT membership_nid, event_nid, forgotten FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND event_nid != 0 AND target_nid = $2"

const selectMembershipsFromFrameAndMembershipSQL = "" +
	"SELECT event_nid FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND event_nid != 0 AND membership_nid = $2 and forgotten = false"

const selectLocalMembershipsFromFrameAndMembershipSQL = "" +
	"SELECT event_nid FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND event_nid != 0 AND membership_nid = $2" +
	" AND target_local = true and forgotten = false"

const selectMembershipsFromFrameSQL = "" +
	"SELECT event_nid FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND event_nid != 0 and forgotten = false"

const selectLocalMembershipsFromFrameSQL = "" +
	"SELECT event_nid FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND event_nid != 0" +
	" AND target_local = true and forgotten = false"

const selectMembershipForUpdateSQL = "" +
	"SELECT membership_nid FROM dataframe_membership" +
	" WHERE frame_nid = $1 AND target_nid = $2"

const updateMembershipSQL = "" +
	"UPDATE dataframe_membership SET sender_nid = $1, membership_nid = $2, event_nid = $3, forgotten = $4" +
	" WHERE frame_nid = $5 AND target_nid = $6"

const updateMembershipForgetFrame = "" +
	"UPDATE dataframe_membership SET forgotten = $1" +
	" WHERE frame_nid = $2 AND target_nid = $3"

const selectFramesWithMembershipSQL = "" +
	"SELECT frame_nid FROM dataframe_membership WHERE membership_nid = $1 AND target_nid = $2 and forgotten = false"

// selectKnownUsersSQL uses a sub-select statement here to find frames that the user is
// joined to. Since this information is used to populate the user directory, we will
// only return users that the user would ordinarily be able to see anyway.
var selectKnownUsersSQL = "" +
	"SELECT DISTINCT event_state_key FROM dataframe_membership INNER JOIN dataframe_event_state_keys ON " +
	"dataframe_membership.target_nid = dataframe_event_state_keys.event_state_key_nid" +
	" WHERE frame_nid IN (" +
	"  SELECT DISTINCT frame_nid FROM dataframe_membership WHERE target_nid=$1 AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	") AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) + " AND event_state_key LIKE $2 LIMIT $3"

// selectLocalServerInFrameSQL is an optimised case for checking if we, the local server,
// are in the frame by using the target_local column of the membership table. Normally when
// we want to know if a server is in a frame, we have to unmarshal the entire frame state which
// is expensive. The presence of a single row from this query suggests we're still in the
// frame, no rows returned suggests we aren't.
const selectLocalServerInFrameSQL = "" +
	"SELECT frame_nid FROM dataframe_membership WHERE target_local = 1 AND membership_nid = $1 AND frame_nid = $2 LIMIT 1"

// selectServerMembersInFrameSQL is an optimised case for checking for server members in a frame.
// The JOIN is significantly leaner than the previous case of looking up event NIDs and reading the
// membership events from the database, as the JOIN query amounts to little more than two index
// scans which are very fast. The presence of a single row from this query suggests the server is
// in the frame, no rows returned suggests they aren't.
const selectServerInFrameSQL = "" +
	"SELECT frame_nid FROM dataframe_membership" +
	" JOIN dataframe_event_state_keys ON dataframe_membership.target_nid = dataframe_event_state_keys.event_state_key_nid" +
	" WHERE membership_nid = $1 AND frame_nid = $2 AND event_state_key LIKE '%:' || $3 LIMIT 1"

const deleteMembershipSQL = "" +
	"DELETE FROM dataframe_membership WHERE frame_nid = $1 AND target_nid = $2"

const selectJoinedUsersSQL = `
SELECT DISTINCT target_nid
FROM dataframe_membership m
WHERE membership_nid > $1 AND target_nid IN ($2)
`

type membershipStatements struct {
	db                                              *sql.DB
	insertMembershipStmt                            *sql.Stmt
	selectMembershipForUpdateStmt                   *sql.Stmt
	selectMembershipFromFrameAndTargetStmt           *sql.Stmt
	selectMembershipsFromFrameAndMembershipStmt      *sql.Stmt
	selectLocalMembershipsFromFrameAndMembershipStmt *sql.Stmt
	selectMembershipsFromFrameStmt                   *sql.Stmt
	selectLocalMembershipsFromFrameStmt              *sql.Stmt
	selectFramesWithMembershipStmt                   *sql.Stmt
	updateMembershipStmt                            *sql.Stmt
	selectKnownUsersStmt                            *sql.Stmt
	updateMembershipForgetFrameStmt                  *sql.Stmt
	selectLocalServerInFrameStmt                     *sql.Stmt
	selectServerInFrameStmt                          *sql.Stmt
	deleteMembershipStmt                            *sql.Stmt
	// selectJoinedUsersStmt                           *sql.Stmt // Prepared at runtime
}

func CreateMembershipTable(db *sql.DB) error {
	_, err := db.Exec(membershipSchema)
	if err != nil {
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "dataframe: add forgotten column",
		Up:      deltas.UpAddForgottenColumn,
	})
	return m.Up(context.Background())
}

func PrepareMembershipTable(db *sql.DB) (tables.Membership, error) {
	s := &membershipStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertMembershipStmt, insertMembershipSQL},
		{&s.selectMembershipForUpdateStmt, selectMembershipForUpdateSQL},
		{&s.selectMembershipFromFrameAndTargetStmt, selectMembershipFromFrameAndTargetSQL},
		{&s.selectMembershipsFromFrameAndMembershipStmt, selectMembershipsFromFrameAndMembershipSQL},
		{&s.selectLocalMembershipsFromFrameAndMembershipStmt, selectLocalMembershipsFromFrameAndMembershipSQL},
		{&s.selectMembershipsFromFrameStmt, selectMembershipsFromFrameSQL},
		{&s.selectLocalMembershipsFromFrameStmt, selectLocalMembershipsFromFrameSQL},
		{&s.updateMembershipStmt, updateMembershipSQL},
		{&s.selectFramesWithMembershipStmt, selectFramesWithMembershipSQL},
		{&s.selectKnownUsersStmt, selectKnownUsersSQL},
		{&s.updateMembershipForgetFrameStmt, updateMembershipForgetFrame},
		{&s.selectLocalServerInFrameStmt, selectLocalServerInFrameSQL},
		{&s.selectServerInFrameStmt, selectServerInFrameSQL},
		{&s.deleteMembershipStmt, deleteMembershipSQL},
	}.Prepare(db)
}

func (s *membershipStatements) InsertMembership(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
	localTarget bool,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertMembershipStmt)
	_, err := stmt.ExecContext(ctx, frameNID, targetUserNID, localTarget)
	return err
}

func (s *membershipStatements) SelectMembershipForUpdate(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
) (membership tables.MembershipState, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipForUpdateStmt)
	err = stmt.QueryRowContext(
		ctx, frameNID, targetUserNID,
	).Scan(&membership)
	return
}

func (s *membershipStatements) SelectMembershipFromFrameAndTarget(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
) (eventNID types.EventNID, membership tables.MembershipState, forgotten bool, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipFromFrameAndTargetStmt)
	err = stmt.QueryRowContext(
		ctx, frameNID, targetUserNID,
	).Scan(&membership, &eventNID, &forgotten)
	return
}

func (s *membershipStatements) SelectMembershipsFromFrame(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	var selectStmt *sql.Stmt
	if localOnly {
		selectStmt = s.selectLocalMembershipsFromFrameStmt
	} else {
		selectStmt = s.selectMembershipsFromFrameStmt
	}
	selectStmt = sqlutil.TxStmt(txn, selectStmt)
	rows, err := selectStmt.QueryContext(ctx, frameNID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectMembershipsFromFrame: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	return
}

func (s *membershipStatements) SelectMembershipsFromFrameAndMembership(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, membership tables.MembershipState, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	var stmt *sql.Stmt
	if localOnly {
		stmt = s.selectLocalMembershipsFromFrameAndMembershipStmt
	} else {
		stmt = s.selectMembershipsFromFrameAndMembershipStmt
	}
	stmt = sqlutil.TxStmt(txn, stmt)
	rows, err := stmt.QueryContext(ctx, frameNID, membership)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectMembershipsFromFrameAndMembership: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	return
}

func (s *membershipStatements) UpdateMembership(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership tables.MembershipState,
	eventNID types.EventNID, forgotten bool,
) (bool, error) {
	stmt := sqlutil.TxStmt(txn, s.updateMembershipStmt)
	res, err := stmt.ExecContext(
		ctx, senderUserNID, membership, eventNID, forgotten, frameNID, targetUserNID,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (s *membershipStatements) SelectFramesWithMembership(
	ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, membershipState tables.MembershipState,
) ([]types.FrameNID, error) {
	stmt := sqlutil.TxStmt(txn, s.selectFramesWithMembershipStmt)
	rows, err := stmt.QueryContext(ctx, membershipState, userID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectFramesWithMembership: rows.close() failed")
	var frameNIDs []types.FrameNID
	var frameNID types.FrameNID
	for rows.Next() {
		if err := rows.Scan(&frameNID); err != nil {
			return nil, err
		}
		frameNIDs = append(frameNIDs, frameNID)
	}
	return frameNIDs, nil
}

func (s *membershipStatements) SelectJoinedUsersSetForFrames(ctx context.Context, txn *sql.Tx, frameNIDs []types.FrameNID, userNIDs []types.EventStateKeyNID, localOnly bool) (map[types.EventStateKeyNID]int, error) {
	params := make([]interface{}, 0, 1+len(frameNIDs)+len(userNIDs))
	params = append(params, localOnly)
	for _, v := range frameNIDs {
		params = append(params, v)
	}
	for _, v := range userNIDs {
		params = append(params, v)
	}

	query := strings.Replace(selectJoinedUsersSetForFramesSQL, "($2)", sqlutil.QueryVariadicOffset(len(frameNIDs), 1), 1)
	if len(userNIDs) > 0 {
		query = strings.Replace(selectJoinedUsersSetForFramesAndUserSQL, "($2)", sqlutil.QueryVariadicOffset(len(frameNIDs), 1), 1)
		query = strings.Replace(query, "($3)", sqlutil.QueryVariadicOffset(len(userNIDs), len(frameNIDs)+1), 1)
	}
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, query, params...)
	} else {
		rows, err = s.db.QueryContext(ctx, query, params...)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsersSetForFrames: rows.close() failed")
	result := make(map[types.EventStateKeyNID]int)
	var userID types.EventStateKeyNID
	var count int
	for rows.Next() {
		if err := rows.Scan(&userID, &count); err != nil {
			return nil, err
		}
		result[userID] = count
	}
	return result, rows.Err()
}

func (s *membershipStatements) SelectKnownUsers(ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, searchString string, limit int) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectKnownUsersStmt)
	rows, err := stmt.QueryContext(ctx, userID, fmt.Sprintf("%%%s%%", searchString), limit)
	if err != nil {
		return nil, err
	}
	result := []string{}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectKnownUsers: rows.close() failed")
	var resUserID string
	for rows.Next() {
		if err := rows.Scan(&resUserID); err != nil {
			return nil, err
		}
		result = append(result, resUserID)
	}
	return result, rows.Err()
}

func (s *membershipStatements) UpdateForgetMembership(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
	forget bool,
) error {
	_, err := sqlutil.TxStmt(txn, s.updateMembershipForgetFrameStmt).ExecContext(
		ctx, forget, frameNID, targetUserNID,
	)
	return err
}

func (s *membershipStatements) SelectLocalServerInFrame(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID) (bool, error) {
	var nid types.FrameNID
	stmt := sqlutil.TxStmt(txn, s.selectLocalServerInFrameStmt)
	err := stmt.QueryRowContext(ctx, tables.MembershipStateJoin, frameNID).Scan(&nid)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	found := nid > 0
	return found, nil
}

func (s *membershipStatements) SelectServerInFrame(ctx context.Context, txn *sql.Tx, frameNID types.FrameNID, serverName spec.ServerName) (bool, error) {
	var nid types.FrameNID
	stmt := sqlutil.TxStmt(txn, s.selectServerInFrameStmt)
	err := stmt.QueryRowContext(ctx, tables.MembershipStateJoin, frameNID, serverName).Scan(&nid)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return frameNID == nid, nil
}

func (s *membershipStatements) DeleteMembership(
	ctx context.Context, txn *sql.Tx,
	frameNID types.FrameNID, targetUserNID types.EventStateKeyNID,
) error {
	_, err := sqlutil.TxStmt(txn, s.deleteMembershipStmt).ExecContext(
		ctx, frameNID, targetUserNID,
	)
	return err
}

func (s *membershipStatements) SelectJoinedUsers(
	ctx context.Context, txn *sql.Tx,
	targetUserNIDs []types.EventStateKeyNID,
) ([]types.EventStateKeyNID, error) {
	result := make([]types.EventStateKeyNID, 0, len(targetUserNIDs))

	qry := strings.Replace(selectJoinedUsersSQL, "($2)", sqlutil.QueryVariadicOffset(len(targetUserNIDs), 1), 1)

	stmt, err := s.db.Prepare(qry)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, stmt, "SelectJoinedUsers: stmt.Close failed")

	params := make([]any, len(targetUserNIDs)+1)
	params[0] = tables.MembershipStateLeaveOrBan
	for i := range targetUserNIDs {
		params[i+1] = targetUserNIDs[i]
	}

	stmt = sqlutil.TxStmt(txn, stmt)
	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedUsers: rows.close() failed")
	var targetNID types.EventStateKeyNID
	for rows.Next() {
		if err = rows.Scan(&targetNID); err != nil {
			return nil, err
		}
		result = append(result, targetNID)
	}

	return result, rows.Err()
}
