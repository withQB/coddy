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
	"errors"
	"fmt"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	rstypes "github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/syncapi/storage/sqlite3/deltas"
	"github.com/withqb/coddy/services/syncapi/storage/tables"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

const currentFrameStateSchema = `
-- Stores the current frame state for every frame.
CREATE TABLE IF NOT EXISTS syncapi_current_frame_state (
    frame_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    type TEXT NOT NULL,
    sender TEXT NOT NULL,
    contains_url BOOL NOT NULL DEFAULT false,
    state_key TEXT NOT NULL,
    headered_event_json TEXT NOT NULL,
    membership TEXT,
    added_at BIGINT,
    history_visibility SMALLINT NOT NULL DEFAULT 2, -- The history visibility before this event (1 - world_readable; 2 - shared; 3 - invited; 4 - joined)
    UNIQUE (frame_id, type, state_key)
);
-- for event deletion
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_id_idx ON syncapi_current_frame_state(event_id, frame_id, type, sender, contains_url);
-- for querying membership states of users
-- CREATE INDEX IF NOT EXISTS syncapi_membership_idx ON syncapi_current_frame_state(type, state_key, membership) WHERE membership IS NOT NULL AND membership != 'leave';
-- for querying state by event IDs
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_current_frame_state_eventid_idx ON syncapi_current_frame_state(event_id);
-- for improving selectFrameIDsWithAnyMembershipSQL
CREATE INDEX IF NOT EXISTS syncapi_current_frame_state_type_state_key_idx ON syncapi_current_frame_state(type, state_key);
`

const upsertFrameStateSQL = "" +
	"INSERT INTO syncapi_current_frame_state (frame_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, added_at, history_visibility)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" +
	" ON CONFLICT (frame_id, type, state_key)" +
	" DO UPDATE SET event_id = $2, sender=$4, contains_url=$5, headered_event_json = $7, membership = $8, added_at = $9"

const deleteFrameStateByEventIDSQL = "" +
	"DELETE FROM syncapi_current_frame_state WHERE event_id = $1"

const deleteFrameStateForFrameSQL = "" +
	"DELETE FROM syncapi_current_frame_state WHERE frame_id = $1"

const selectFrameIDsWithMembershipSQL = "" +
	"SELECT DISTINCT frame_id FROM syncapi_current_frame_state WHERE type = 'm.frame.member' AND state_key = $1 AND membership = $2"

const selectFrameIDsWithAnyMembershipSQL = "" +
	"SELECT frame_id, membership FROM syncapi_current_frame_state WHERE type = 'm.frame.member' AND state_key = $1"

const selectCurrentStateSQL = "" +
	"SELECT event_id, headered_event_json FROM syncapi_current_frame_state WHERE frame_id = $1"

// WHEN, ORDER BY and LIMIT will be added by prepareWithFilter

const selectJoinedUsersSQL = "" +
	"SELECT frame_id, state_key FROM syncapi_current_frame_state WHERE type = 'm.frame.member' AND membership = 'join'"

const selectJoinedUsersInFrameSQL = "" +
	"SELECT frame_id, state_key FROM syncapi_current_frame_state WHERE type = 'm.frame.member' AND membership = 'join' AND frame_id IN ($1)"

const selectStateEventSQL = "" +
	"SELECT headered_event_json FROM syncapi_current_frame_state WHERE frame_id = $1 AND type = $2 AND state_key = $3"

const selectEventsWithEventIDsSQL = "" +
	"SELECT event_id, added_at, headered_event_json, history_visibility FROM syncapi_current_frame_state WHERE event_id IN ($1)"

const selectSharedUsersSQL = "" +
	"SELECT state_key FROM syncapi_current_frame_state WHERE frame_id IN(" +
	"	SELECT DISTINCT frame_id FROM syncapi_current_frame_state WHERE state_key = $1 AND membership='join'" +
	") AND type = 'm.frame.member' AND state_key IN ($2) AND membership IN ('join', 'invite');"

const selectMembershipCount = `SELECT count(*) FROM syncapi_current_frame_state WHERE type = 'm.frame.member' AND frame_id = $1 AND membership = $2`

const selectFrameHeroes = `
SELECT state_key FROM syncapi_current_frame_state
WHERE type = 'm.frame.member' AND frame_id = $1 AND state_key != $2 AND membership IN ($3)
ORDER BY added_at, state_key
LIMIT 5
`

type currentFrameStateStatements struct {
	db                                 *sql.DB
	streamIDStatements                 *StreamIDStatements
	upsertFrameStateStmt                *sql.Stmt
	deleteFrameStateByEventIDStmt       *sql.Stmt
	deleteFrameStateForFrameStmt         *sql.Stmt
	selectFrameIDsWithMembershipStmt    *sql.Stmt
	selectFrameIDsWithAnyMembershipStmt *sql.Stmt
	selectJoinedUsersStmt              *sql.Stmt
	//selectJoinedUsersInFrameStmt      *sql.Stmt - prepared at runtime due to variadic
	selectStateEventStmt *sql.Stmt
	//selectSharedUsersSQL             *sql.Stmt - prepared at runtime due to variadic
	selectMembershipCountStmt *sql.Stmt
	//selectFrameHeroes          *sql.Stmt - prepared at runtime due to variadic
}

func NewSqliteCurrentFrameStateTable(db *sql.DB, streamID *StreamIDStatements) (tables.CurrentFrameState, error) {
	s := &currentFrameStateStatements{
		db:                 db,
		streamIDStatements: streamID,
	}
	_, err := db.Exec(currentFrameStateSchema)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "syncapi: add history visibility column (current_frame_state)",
		Up:      deltas.UpAddHistoryVisibilityColumnCurrentFrameState,
	})
	err = m.Up(context.Background())
	if err != nil {
		return nil, err
	}

	return s, sqlutil.StatementList{
		{&s.upsertFrameStateStmt, upsertFrameStateSQL},
		{&s.deleteFrameStateByEventIDStmt, deleteFrameStateByEventIDSQL},
		{&s.deleteFrameStateForFrameStmt, deleteFrameStateForFrameSQL},
		{&s.selectFrameIDsWithMembershipStmt, selectFrameIDsWithMembershipSQL},
		{&s.selectFrameIDsWithAnyMembershipStmt, selectFrameIDsWithAnyMembershipSQL},
		{&s.selectJoinedUsersStmt, selectJoinedUsersSQL},
		{&s.selectStateEventStmt, selectStateEventSQL},
		{&s.selectMembershipCountStmt, selectMembershipCount},
	}.Prepare(db)
}

// SelectJoinedUsers returns a map of frame ID to a list of joined user IDs.
func (s *currentFrameStateStatements) SelectJoinedUsers(
	ctx context.Context, txn *sql.Tx,
) (map[string][]string, error) {
	rows, err := sqlutil.TxStmt(txn, s.selectJoinedUsersStmt).QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsers: rows.close() failed")

	result := make(map[string][]string)
	var frameID string
	var userID string
	for rows.Next() {
		if err := rows.Scan(&frameID, &userID); err != nil {
			return nil, err
		}
		users := result[frameID]
		users = append(users, userID)
		result[frameID] = users
	}
	return result, nil
}

// SelectJoinedUsersInFrame returns a map of frame ID to a list of joined user IDs for a given frame.
func (s *currentFrameStateStatements) SelectJoinedUsersInFrame(
	ctx context.Context, txn *sql.Tx, frameIDs []string,
) (map[string][]string, error) {
	query := strings.Replace(selectJoinedUsersInFrameSQL, "($1)", sqlutil.QueryVariadic(len(frameIDs)), 1)
	params := make([]interface{}, 0, len(frameIDs))
	for _, frameID := range frameIDs {
		params = append(params, frameID)
	}
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("SelectJoinedUsersInFrame s.db.Prepare: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, stmt, "SelectJoinedUsersInFrame: stmt.close() failed")

	rows, err := sqlutil.TxStmt(txn, stmt).QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectJoinedUsersInFrame: rows.close() failed")

	result := make(map[string][]string)
	var userID, frameID string
	for rows.Next() {
		if err := rows.Scan(&frameID, &userID); err != nil {
			return nil, err
		}
		users := result[frameID]
		users = append(users, userID)
		result[frameID] = users
	}
	return result, rows.Err()
}

// SelectFrameIDsWithMembership returns the list of frame IDs which have the given user in the given membership state.
func (s *currentFrameStateStatements) SelectFrameIDsWithMembership(
	ctx context.Context,
	txn *sql.Tx,
	userID string,
	membership string, // nolint: unparam
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectFrameIDsWithMembershipStmt)
	rows, err := stmt.QueryContext(ctx, userID, membership)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectFrameIDsWithMembership: rows.close() failed")

	var result []string
	for rows.Next() {
		var frameID string
		if err := rows.Scan(&frameID); err != nil {
			return nil, err
		}
		result = append(result, frameID)
	}
	return result, nil
}

// SelectFrameIDsWithAnyMembership returns a map of all memberships for the given user.
func (s *currentFrameStateStatements) SelectFrameIDsWithAnyMembership(
	ctx context.Context,
	txn *sql.Tx,
	userID string,
) (map[string]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectFrameIDsWithAnyMembershipStmt)
	rows, err := stmt.QueryContext(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectFrameIDsWithAnyMembership: rows.close() failed")

	result := map[string]string{}
	for rows.Next() {
		var frameID string
		var membership string
		if err := rows.Scan(&frameID, &membership); err != nil {
			return nil, err
		}
		result[frameID] = membership
	}
	return result, rows.Err()
}

// CurrentState returns all the current state events for the given frame.
func (s *currentFrameStateStatements) SelectCurrentState(
	ctx context.Context, txn *sql.Tx, frameID string,
	stateFilter *synctypes.StateFilter,
	excludeEventIDs []string,
) ([]*rstypes.HeaderedEvent, error) {
	// We're going to query members later, so remove them from this request
	if stateFilter.LazyLoadMembers && !stateFilter.IncludeRedundantMembers {
		notTypes := &[]string{spec.MFrameMember}
		if stateFilter.NotTypes != nil {
			*stateFilter.NotTypes = append(*stateFilter.NotTypes, spec.MFrameMember)
		} else {
			stateFilter.NotTypes = notTypes
		}
	}
	stmt, params, err := prepareWithFilters(
		s.db, txn, selectCurrentStateSQL,
		[]interface{}{
			frameID,
		},
		stateFilter.Senders, stateFilter.NotSenders,
		stateFilter.Types, stateFilter.NotTypes,
		excludeEventIDs, stateFilter.ContainsURL, 0,
		FilterOrderNone,
	)
	if err != nil {
		return nil, fmt.Errorf("s.prepareWithFilters: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectCurrentState: rows.close() failed")

	return rowsToEvents(rows)
}

func (s *currentFrameStateStatements) DeleteFrameStateByEventID(
	ctx context.Context, txn *sql.Tx, eventID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteFrameStateByEventIDStmt)
	_, err := stmt.ExecContext(ctx, eventID)
	return err
}

func (s *currentFrameStateStatements) DeleteFrameStateForFrame(
	ctx context.Context, txn *sql.Tx, frameID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteFrameStateForFrameStmt)
	_, err := stmt.ExecContext(ctx, frameID)
	return err
}

func (s *currentFrameStateStatements) UpsertFrameState(
	ctx context.Context, txn *sql.Tx,
	event *rstypes.HeaderedEvent, membership *string, addedAt types.StreamPosition,
) error {
	// Parse content as JSON and search for an "url" key
	containsURL := false
	var content map[string]interface{}
	if json.Unmarshal(event.Content(), &content) != nil {
		// Set containsURL to true if url is present
		_, containsURL = content["url"]
	}

	headeredJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// upsert state event
	stmt := sqlutil.TxStmt(txn, s.upsertFrameStateStmt)
	_, err = stmt.ExecContext(
		ctx,
		event.FrameID(),
		event.EventID(),
		event.Type(),
		event.UserID.String(),
		containsURL,
		*event.StateKeyResolved,
		headeredJSON,
		membership,
		addedAt,
		event.Visibility,
	)
	return err
}

func minOfInts(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (s *currentFrameStateStatements) SelectEventsWithEventIDs(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) ([]types.StreamEvent, error) {
	iEventIDs := make([]interface{}, len(eventIDs))
	for k, v := range eventIDs {
		iEventIDs[k] = v
	}
	res := make([]types.StreamEvent, 0, len(eventIDs))
	var start int
	for start < len(eventIDs) {
		n := minOfInts(len(eventIDs)-start, 999)
		query := strings.Replace(selectEventsWithEventIDsSQL, "($1)", sqlutil.QueryVariadic(n), 1)
		var rows *sql.Rows
		var err error
		if txn == nil {
			rows, err = s.db.QueryContext(ctx, query, iEventIDs[start:start+n]...)
		} else {
			rows, err = txn.QueryContext(ctx, query, iEventIDs[start:start+n]...)
		}
		if err != nil {
			return nil, err
		}
		start = start + n
		events, err := currentFrameStateRowsToStreamEvents(rows)
		internal.CloseAndLogIfError(ctx, rows, "selectEventsWithEventIDs: rows.close() failed")
		if err != nil {
			return nil, err
		}
		res = append(res, events...)
	}
	return res, nil
}

func currentFrameStateRowsToStreamEvents(rows *sql.Rows) ([]types.StreamEvent, error) {
	var events []types.StreamEvent
	for rows.Next() {
		var (
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			historyVisibility xtools.HistoryVisibility
		)
		if err := rows.Scan(&eventID, &streamPos, &eventBytes, &historyVisibility); err != nil {
			return nil, err
		}
		// TDO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		ev.Visibility = historyVisibility

		events = append(events, types.StreamEvent{
			HeaderedEvent:  &ev,
			StreamPosition: streamPos,
		})
	}

	return events, nil
}

func rowsToEvents(rows *sql.Rows) ([]*rstypes.HeaderedEvent, error) {
	result := []*rstypes.HeaderedEvent{}
	for rows.Next() {
		var eventID string
		var eventBytes []byte
		if err := rows.Scan(&eventID, &eventBytes); err != nil {
			return nil, err
		}
		// TDO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}
		result = append(result, &ev)
	}
	return result, nil
}

func (s *currentFrameStateStatements) SelectStateEvent(
	ctx context.Context, txn *sql.Tx, frameID, evType, stateKey string,
) (*rstypes.HeaderedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectStateEventStmt)
	var res []byte
	err := stmt.QueryRowContext(ctx, frameID, evType, stateKey).Scan(&res)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ev rstypes.HeaderedEvent
	if err = json.Unmarshal(res, &ev); err != nil {
		return nil, err
	}
	return &ev, err
}

func (s *currentFrameStateStatements) SelectSharedUsers(
	ctx context.Context, txn *sql.Tx, userID string, otherUserIDs []string,
) ([]string, error) {

	params := make([]interface{}, len(otherUserIDs)+1)
	params[0] = userID
	for k, v := range otherUserIDs {
		params[k+1] = v
	}

	var provider sqlutil.QueryProvider
	if txn == nil {
		provider = s.db
	} else {
		provider = txn
	}

	result := make([]string, 0, len(otherUserIDs))
	query := strings.Replace(selectSharedUsersSQL, "($2)", sqlutil.QueryVariadicOffset(len(otherUserIDs), 1), 1)
	err := sqlutil.RunLimitedVariablesQuery(
		ctx, query, provider, params, sqlutil.SQLite3MaxVariables,
		func(rows *sql.Rows) error {
			var stateKey string
			for rows.Next() {
				if err := rows.Scan(&stateKey); err != nil {
					return err
				}
				result = append(result, stateKey)
			}
			return nil
		},
	)

	return result, err
}

func (s *currentFrameStateStatements) SelectFrameHeroes(ctx context.Context, txn *sql.Tx, frameID, excludeUserID string, memberships []string) ([]string, error) {
	params := make([]interface{}, len(memberships)+2)
	params[0] = frameID
	params[1] = excludeUserID
	for k, v := range memberships {
		params[k+2] = v
	}

	query := strings.Replace(selectFrameHeroes, "($3)", sqlutil.QueryVariadicOffset(len(memberships), 2), 1)
	var stmt *sql.Stmt
	var err error
	if txn != nil {
		stmt, err = txn.Prepare(query)
	} else {
		stmt, err = s.db.Prepare(query)
	}
	if err != nil {
		return []string{}, err
	}
	defer internal.CloseAndLogIfError(ctx, stmt, "selectFrameHeroes: stmt.close() failed")

	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectFrameHeroes: rows.close() failed")

	var stateKey string
	result := make([]string, 0, 5)
	for rows.Next() {
		if err = rows.Scan(&stateKey); err != nil {
			return nil, err
		}
		result = append(result, stateKey)
	}
	return result, rows.Err()
}

func (s *currentFrameStateStatements) SelectMembershipCount(ctx context.Context, txn *sql.Tx, frameID, membership string) (count int, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipCountStmt)
	err = stmt.QueryRowContext(ctx, frameID, membership).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}
