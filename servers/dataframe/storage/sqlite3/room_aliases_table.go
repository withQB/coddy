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

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/storage/tables"
)

const frameAliasesSchema = `
  CREATE TABLE IF NOT EXISTS dataframe_frame_aliases (
    alias TEXT NOT NULL PRIMARY KEY,
    frame_id TEXT NOT NULL,
    creator_id TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS dataframe_frame_id_idx ON dataframe_frame_aliases(frame_id);
`

const insertFrameAliasSQL = `
	INSERT INTO dataframe_frame_aliases (alias, frame_id, creator_id) VALUES ($1, $2, $3)
`

const selectFrameIDFromAliasSQL = `
	SELECT frame_id FROM dataframe_frame_aliases WHERE alias = $1
`

const selectAliasesFromFrameIDSQL = `
	SELECT alias FROM dataframe_frame_aliases WHERE frame_id = $1
`

const selectCreatorIDFromAliasSQL = `
	SELECT creator_id FROM dataframe_frame_aliases WHERE alias = $1
`

const deleteFrameAliasSQL = `
	DELETE FROM dataframe_frame_aliases WHERE alias = $1
`

type frameAliasesStatements struct {
	db                           *sql.DB
	insertFrameAliasStmt          *sql.Stmt
	selectFrameIDFromAliasStmt    *sql.Stmt
	selectAliasesFromFrameIDStmt  *sql.Stmt
	selectCreatorIDFromAliasStmt *sql.Stmt
	deleteFrameAliasStmt          *sql.Stmt
}

func CreateFrameAliasesTable(db *sql.DB) error {
	_, err := db.Exec(frameAliasesSchema)
	return err
}

func PrepareFrameAliasesTable(db *sql.DB) (tables.FrameAliases, error) {
	s := &frameAliasesStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertFrameAliasStmt, insertFrameAliasSQL},
		{&s.selectFrameIDFromAliasStmt, selectFrameIDFromAliasSQL},
		{&s.selectAliasesFromFrameIDStmt, selectAliasesFromFrameIDSQL},
		{&s.selectCreatorIDFromAliasStmt, selectCreatorIDFromAliasSQL},
		{&s.deleteFrameAliasStmt, deleteFrameAliasSQL},
	}.Prepare(db)
}

func (s *frameAliasesStatements) InsertFrameAlias(
	ctx context.Context, txn *sql.Tx, alias string, frameID string, creatorUserID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertFrameAliasStmt)
	_, err := stmt.ExecContext(ctx, alias, frameID, creatorUserID)
	return err
}

func (s *frameAliasesStatements) SelectFrameIDFromAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) (frameID string, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectFrameIDFromAliasStmt)
	err = stmt.QueryRowContext(ctx, alias).Scan(&frameID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return
}

func (s *frameAliasesStatements) SelectAliasesFromFrameID(
	ctx context.Context, txn *sql.Tx, frameID string,
) (aliases []string, err error) {
	aliases = []string{}
	stmt := sqlutil.TxStmt(txn, s.selectAliasesFromFrameIDStmt)
	rows, err := stmt.QueryContext(ctx, frameID)
	if err != nil {
		return
	}

	defer internal.CloseAndLogIfError(ctx, rows, "selectAliasesFromFrameID: rows.close() failed")

	var alias string
	for rows.Next() {
		if err = rows.Scan(&alias); err != nil {
			return
		}

		aliases = append(aliases, alias)
	}

	return
}

func (s *frameAliasesStatements) SelectCreatorIDFromAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) (creatorID string, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectCreatorIDFromAliasStmt)
	err = stmt.QueryRowContext(ctx, alias).Scan(&creatorID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return
}

func (s *frameAliasesStatements) DeleteFrameAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteFrameAliasStmt)
	_, err := stmt.ExecContext(ctx, alias)
	return err
}
