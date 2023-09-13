package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/withqb/coddy/apis/userapi/storage/tables"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/xtools/spec"
)

const accountDataSchema = `
-- Stores data about accounts data.
CREATE TABLE IF NOT EXISTS userapi_account_datas (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The frame ID for this data (empty string if not specific to a frame)
    frame_id TEXT,
    -- The account data type
    type TEXT NOT NULL,
    -- The account data content
    content TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_account_datas_idx ON userapi_account_datas(localpart, server_name, frame_id, type);
`

const insertAccountDataSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, frame_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, frame_id, type) DO UPDATE SET content = $5
`

const selectAccountDataSQL = "" +
	"SELECT frame_id, type, content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

const selectAccountDataByTypeSQL = "" +
	"SELECT content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND frame_id = $3 AND type = $4"

type accountDataStatements struct {
	db                          *sql.DB
	insertAccountDataStmt       *sql.Stmt
	selectAccountDataStmt       *sql.Stmt
	selectAccountDataByTypeStmt *sql.Stmt
}

func NewSQLiteAccountDataTable(db *sql.DB) (tables.AccountDataTable, error) {
	s := &accountDataStatements{
		db: db,
	}
	_, err := db.Exec(accountDataSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertAccountDataStmt, insertAccountDataSQL},
		{&s.selectAccountDataStmt, selectAccountDataSQL},
		{&s.selectAccountDataByTypeStmt, selectAccountDataByTypeSQL},
	}.Prepare(db)
}

func (s *accountDataStatements) InsertAccountData(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	frameID, dataType string, content json.RawMessage,
) error {
	_, err := sqlutil.TxStmt(txn, s.insertAccountDataStmt).ExecContext(ctx, localpart, serverName, frameID, dataType, content)
	return err
}

func (s *accountDataStatements) SelectAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (
	/* global */ map[string]json.RawMessage,
	/* frames */ map[string]map[string]json.RawMessage,
	error,
) {
	rows, err := s.selectAccountDataStmt.QueryContext(ctx, localpart, serverName)
	if err != nil {
		return nil, nil, err
	}

	global := map[string]json.RawMessage{}
	frames := map[string]map[string]json.RawMessage{}

	for rows.Next() {
		var frameID string
		var dataType string
		var content []byte

		if err = rows.Scan(&frameID, &dataType, &content); err != nil {
			return nil, nil, err
		}

		if frameID != "" {
			if _, ok := frames[frameID]; !ok {
				frames[frameID] = map[string]json.RawMessage{}
			}
			frames[frameID][dataType] = content
		} else {
			global[dataType] = content
		}
	}

	return global, frames, nil
}

func (s *accountDataStatements) SelectAccountDataByType(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	frameID, dataType string,
) (data json.RawMessage, err error) {
	var bytes []byte
	stmt := s.selectAccountDataByTypeStmt
	if err = stmt.QueryRowContext(ctx, localpart, serverName, frameID, dataType).Scan(&bytes); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return
	}
	data = json.RawMessage(bytes)
	return
}
