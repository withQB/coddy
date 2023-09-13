package sqlite3

import (
	"context"
	"database/sql"
	"time"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/federationapi/types"
	"github.com/withqb/xtools/spec"
)

const inboundPeeksSchema = `
CREATE TABLE IF NOT EXISTS federationsender_inbound_peeks (
	frame_id TEXT NOT NULL,
	server_name TEXT NOT NULL,
	peek_id TEXT NOT NULL,
    creation_ts INTEGER NOT NULL,
    renewed_ts INTEGER NOT NULL,
    renewal_interval INTEGER NOT NULL,
	UNIQUE (frame_id, server_name, peek_id)
);
`

const insertInboundPeekSQL = "" +
	"INSERT INTO federationsender_inbound_peeks (frame_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval) VALUES ($1, $2, $3, $4, $5, $6)"

const selectInboundPeekSQL = "" +
	"SELECT frame_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE frame_id = $1 and server_name = $2 and peek_id = $3"

const selectInboundPeeksSQL = "" +
	"SELECT frame_id, server_name, peek_id, creation_ts, renewed_ts, renewal_interval FROM federationsender_inbound_peeks WHERE frame_id = $1 ORDER BY creation_ts"

const renewInboundPeekSQL = "" +
	"UPDATE federationsender_inbound_peeks SET renewed_ts=$1, renewal_interval=$2 WHERE frame_id = $3 and server_name = $4 and peek_id = $5"

const deleteInboundPeekSQL = "" +
	"DELETE FROM federationsender_inbound_peeks WHERE frame_id = $1 and server_name = $2 and peek_id = $3"

const deleteInboundPeeksSQL = "" +
	"DELETE FROM federationsender_inbound_peeks WHERE frame_id = $1"

type inboundPeeksStatements struct {
	db                     *sql.DB
	insertInboundPeekStmt  *sql.Stmt
	selectInboundPeekStmt  *sql.Stmt
	selectInboundPeeksStmt *sql.Stmt
	renewInboundPeekStmt   *sql.Stmt
	deleteInboundPeekStmt  *sql.Stmt
	deleteInboundPeeksStmt *sql.Stmt
}

func NewSQLiteInboundPeeksTable(db *sql.DB) (s *inboundPeeksStatements, err error) {
	s = &inboundPeeksStatements{
		db: db,
	}
	_, err = db.Exec(inboundPeeksSchema)
	if err != nil {
		return
	}

	return s, sqlutil.StatementList{
		{&s.insertInboundPeekStmt, insertInboundPeekSQL},
		{&s.selectInboundPeekStmt, selectInboundPeekSQL},
		{&s.selectInboundPeekStmt, selectInboundPeekSQL},
		{&s.selectInboundPeeksStmt, selectInboundPeeksSQL},
		{&s.renewInboundPeekStmt, renewInboundPeekSQL},
		{&s.deleteInboundPeeksStmt, deleteInboundPeeksSQL},
		{&s.deleteInboundPeekStmt, deleteInboundPeekSQL},
	}.Prepare(db)
}

func (s *inboundPeeksStatements) InsertInboundPeek(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName, frameID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	stmt := sqlutil.TxStmt(txn, s.insertInboundPeekStmt)
	_, err = stmt.ExecContext(ctx, frameID, serverName, peekID, nowMilli, nowMilli, renewalInterval)
	return
}

func (s *inboundPeeksStatements) RenewInboundPeek(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName, frameID, peekID string, renewalInterval int64,
) (err error) {
	nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
	_, err = sqlutil.TxStmt(txn, s.renewInboundPeekStmt).ExecContext(ctx, nowMilli, renewalInterval, frameID, serverName, peekID)
	return
}

func (s *inboundPeeksStatements) SelectInboundPeek(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName, frameID, peekID string,
) (*types.InboundPeek, error) {
	row := sqlutil.TxStmt(txn, s.selectInboundPeeksStmt).QueryRowContext(ctx, frameID)
	inboundPeek := types.InboundPeek{}
	err := row.Scan(
		&inboundPeek.FrameID,
		&inboundPeek.ServerName,
		&inboundPeek.PeekID,
		&inboundPeek.CreationTimestamp,
		&inboundPeek.RenewedTimestamp,
		&inboundPeek.RenewalInterval,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inboundPeek, nil
}

func (s *inboundPeeksStatements) SelectInboundPeeks(
	ctx context.Context, txn *sql.Tx, frameID string,
) (inboundPeeks []types.InboundPeek, err error) {
	rows, err := sqlutil.TxStmt(txn, s.selectInboundPeeksStmt).QueryContext(ctx, frameID)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectInboundPeeks: rows.close() failed")

	for rows.Next() {
		inboundPeek := types.InboundPeek{}
		if err = rows.Scan(
			&inboundPeek.FrameID,
			&inboundPeek.ServerName,
			&inboundPeek.PeekID,
			&inboundPeek.CreationTimestamp,
			&inboundPeek.RenewedTimestamp,
			&inboundPeek.RenewalInterval,
		); err != nil {
			return
		}
		inboundPeeks = append(inboundPeeks, inboundPeek)
	}

	return inboundPeeks, rows.Err()
}

func (s *inboundPeeksStatements) DeleteInboundPeek(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName, frameID, peekID string,
) (err error) {
	_, err = sqlutil.TxStmt(txn, s.deleteInboundPeekStmt).ExecContext(ctx, frameID, serverName, peekID)
	return
}

func (s *inboundPeeksStatements) DeleteInboundPeeks(
	ctx context.Context, txn *sql.Tx, frameID string,
) (err error) {
	_, err = sqlutil.TxStmt(txn, s.deleteInboundPeeksStmt).ExecContext(ctx, frameID)
	return
}
