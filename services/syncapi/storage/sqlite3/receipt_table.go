package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/syncapi/storage/sqlite3/deltas"
	"github.com/withqb/coddy/services/syncapi/storage/tables"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/xtools/spec"
)

const receiptsSchema = `
-- Stores data about receipts
CREATE TABLE IF NOT EXISTS syncapi_receipts (
	-- The ID
	id BIGINT,
	frame_id TEXT NOT NULL,
	receipt_type TEXT NOT NULL,
	user_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	receipt_ts BIGINT NOT NULL,
	CONSTRAINT syncapi_receipts_unique UNIQUE (frame_id, receipt_type, user_id)
);
CREATE INDEX IF NOT EXISTS syncapi_receipts_frame_id_idx ON syncapi_receipts(frame_id);
`

const upsertReceipt = "" +
	"INSERT INTO syncapi_receipts" +
	" (id, frame_id, receipt_type, user_id, event_id, receipt_ts)" +
	" VALUES ($1, $2, $3, $4, $5, $6)" +
	" ON CONFLICT (frame_id, receipt_type, user_id)" +
	" DO UPDATE SET id = $7, event_id = $8, receipt_ts = $9"

const selectFrameReceipts = "" +
	"SELECT id, frame_id, receipt_type, user_id, event_id, receipt_ts" +
	" FROM syncapi_receipts" +
	" WHERE id > $1 and frame_id in ($2)"

const selectMaxReceiptIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_receipts"

const purgeReceiptsSQL = "" +
	"DELETE FROM syncapi_receipts WHERE frame_id = $1"

type receiptStatements struct {
	db                 *sql.DB
	streamIDStatements *StreamIDStatements
	upsertReceipt      *sql.Stmt
	selectFrameReceipts *sql.Stmt
	selectMaxReceiptID *sql.Stmt
	purgeReceiptsStmt  *sql.Stmt
}

func NewSqliteReceiptsTable(db *sql.DB, streamID *StreamIDStatements) (tables.Receipts, error) {
	_, err := db.Exec(receiptsSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "syncapi: fix sequences",
		Up:      deltas.UpFixSequences,
	})
	err = m.Up(context.Background())
	if err != nil {
		return nil, err
	}
	r := &receiptStatements{
		db:                 db,
		streamIDStatements: streamID,
	}
	return r, sqlutil.StatementList{
		{&r.upsertReceipt, upsertReceipt},
		{&r.selectFrameReceipts, selectFrameReceipts},
		{&r.selectMaxReceiptID, selectMaxReceiptIDSQL},
		{&r.purgeReceiptsStmt, purgeReceiptsSQL},
	}.Prepare(db)
}

// UpsertReceipt creates new user receipts
func (r *receiptStatements) UpsertReceipt(ctx context.Context, txn *sql.Tx, frameId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	pos, err = r.streamIDStatements.nextReceiptID(ctx, txn)
	if err != nil {
		return
	}
	stmt := sqlutil.TxStmt(txn, r.upsertReceipt)
	_, err = stmt.ExecContext(ctx, pos, frameId, receiptType, userId, eventId, timestamp, pos, eventId, timestamp)
	return
}

// SelectFrameReceiptsAfter select all receipts for a given frame after a specific timestamp
func (r *receiptStatements) SelectFrameReceiptsAfter(ctx context.Context, txn *sql.Tx, frameIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	selectSQL := strings.Replace(selectFrameReceipts, "($2)", sqlutil.QueryVariadicOffset(len(frameIDs), 1), 1)
	var lastPos types.StreamPosition
	params := make([]interface{}, len(frameIDs)+1)
	params[0] = streamPos
	for k, v := range frameIDs {
		params[k+1] = v
	}
	prep, err := r.db.Prepare(selectSQL)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to prepare statement: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, prep, "SelectFrameReceiptsAfter: prep.close() failed")
	rows, err := sqlutil.TxStmt(txn, prep).QueryContext(ctx, params...)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to query frame receipts: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectFrameReceiptsAfter: rows.close() failed")
	var res []types.OutputReceiptEvent
	for rows.Next() {
		r := types.OutputReceiptEvent{}
		var id types.StreamPosition
		err = rows.Scan(&id, &r.FrameID, &r.Type, &r.UserID, &r.EventID, &r.Timestamp)
		if err != nil {
			return 0, res, fmt.Errorf("unable to scan row to api.Receipts: %w", err)
		}
		res = append(res, r)
		if id > lastPos {
			lastPos = id
		}
	}
	return lastPos, res, rows.Err()
}

func (s *receiptStatements) SelectMaxReceiptID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	var nullableID sql.NullInt64
	stmt := sqlutil.TxStmt(txn, s.selectMaxReceiptID)
	err = stmt.QueryRowContext(ctx).Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

func (s *receiptStatements) PurgeReceipts(
	ctx context.Context, txn *sql.Tx, frameID string,
) error {
	_, err := sqlutil.TxStmt(txn, s.purgeReceiptsStmt).ExecContext(ctx, frameID)
	return err
}
