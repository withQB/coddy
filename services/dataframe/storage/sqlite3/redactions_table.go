package sqlite3

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/storage/tables"
)

const redactionsSchema = `
-- Stores information about the redacted state of events.
-- We need to track redactions rather than blindly updating the event JSON table on receipt of a redaction
-- because we might receive the redaction BEFORE we receive the event which it redacts (think backfill).
CREATE TABLE IF NOT EXISTS dataframe_redactions (
    redaction_event_id TEXT PRIMARY KEY,
	redacts_event_id TEXT NOT NULL,
	-- Initially FALSE, set to TRUE when the redaction has been validated according to frames v3+ spec
	validated BOOLEAN NOT NULL
);
`

const insertRedactionSQL = "" +
	"INSERT OR IGNORE INTO dataframe_redactions (redaction_event_id, redacts_event_id, validated)" +
	" VALUES ($1, $2, $3)"

const selectRedactionInfoByRedactionEventIDSQL = "" +
	"SELECT redaction_event_id, redacts_event_id, validated FROM dataframe_redactions" +
	" WHERE redaction_event_id = $1"

const selectRedactionInfoByEventBeingRedactedSQL = "" +
	"SELECT redaction_event_id, redacts_event_id, validated FROM dataframe_redactions" +
	" WHERE redacts_event_id = $1"

const markRedactionValidatedSQL = "" +
	" UPDATE dataframe_redactions SET validated = $1 WHERE redaction_event_id = $2"

type redactionStatements struct {
	db                                          *sql.DB
	insertRedactionStmt                         *sql.Stmt
	selectRedactionInfoByRedactionEventIDStmt   *sql.Stmt
	selectRedactionInfoByEventBeingRedactedStmt *sql.Stmt
	markRedactionValidatedStmt                  *sql.Stmt
}

func CreateRedactionsTable(db *sql.DB) error {
	_, err := db.Exec(redactionsSchema)
	return err
}

func PrepareRedactionsTable(db *sql.DB) (tables.Redactions, error) {
	s := &redactionStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertRedactionStmt, insertRedactionSQL},
		{&s.selectRedactionInfoByRedactionEventIDStmt, selectRedactionInfoByRedactionEventIDSQL},
		{&s.selectRedactionInfoByEventBeingRedactedStmt, selectRedactionInfoByEventBeingRedactedSQL},
		{&s.markRedactionValidatedStmt, markRedactionValidatedSQL},
	}.Prepare(db)
}

func (s *redactionStatements) InsertRedaction(
	ctx context.Context, txn *sql.Tx, info tables.RedactionInfo,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertRedactionStmt)
	_, err := stmt.ExecContext(ctx, info.RedactionEventID, info.RedactsEventID, info.Validated)
	return err
}

func (s *redactionStatements) SelectRedactionInfoByRedactionEventID(
	ctx context.Context, txn *sql.Tx, redactionEventID string,
) (info *tables.RedactionInfo, err error) {
	info = &tables.RedactionInfo{}
	stmt := sqlutil.TxStmt(txn, s.selectRedactionInfoByRedactionEventIDStmt)
	err = stmt.QueryRowContext(ctx, redactionEventID).Scan(
		&info.RedactionEventID, &info.RedactsEventID, &info.Validated,
	)
	if err == sql.ErrNoRows {
		info = nil
		err = nil
	}
	return
}

func (s *redactionStatements) SelectRedactionInfoByEventBeingRedacted(
	ctx context.Context, txn *sql.Tx, eventID string,
) (info *tables.RedactionInfo, err error) {
	info = &tables.RedactionInfo{}
	stmt := sqlutil.TxStmt(txn, s.selectRedactionInfoByEventBeingRedactedStmt)
	err = stmt.QueryRowContext(ctx, eventID).Scan(
		&info.RedactionEventID, &info.RedactsEventID, &info.Validated,
	)
	if err == sql.ErrNoRows {
		info = nil
		err = nil
	}
	return
}

func (s *redactionStatements) MarkRedactionValidated(
	ctx context.Context, txn *sql.Tx, redactionEventID string, validated bool,
) error {
	stmt := sqlutil.TxStmt(txn, s.markRedactionValidatedStmt)
	_, err := stmt.ExecContext(ctx, validated, redactionEventID)
	return err
}
