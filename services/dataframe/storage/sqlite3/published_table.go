package sqlite3

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/storage/sqlite3/deltas"
	"github.com/withqb/coddy/services/dataframe/storage/tables"
)

const publishedSchema = `
-- Stores which frames are published in the frame directory
CREATE TABLE IF NOT EXISTS dataframe_published (
    -- The frame ID of the frame
    frame_id TEXT NOT NULL,
    -- The appservice ID of the frame
    appservice_id TEXT NOT NULL,
    -- The network_id of the frame
    network_id TEXT NOT NULL,
    -- Whether it is published or not
    published BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (frame_id, appservice_id, network_id)
);
`

const upsertPublishedSQL = "" +
	"INSERT INTO dataframe_published (frame_id, appservice_id, network_id, published) VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (frame_id, appservice_id, network_id) DO UPDATE SET published = $4"

const selectAllPublishedSQL = "" +
	"SELECT frame_id FROM dataframe_published WHERE published = $1 AND CASE WHEN $2 THEN 1=1 ELSE network_id = '' END ORDER BY frame_id ASC"

const selectNetworkPublishedSQL = "" +
	"SELECT frame_id FROM dataframe_published WHERE published = $1 AND network_id = $2 ORDER BY frame_id ASC"

const selectPublishedSQL = "" +
	"SELECT published FROM dataframe_published WHERE frame_id = $1"

type publishedStatements struct {
	db                         *sql.DB
	upsertPublishedStmt        *sql.Stmt
	selectAllPublishedStmt     *sql.Stmt
	selectPublishedStmt        *sql.Stmt
	selectNetworkPublishedStmt *sql.Stmt
}

func CreatePublishedTable(db *sql.DB) error {
	_, err := db.Exec(publishedSchema)
	if err != nil {
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "dataframe: published appservice",
		Up:      deltas.UpPulishedAppservice,
	})
	return m.Up(context.Background())
}

func PreparePublishedTable(db *sql.DB) (tables.Published, error) {
	s := &publishedStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.upsertPublishedStmt, upsertPublishedSQL},
		{&s.selectAllPublishedStmt, selectAllPublishedSQL},
		{&s.selectPublishedStmt, selectPublishedSQL},
		{&s.selectNetworkPublishedStmt, selectNetworkPublishedSQL},
	}.Prepare(db)
}

func (s *publishedStatements) UpsertFramePublished(
	ctx context.Context, txn *sql.Tx, frameID, appserviceID, networkID string, published bool,
) error {
	stmt := sqlutil.TxStmt(txn, s.upsertPublishedStmt)
	_, err := stmt.ExecContext(ctx, frameID, appserviceID, networkID, published)
	return err
}

func (s *publishedStatements) SelectPublishedFromFrameID(
	ctx context.Context, txn *sql.Tx, frameID string,
) (published bool, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectPublishedStmt)
	err = stmt.QueryRowContext(ctx, frameID).Scan(&published)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return
}

func (s *publishedStatements) SelectAllPublishedFrames(
	ctx context.Context, txn *sql.Tx, networkID string, published, includeAllNetworks bool,
) ([]string, error) {
	var rows *sql.Rows
	var err error
	if networkID != "" {
		stmt := sqlutil.TxStmt(txn, s.selectNetworkPublishedStmt)
		rows, err = stmt.QueryContext(ctx, published, networkID)
	} else {
		stmt := sqlutil.TxStmt(txn, s.selectAllPublishedStmt)
		rows, err = stmt.QueryContext(ctx, published, includeAllNetworks)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectAllPublishedStmt: rows.close() failed")

	var frameIDs []string
	var frameID string
	for rows.Next() {
		if err = rows.Scan(&frameID); err != nil {
			return nil, err
		}

		frameIDs = append(frameIDs, frameID)
	}
	return frameIDs, rows.Err()
}
