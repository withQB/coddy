package sqlite3

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/syncapi/storage/tables"
	"github.com/withqb/coddy/services/syncapi/types"
)

const relationsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_relations (
	id BIGINT PRIMARY KEY,
	frame_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	child_event_id TEXT NOT NULL,
	child_event_type TEXT NOT NULL,
	rel_type TEXT NOT NULL,
	UNIQUE (frame_id, event_id, child_event_id, rel_type)
);
`

const insertRelationSQL = "" +
	"INSERT INTO syncapi_relations (" +
	"  id, frame_id, event_id, child_event_id, child_event_type, rel_type" +
	") VALUES ($1, $2, $3, $4, $5, $6) " +
	" ON CONFLICT DO NOTHING"

const deleteRelationSQL = "" +
	"DELETE FROM syncapi_relations WHERE frame_id = $1 AND child_event_id = $2"

const selectRelationsInRangeAscSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE frame_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id > $5 AND id <= $6" +
	" ORDER BY id ASC LIMIT $7"

const selectRelationsInRangeDescSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE frame_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id >= $5 AND id < $6" +
	" ORDER BY id DESC LIMIT $7"

const selectMaxRelationIDSQL = "" +
	"SELECT COALESCE(MAX(id), 0) FROM syncapi_relations"

type relationsStatements struct {
	streamIDStatements             *StreamIDStatements
	insertRelationStmt             *sql.Stmt
	selectRelationsInRangeAscStmt  *sql.Stmt
	selectRelationsInRangeDescStmt *sql.Stmt
	deleteRelationStmt             *sql.Stmt
	selectMaxRelationIDStmt        *sql.Stmt
}

func NewSqliteRelationsTable(db *sql.DB, streamID *StreamIDStatements) (tables.Relations, error) {
	s := &relationsStatements{
		streamIDStatements: streamID,
	}
	_, err := db.Exec(relationsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertRelationStmt, insertRelationSQL},
		{&s.selectRelationsInRangeAscStmt, selectRelationsInRangeAscSQL},
		{&s.selectRelationsInRangeDescStmt, selectRelationsInRangeDescSQL},
		{&s.deleteRelationStmt, deleteRelationSQL},
		{&s.selectMaxRelationIDStmt, selectMaxRelationIDSQL},
	}.Prepare(db)
}

func (s *relationsStatements) InsertRelation(
	ctx context.Context, txn *sql.Tx, frameID, eventID, childEventID, childEventType, relType string,
) (err error) {
	var streamPos types.StreamPosition
	if streamPos, err = s.streamIDStatements.nextRelationID(ctx, txn); err != nil {
		return
	}
	_, err = sqlutil.TxStmt(txn, s.insertRelationStmt).ExecContext(
		ctx, streamPos, frameID, eventID, childEventID, childEventType, relType,
	)
	return
}

func (s *relationsStatements) DeleteRelation(
	ctx context.Context, txn *sql.Tx, frameID, childEventID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteRelationStmt)
	_, err := stmt.ExecContext(
		ctx, frameID, childEventID,
	)
	return err
}

// SelectRelationsInRange returns a map rel_type -> []child_event_id
func (s *relationsStatements) SelectRelationsInRange(
	ctx context.Context, txn *sql.Tx, frameID, eventID, relType, eventType string,
	r types.Range, limit int,
) (map[string][]types.RelationEntry, types.StreamPosition, error) {
	var lastPos types.StreamPosition
	var stmt *sql.Stmt
	if r.Backwards {
		stmt = sqlutil.TxStmt(txn, s.selectRelationsInRangeDescStmt)
	} else {
		stmt = sqlutil.TxStmt(txn, s.selectRelationsInRangeAscStmt)
	}
	rows, err := stmt.QueryContext(ctx, frameID, eventID, relType, eventType, r.Low(), r.High(), limit)
	if err != nil {
		return nil, lastPos, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRelationsInRange: rows.close() failed")
	result := map[string][]types.RelationEntry{}
	var (
		id           types.StreamPosition
		childEventID string
		relationType string
	)
	for rows.Next() {
		if err = rows.Scan(&id, &childEventID, &relationType); err != nil {
			return nil, lastPos, err
		}
		if id > lastPos {
			lastPos = id
		}
		result[relationType] = append(result[relationType], types.RelationEntry{
			Position: id,
			EventID:  childEventID,
		})
	}
	if lastPos == 0 {
		lastPos = r.To
	}
	return result, lastPos, rows.Err()
}

func (s *relationsStatements) SelectMaxRelationID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMaxRelationIDStmt)
	err = stmt.QueryRowContext(ctx).Scan(&id)
	return
}
