package sqlite3

import (
	"context"
	"database/sql"
	"strings"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/syncapi/storage/tables"
	"github.com/withqb/coddy/services/syncapi/types"
)

func NewSqliteNotificationDataTable(db *sql.DB, streamID *StreamIDStatements) (tables.NotificationData, error) {
	_, err := db.Exec(notificationDataSchema)
	if err != nil {
		return nil, err
	}
	r := &notificationDataStatements{
		streamIDStatements: streamID,
		db:                 db,
	}
	return r, sqlutil.StatementList{
		{&r.upsertFrameUnreadCounts, upsertFrameUnreadNotificationCountsSQL},
		{&r.selectMaxID, selectMaxNotificationIDSQL},
		{&r.purgeNotificationData, purgeNotificationDataSQL},
		// {&r.selectUserUnreadCountsForFrames, selectUserUnreadNotificationsForFrames}, // used at runtime
	}.Prepare(db)
}

type notificationDataStatements struct {
	db                     *sql.DB
	streamIDStatements     *StreamIDStatements
	upsertFrameUnreadCounts *sql.Stmt
	selectMaxID            *sql.Stmt
	purgeNotificationData  *sql.Stmt
	//selectUserUnreadCountsForFrames *sql.Stmt
}

const notificationDataSchema = `
CREATE TABLE IF NOT EXISTS syncapi_notification_data (
	id INTEGER PRIMARY KEY,
	user_id TEXT NOT NULL,
	frame_id TEXT NOT NULL,
	notification_count BIGINT NOT NULL DEFAULT 0,
	highlight_count BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT syncapi_notifications_unique UNIQUE (user_id, frame_id)
);`

const upsertFrameUnreadNotificationCountsSQL = `INSERT INTO syncapi_notification_data
  (user_id, frame_id, notification_count, highlight_count)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (user_id, frame_id)
  DO UPDATE SET id = $5, notification_count = $6, highlight_count = $7`

const selectUserUnreadNotificationsForFrames = `SELECT frame_id, notification_count, highlight_count
	FROM syncapi_notification_data
	WHERE user_id = $1 AND
	      frame_id IN ($2)`

const selectMaxNotificationIDSQL = `SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE MAX(id) END FROM syncapi_notification_data`

const purgeNotificationDataSQL = "" +
	"DELETE FROM syncapi_notification_data WHERE frame_id = $1"

func (r *notificationDataStatements) UpsertFrameUnreadCounts(ctx context.Context, txn *sql.Tx, userID, frameID string, notificationCount, highlightCount int) (pos types.StreamPosition, err error) {
	pos, err = r.streamIDStatements.nextNotificationID(ctx, nil)
	if err != nil {
		return
	}
	_, err = r.upsertFrameUnreadCounts.ExecContext(ctx, userID, frameID, notificationCount, highlightCount, pos, notificationCount, highlightCount)
	return
}

func (r *notificationDataStatements) SelectUserUnreadCountsForFrames(
	ctx context.Context, txn *sql.Tx, userID string, frameIDs []string,
) (map[string]*eventutil.NotificationData, error) {
	params := make([]interface{}, len(frameIDs)+1)
	params[0] = userID
	for i := range frameIDs {
		params[i+1] = frameIDs[i]
	}
	sql := strings.Replace(selectUserUnreadNotificationsForFrames, "($2)", sqlutil.QueryVariadicOffset(len(frameIDs), 1), 1)
	prep, err := r.db.PrepareContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, prep, "SelectUserUnreadCountsForFrames: prep.close() failed")
	rows, err := sqlutil.TxStmt(txn, prep).QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectUserUnreadCountsForFrames: rows.close() failed")

	frameCounts := map[string]*eventutil.NotificationData{}
	var frameID string
	var notificationCount, highlightCount int
	for rows.Next() {
		if err = rows.Scan(&frameID, &notificationCount, &highlightCount); err != nil {
			return nil, err
		}

		frameCounts[frameID] = &eventutil.NotificationData{
			FrameID:                  frameID,
			UnreadNotificationCount: notificationCount,
			UnreadHighlightCount:    highlightCount,
		}
	}
	return frameCounts, rows.Err()
}

func (r *notificationDataStatements) SelectMaxID(ctx context.Context, txn *sql.Tx) (int64, error) {
	var id int64
	err := sqlutil.TxStmt(txn, r.selectMaxID).QueryRowContext(ctx).Scan(&id)
	return id, err
}

func (s *notificationDataStatements) PurgeNotificationData(
	ctx context.Context, txn *sql.Tx, frameID string,
) error {
	_, err := sqlutil.TxStmt(txn, s.purgeNotificationData).ExecContext(ctx, frameID)
	return err
}
