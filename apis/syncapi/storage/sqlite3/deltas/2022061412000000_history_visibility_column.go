package deltas

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
)

func UpAddHistoryVisibilityColumnOutputFrameEvents(ctx context.Context, tx *sql.Tx) error {
	// SQLite doesn't have "if exists", so check if the column exists. If the query doesn't return an error, it already exists.
	// Required for unit tests, as otherwise a duplicate column error will show up.
	_, err := tx.QueryContext(ctx, "SELECT history_visibility FROM syncapi_output_frame_events LIMIT 1")
	if err == nil {
		return nil
	}
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE syncapi_output_frame_events ADD COLUMN history_visibility SMALLINT NOT NULL DEFAULT 2;
		UPDATE syncapi_output_frame_events SET history_visibility = 4 WHERE type IN ('m.frame.message', 'm.frame.encrypted');
	`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	return nil
}

// UpSetHistoryVisibility sets the history visibility for already stored events.
// Requires current_frame_state and output_frame_events to be created.
func UpSetHistoryVisibility(ctx context.Context, tx *sql.Tx) error {
	// get the current frame history visibilities
	historyVisibilities, err := currentHistoryVisibilities(ctx, tx)
	if err != nil {
		return err
	}

	// update the history visibility
	for frameID, hisVis := range historyVisibilities {
		_, err = tx.ExecContext(ctx, `UPDATE syncapi_output_frame_events SET history_visibility = $1 
                        WHERE type IN ('m.frame.message', 'm.frame.encrypted') AND frame_id = $2 AND history_visibility <> $1`, hisVis, frameID)
		if err != nil {
			return fmt.Errorf("failed to update history visibility: %w", err)
		}
	}

	return nil
}

func UpAddHistoryVisibilityColumnCurrentFrameState(ctx context.Context, tx *sql.Tx) error {
	// SQLite doesn't have "if exists", so check if the column exists. If the query doesn't return an error, it already exists.
	// Required for unit tests, as otherwise a duplicate column error will show up.
	_, err := tx.QueryContext(ctx, "SELECT history_visibility FROM syncapi_current_frame_state LIMIT 1")
	if err == nil {
		return nil
	}
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE syncapi_current_frame_state ADD COLUMN history_visibility SMALLINT NOT NULL DEFAULT 2;
		UPDATE syncapi_current_frame_state SET history_visibility = 4 WHERE type IN ('m.frame.message', 'm.frame.encrypted');
	`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}

	return nil
}

// currentHistoryVisibilities returns a map from frameID to current history visibility.
// If the history visibility was changed after frame creation, defaults to joined.
func currentHistoryVisibilities(ctx context.Context, tx *sql.Tx) (map[string]xtools.HistoryVisibility, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT frame_id, headered_event_json FROM syncapi_current_frame_state
		WHERE type = 'm.frame.history_visibility' AND state_key = '';
`)
	if err != nil {
		return nil, fmt.Errorf("failed to query current frame state: %w", err)
	}
	defer rows.Close() // nolint: errcheck
	var eventBytes []byte
	var frameID string
	var event types.HeaderedEvent
	var hisVis xtools.HistoryVisibility
	historyVisibilities := make(map[string]xtools.HistoryVisibility)
	for rows.Next() {
		if err = rows.Scan(&frameID, &eventBytes); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if err = json.Unmarshal(eventBytes, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}
		historyVisibilities[frameID] = xtools.HistoryVisibilityJoined
		if hisVis, err = event.HistoryVisibility(); err == nil && event.Depth() < 10 {
			historyVisibilities[frameID] = hisVis
		}
	}
	return historyVisibilities, nil
}

func DownAddHistoryVisibilityColumn(ctx context.Context, tx *sql.Tx) error {
	// SQLite doesn't have "if exists", so check if the column exists.
	_, err := tx.QueryContext(ctx, "SELECT history_visibility FROM syncapi_output_frame_events LIMIT 1")
	if err != nil {
		// The column probably doesn't exist
		return nil
	}
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE syncapi_output_frame_events DROP COLUMN history_visibility;
	`)
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	_, err = tx.QueryContext(ctx, "SELECT history_visibility FROM syncapi_current_frame_state LIMIT 1")
	if err != nil {
		// The column probably doesn't exist
		return nil
	}
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE syncapi_current_frame_state DROP COLUMN history_visibility;
	`)
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	return nil
}
