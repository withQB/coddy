package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

func UpPulishedAppservice(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `	ALTER TABLE dataframe_published RENAME TO dataframe_published_tmp;
CREATE TABLE IF NOT EXISTS dataframe_published (
    frame_id TEXT NOT NULL,
    appservice_id TEXT NOT NULL DEFAULT '',
    network_id TEXT NOT NULL DEFAULT '',
    published BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT unique_published_idx PRIMARY KEY (frame_id, appservice_id, network_id)
);
INSERT
    INTO dataframe_published (
      frame_id, published
    ) SELECT
        frame_id, published
    FROM dataframe_published_tmp
;
DROP TABLE dataframe_published_tmp;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	return nil
}

func DownPublishedAppservice(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `	ALTER TABLE dataframe_published RENAME TO dataframe_published_tmp;
CREATE TABLE IF NOT EXISTS dataframe_published (
    frame_id TEXT NOT NULL PRIMARY KEY,
    published BOOLEAN NOT NULL DEFAULT false
);
INSERT
    INTO dataframe_published (
      frame_id, published
    ) SELECT
        frame_id, published
    FROM dataframe_published_tmp
;
DROP TABLE dataframe_published_tmp;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	return nil
}
