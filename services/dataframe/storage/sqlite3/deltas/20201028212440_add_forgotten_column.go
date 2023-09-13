package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

func UpAddForgottenColumn(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `	ALTER TABLE dataframe_membership RENAME TO dataframe_membership_tmp;
CREATE TABLE IF NOT EXISTS dataframe_membership (
		frame_nid INTEGER NOT NULL,
		target_nid INTEGER NOT NULL,
		sender_nid INTEGER NOT NULL DEFAULT 0,
		membership_nid INTEGER NOT NULL DEFAULT 1,
		event_nid INTEGER NOT NULL DEFAULT 0,
		target_local BOOLEAN NOT NULL DEFAULT false,
		forgotten BOOLEAN NOT NULL DEFAULT false,
		UNIQUE (frame_nid, target_nid)
	);
INSERT
    INTO dataframe_membership (
      frame_nid, target_nid, sender_nid, membership_nid, event_nid, target_local
    ) SELECT
        frame_nid, target_nid, sender_nid, membership_nid, event_nid, target_local
    FROM dataframe_membership_tmp
;
DROP TABLE dataframe_membership_tmp;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	return nil
}

func DownAddForgottenColumn(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `	ALTER TABLE dataframe_membership RENAME TO dataframe_membership_tmp;
CREATE TABLE IF NOT EXISTS dataframe_membership (
		frame_nid INTEGER NOT NULL,
		target_nid INTEGER NOT NULL,
		sender_nid INTEGER NOT NULL DEFAULT 0,
		membership_nid INTEGER NOT NULL DEFAULT 1,
		event_nid INTEGER NOT NULL DEFAULT 0,
		target_local BOOLEAN NOT NULL DEFAULT false,
		UNIQUE (frame_nid, target_nid)
	);
INSERT
    INTO dataframe_membership (
      frame_nid, target_nid, sender_nid, membership_nid, event_nid, target_local
    ) SELECT
        frame_nid, target_nid, sender_nid, membership_nid, event_nid, target_local
    FROM dataframe_membership_tmp
;
DROP TABLE dataframe_membership_tmp;`)
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	return nil
}
