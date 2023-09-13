package deltas

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/withqb/xtools/spec"
)

func UpAddexpiresat(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE federationsender_queue_edus ADD COLUMN IF NOT EXISTS expires_at BIGINT NOT NULL DEFAULT 0;")
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE federationsender_queue_edus SET expires_at = $1 WHERE edu_type != 'm.direct_to_device'", spec.AsTimestamp(time.Now().Add(time.Hour*24)))
	if err != nil {
		return fmt.Errorf("failed to update queue_edus: %w", err)
	}
	return nil
}

func DownAddexpiresat(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE federationsender_queue_edus DROP COLUMN expires_at;")
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	return nil
}
