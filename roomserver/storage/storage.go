//go:build !wasm
// +build !wasm

package storage

import (
	"context"
	"fmt"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/roomserver/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
)

// Open opens a database connection.
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.Open(ctx, conMan, dbProperties, cache)

	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
