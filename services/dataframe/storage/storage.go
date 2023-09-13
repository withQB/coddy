package storage

import (
	"context"
	"fmt"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
)

// Open opens a database connection.
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.DataFrameCaches) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.Open(ctx, conMan, dbProperties, cache)

	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
