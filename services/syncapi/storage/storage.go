package storage

import (
	"context"
	"fmt"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/syncapi/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
)

// NewSyncServerDatasource opens a database connection.
func NewSyncServerDatasource(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(ctx, conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
