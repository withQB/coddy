package storage

import (
	"context"
	"fmt"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/federationapi/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

// NewDatabase opens a new database
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(ctx, conMan, dbProperties, cache, isLocalServerName)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
