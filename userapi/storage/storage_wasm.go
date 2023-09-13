package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/userapi/storage/sqlite3"
	"github.com/withqb/xtools/spec"
)

func NewUserDatabase(
	ctx context.Context,
	conMan sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	serverName spec.ServerName,
	bcryptCost int,
	openIDTokenLifetimeMS int64,
	loginTokenLifetime time.Duration,
	serverNoticesLocalpart string,
) (UserDatabase, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewUserDatabase(ctx, conMan, dbProperties, serverName, bcryptCost, openIDTokenLifetimeMS, loginTokenLifetime, serverNoticesLocalpart)
	case dbProperties.ConnectionString.IsPostgres():
		return nil, fmt.Errorf("can't use Postgres implementation")
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}

// NewKeyDatabase opens a new Postgres or Sqlite database (base on dataSourceName) scheme)
// and sets postgres connection parameters.
func NewKeyDatabase(conMan sqlutil.Connections, dbProperties *config.DatabaseOptions) (KeyDatabase, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewKeyDatabase(conMan, dbProperties)
	case dbProperties.ConnectionString.IsPostgres():
		return nil, fmt.Errorf("can't use Postgres implementation")
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
