package storage

import (
	"fmt"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/mediaapi/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
)

// Open opens a postgres database.
func NewMediaAPIDatasource(conMan sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(conMan, dbProperties)
	case dbProperties.ConnectionString.IsPostgres():
		return nil, fmt.Errorf("can't use Postgres implementation")
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
