package storage

import (
	"fmt"

	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/mediaapi/storage/sqlite3"
	"github.com/withqb/coddy/setup/config"
)

// NewMediaAPIDatasource opens a database connection.
func NewMediaAPIDatasource(conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
