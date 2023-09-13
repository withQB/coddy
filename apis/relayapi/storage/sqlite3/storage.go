package sqlite3

import (
	"database/sql"

	"github.com/withqb/coddy/apis/relayapi/storage/shared"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

// Database stores information needed by the federation sender
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database
func NewDatabase(
	conMan *sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	cache caching.FederationCache,
	isLocalServerName func(spec.ServerName) bool,
) (*Database, error) {
	var d Database
	var err error
	if d.db, d.writer, err = conMan.Connection(dbProperties); err != nil {
		return nil, err
	}
	queue, err := NewSQLiteRelayQueueTable(d.db)
	if err != nil {
		return nil, err
	}
	queueJSON, err := NewSQLiteRelayQueueJSONTable(d.db)
	if err != nil {
		return nil, err
	}
	d.Database = shared.Database{
		DB:                d.db,
		IsLocalServerName: isLocalServerName,
		Cache:             cache,
		Writer:            d.writer,
		RelayQueue:        queue,
		RelayQueueJSON:    queueJSON,
	}
	return &d, nil
}
