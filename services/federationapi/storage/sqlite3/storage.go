package sqlite3

import (
	"context"
	"database/sql"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/federationapi/storage/shared"
	"github.com/withqb/coddy/services/federationapi/storage/sqlite3/deltas"
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
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (*Database, error) {
	var d Database
	var err error
	if d.db, d.writer, err = conMan.Connection(dbProperties); err != nil {
		return nil, err
	}
	blacklist, err := NewSQLiteBlacklistTable(d.db)
	if err != nil {
		return nil, err
	}
	joinedHosts, err := NewSQLiteJoinedHostsTable(d.db)
	if err != nil {
		return nil, err
	}
	queuePDUs, err := NewSQLiteQueuePDUsTable(d.db)
	if err != nil {
		return nil, err
	}
	queueEDUs, err := NewSQLiteQueueEDUsTable(d.db)
	if err != nil {
		return nil, err
	}
	queueJSON, err := NewSQLiteQueueJSONTable(d.db)
	if err != nil {
		return nil, err
	}
	assumedOffline, err := NewSQLiteAssumedOfflineTable(d.db)
	if err != nil {
		return nil, err
	}
	relayServers, err := NewSQLiteRelayServersTable(d.db)
	if err != nil {
		return nil, err
	}
	outboundPeeks, err := NewSQLiteOutboundPeeksTable(d.db)
	if err != nil {
		return nil, err
	}
	inboundPeeks, err := NewSQLiteInboundPeeksTable(d.db)
	if err != nil {
		return nil, err
	}
	notaryKeys, err := NewSQLiteNotaryServerKeysTable(d.db)
	if err != nil {
		return nil, err
	}
	notaryKeysMetadata, err := NewSQLiteNotaryServerKeysMetadataTable(d.db)
	if err != nil {
		return nil, err
	}
	serverSigningKeys, err := NewSQLiteServerSigningKeysTable(d.db)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(d.db)
	m.AddMigrations(sqlutil.Migration{
		Version: "federationsender: drop federationsender_frames",
		Up:      deltas.UpRemoveFramesTable,
	})
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}
	if err = queueEDUs.Prepare(); err != nil {
		return nil, err
	}
	d.Database = shared.Database{
		DB:                       d.db,
		IsLocalServerName:        isLocalServerName,
		Cache:                    cache,
		Writer:                   d.writer,
		FederationJoinedHosts:    joinedHosts,
		FederationQueuePDUs:      queuePDUs,
		FederationQueueEDUs:      queueEDUs,
		FederationQueueJSON:      queueJSON,
		FederationBlacklist:      blacklist,
		FederationAssumedOffline: assumedOffline,
		FederationRelayServers:   relayServers,
		FederationOutboundPeeks:  outboundPeeks,
		FederationInboundPeeks:   inboundPeeks,
		NotaryServerKeysJSON:     notaryKeys,
		NotaryServerKeysMetadata: notaryKeysMetadata,
		ServerSigningKeys:        serverSigningKeys,
	}
	return &d, nil
}
