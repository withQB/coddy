package shared

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/federationapi/storage/shared/receipt"
	"github.com/withqb/coddy/services/federationapi/storage/tables"
	"github.com/withqb/coddy/services/federationapi/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

type Database struct {
	DB                       *sql.DB
	IsLocalServerName        func(spec.ServerName) bool
	Cache                    caching.FederationCache
	Writer                   sqlutil.Writer
	FederationQueuePDUs      tables.FederationQueuePDUs
	FederationQueueEDUs      tables.FederationQueueEDUs
	FederationQueueJSON      tables.FederationQueueJSON
	FederationJoinedHosts    tables.FederationJoinedHosts
	FederationBlacklist      tables.FederationBlacklist
	FederationAssumedOffline tables.FederationAssumedOffline
	FederationRelayServers   tables.FederationRelayServers
	FederationOutboundPeeks  tables.FederationOutboundPeeks
	FederationInboundPeeks   tables.FederationInboundPeeks
	NotaryServerKeysJSON     tables.FederationNotaryServerKeysJSON
	NotaryServerKeysMetadata tables.FederationNotaryServerKeysMetadata
	ServerSigningKeys        tables.FederationServerSigningKeys
}

// UpdateFrame updates the joined hosts for a frame and returns what the joined
// hosts were before the update, or nil if this was a duplicate message.
// This is called when we receive a message from kafka, so we pass in
// oldEventID and newEventID to check that we haven't missed any messages or
// this isn't a duplicate message.
func (d *Database) UpdateFrame(
	ctx context.Context,
	frameID string,
	addHosts []types.JoinedHost,
	removeHosts []string,
	purgeFrameFirst bool,
) (joinedHosts []types.JoinedHost, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if purgeFrameFirst {
			if err = d.FederationJoinedHosts.DeleteJoinedHostsForFrame(ctx, txn, frameID); err != nil {
				return fmt.Errorf("d.FederationJoinedHosts.DeleteJoinedHosts: %w", err)
			}
			for _, add := range addHosts {
				if err = d.FederationJoinedHosts.InsertJoinedHosts(ctx, txn, frameID, add.MemberEventID, add.ServerName); err != nil {
					return err
				}
				joinedHosts = append(joinedHosts, add)
			}
		} else {
			if joinedHosts, err = d.FederationJoinedHosts.SelectJoinedHostsWithTx(ctx, txn, frameID); err != nil {
				return err
			}
			for _, add := range addHosts {
				if err = d.FederationJoinedHosts.InsertJoinedHosts(ctx, txn, frameID, add.MemberEventID, add.ServerName); err != nil {
					return err
				}
			}
			if err = d.FederationJoinedHosts.DeleteJoinedHosts(ctx, txn, removeHosts); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// GetJoinedHosts returns the currently joined hosts for frame,
// as known to federationserver.
// Returns an error if something goes wrong.
func (d *Database) GetJoinedHosts(
	ctx context.Context, frameID string,
) ([]types.JoinedHost, error) {
	return d.FederationJoinedHosts.SelectJoinedHosts(ctx, frameID)
}

// GetAllJoinedHosts returns the currently joined hosts for
// all frames known to the federation sender.
// Returns an error if something goes wrong.
func (d *Database) GetAllJoinedHosts(
	ctx context.Context,
) ([]spec.ServerName, error) {
	return d.FederationJoinedHosts.SelectAllJoinedHosts(ctx)
}

func (d *Database) GetJoinedHostsForFrames(
	ctx context.Context,
	frameIDs []string,
	excludeSelf,
	excludeBlacklisted bool,
) ([]spec.ServerName, error) {
	servers, err := d.FederationJoinedHosts.SelectJoinedHostsForFrames(ctx, frameIDs, excludeBlacklisted)
	if err != nil {
		return nil, err
	}
	if excludeSelf {
		for i, server := range servers {
			if d.IsLocalServerName(server) {
				copy(servers[i:], servers[i+1:])
				servers = servers[:len(servers)-1]
				break
			}
		}
	}
	return servers, nil
}

// StoreJSON adds a JSON blob into the queue JSON table and returns
// a NID. The NID will then be used when inserting the per-destination
// metadata entries.
func (d *Database) StoreJSON(
	ctx context.Context, js string,
) (*receipt.Receipt, error) {
	var nid int64
	var err error
	_ = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		nid, err = d.FederationQueueJSON.InsertQueueJSON(ctx, txn, js)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("d.insertQueueJSON: %w", err)
	}
	newReceipt := receipt.NewReceipt(nid)
	return &newReceipt, nil
}

func (d *Database) AddServerToBlacklist(
	serverName spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationBlacklist.InsertBlacklist(context.TODO(), txn, serverName)
	})
}

func (d *Database) RemoveServerFromBlacklist(
	serverName spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationBlacklist.DeleteBlacklist(context.TODO(), txn, serverName)
	})
}

func (d *Database) RemoveAllServersFromBlacklist() error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationBlacklist.DeleteAllBlacklist(context.TODO(), txn)
	})
}

func (d *Database) IsServerBlacklisted(
	serverName spec.ServerName,
) (bool, error) {
	return d.FederationBlacklist.SelectBlacklist(context.TODO(), nil, serverName)
}

func (d *Database) SetServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationAssumedOffline.InsertAssumedOffline(ctx, txn, serverName)
	})
}

func (d *Database) RemoveServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationAssumedOffline.DeleteAssumedOffline(ctx, txn, serverName)
	})
}

func (d *Database) RemoveAllServersAssumedOffline(
	ctx context.Context,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationAssumedOffline.DeleteAllAssumedOffline(ctx, txn)
	})
}

func (d *Database) IsServerAssumedOffline(
	ctx context.Context,
	serverName spec.ServerName,
) (bool, error) {
	return d.FederationAssumedOffline.SelectAssumedOffline(ctx, nil, serverName)
}

func (d *Database) P2PAddRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationRelayServers.InsertRelayServers(ctx, txn, serverName, relayServers)
	})
}

func (d *Database) P2PGetRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
) ([]spec.ServerName, error) {
	return d.FederationRelayServers.SelectRelayServers(ctx, nil, serverName)
}

func (d *Database) P2PRemoveRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
	relayServers []spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationRelayServers.DeleteRelayServers(ctx, txn, serverName, relayServers)
	})
}

func (d *Database) P2PRemoveAllRelayServersForServer(
	ctx context.Context,
	serverName spec.ServerName,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationRelayServers.DeleteAllRelayServers(ctx, txn, serverName)
	})
}

func (d *Database) AddOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationOutboundPeeks.InsertOutboundPeek(ctx, txn, serverName, frameID, peekID, renewalInterval)
	})
}

func (d *Database) RenewOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationOutboundPeeks.RenewOutboundPeek(ctx, txn, serverName, frameID, peekID, renewalInterval)
	})
}

func (d *Database) GetOutboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID,
	peekID string,
) (*types.OutboundPeek, error) {
	return d.FederationOutboundPeeks.SelectOutboundPeek(ctx, nil, serverName, frameID, peekID)
}

func (d *Database) GetOutboundPeeks(
	ctx context.Context,
	frameID string,
) ([]types.OutboundPeek, error) {
	return d.FederationOutboundPeeks.SelectOutboundPeeks(ctx, nil, frameID)
}

func (d *Database) AddInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationInboundPeeks.InsertInboundPeek(ctx, txn, serverName, frameID, peekID, renewalInterval)
	})
}

func (d *Database) RenewInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID string,
	peekID string,
	renewalInterval int64,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.FederationInboundPeeks.RenewInboundPeek(ctx, txn, serverName, frameID, peekID, renewalInterval)
	})
}

func (d *Database) GetInboundPeek(
	ctx context.Context,
	serverName spec.ServerName,
	frameID string,
	peekID string,
) (*types.InboundPeek, error) {
	return d.FederationInboundPeeks.SelectInboundPeek(ctx, nil, serverName, frameID, peekID)
}

func (d *Database) GetInboundPeeks(
	ctx context.Context,
	frameID string,
) ([]types.InboundPeek, error) {
	return d.FederationInboundPeeks.SelectInboundPeeks(ctx, nil, frameID)
}

func (d *Database) UpdateNotaryKeys(
	ctx context.Context,
	serverName spec.ServerName,
	serverKeys xtools.ServerKeys,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		validUntil := serverKeys.ValidUntilTS
		// Servers MUST use the lesser of this field and 7 days into the future when determining if a key is valid.
		// This is to avoid a situation where an attacker publishes a key which is valid for a significant amount of
		// time without a way for the homeserver owner to revoke it.
		weekIntoFuture := time.Now().Add(7 * 24 * time.Hour)
		if weekIntoFuture.Before(validUntil.Time()) {
			validUntil = spec.AsTimestamp(weekIntoFuture)
		}
		notaryID, err := d.NotaryServerKeysJSON.InsertJSONResponse(ctx, txn, serverKeys, serverName, validUntil)
		if err != nil {
			return err
		}
		// update the metadata for the keys
		for keyID := range serverKeys.OldVerifyKeys {
			_, err = d.NotaryServerKeysMetadata.UpsertKey(ctx, txn, serverName, keyID, notaryID, validUntil)
			if err != nil {
				return err
			}
		}
		for keyID := range serverKeys.VerifyKeys {
			_, err = d.NotaryServerKeysMetadata.UpsertKey(ctx, txn, serverName, keyID, notaryID, validUntil)
			if err != nil {
				return err
			}
		}

		// clean up old responses
		return d.NotaryServerKeysMetadata.DeleteOldJSONResponses(ctx, txn)
	})
}

func (d *Database) GetNotaryKeys(
	ctx context.Context,
	serverName spec.ServerName,
	optKeyIDs []xtools.KeyID,
) (sks []xtools.ServerKeys, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sks, err = d.NotaryServerKeysMetadata.SelectKeys(ctx, txn, serverName, optKeyIDs)
		return err
	})
	return sks, err
}

func (d *Database) PurgeFrame(ctx context.Context, frameID string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := d.FederationJoinedHosts.DeleteJoinedHostsForFrame(ctx, txn, frameID); err != nil {
			return fmt.Errorf("failed to purge joined hosts: %w", err)
		}
		if err := d.FederationInboundPeeks.DeleteInboundPeeks(ctx, txn, frameID); err != nil {
			return fmt.Errorf("failed to purge inbound peeks: %w", err)
		}
		if err := d.FederationOutboundPeeks.DeleteOutboundPeeks(ctx, txn, frameID); err != nil {
			return fmt.Errorf("failed to purge outbound peeks: %w", err)
		}
		return nil
	})
}
