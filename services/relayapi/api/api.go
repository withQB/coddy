package api

import (
	"context"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

// RelayInternalAPI is used to query information from the relay server.
type RelayInternalAPI interface {
	RelayServerAPI

	// Retrieve from external relay server all transactions stored for us and process them.
	PerformRelayServerSync(
		ctx context.Context,
		userID spec.UserID,
		relayServer spec.ServerName,
	) error

	// Tells the relayapi whether or not it should act as a relay server for external servers.
	SetRelayingEnabled(bool)

	// Obtain whether the relayapi is currently configured to act as a relay server for external servers.
	RelayingEnabled() bool
}

// RelayServerAPI exposes the store & query transaction functionality of a relay server.
type RelayServerAPI interface {
	// Store transactions for forwarding to the destination at a later time.
	PerformStoreTransaction(
		ctx context.Context,
		transaction xtools.Transaction,
		userID spec.UserID,
	) error

	// Obtain the oldest stored transaction for the specified userID.
	QueryTransactions(
		ctx context.Context,
		userID spec.UserID,
		previousEntry fclient.RelayEntry,
	) (QueryRelayTransactionsResponse, error)
}

type QueryRelayTransactionsResponse struct {
	Transaction   xtools.Transaction `json:"transaction"`
	EntryID       int64              `json:"entry_id"`
	EntriesQueued bool               `json:"entries_queued"`
}
