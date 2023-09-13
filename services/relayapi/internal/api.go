package internal

import (
	"sync"

	rsAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/federationapi/producers"
	"github.com/withqb/coddy/services/relayapi/storage"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

type RelayInternalAPI struct {
	db                     storage.Database
	fedClient              fclient.FederationClient
	rsAPI                  rsAPI.DataframeInternalAPI
	keyRing                *xtools.KeyRing
	producer               *producers.SyncAPIProducer
	presenceEnabledInbound bool
	serverName             spec.ServerName
	relayingEnabledMutex   sync.Mutex
	relayingEnabled        bool
}

func NewRelayInternalAPI(
	db storage.Database,
	fedClient fclient.FederationClient,
	rsAPI rsAPI.DataframeInternalAPI,
	keyRing *xtools.KeyRing,
	producer *producers.SyncAPIProducer,
	presenceEnabledInbound bool,
	serverName spec.ServerName,
	relayingEnabled bool,
) *RelayInternalAPI {
	return &RelayInternalAPI{
		db:                     db,
		fedClient:              fedClient,
		rsAPI:                  rsAPI,
		keyRing:                keyRing,
		producer:               producer,
		presenceEnabledInbound: presenceEnabledInbound,
		serverName:             serverName,
		relayingEnabled:        relayingEnabled,
	}
}
