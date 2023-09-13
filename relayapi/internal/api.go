package internal

import (
	"sync"

	"github.com/withqb/coddy/federationapi/producers"
	"github.com/withqb/coddy/relayapi/storage"
	rsAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

type RelayInternalAPI struct {
	db                     storage.Database
	fedClient              fclient.FederationClient
	rsAPI                  rsAPI.RoomserverInternalAPI
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
	rsAPI rsAPI.RoomserverInternalAPI,
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
