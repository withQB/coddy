package relayapi

import (
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/federationapi/producers"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/relayapi/api"
	"github.com/withqb/coddy/relayapi/internal"
	"github.com/withqb/coddy/relayapi/routing"
	"github.com/withqb/coddy/relayapi/storage"
	rsAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
)

// AddPublicRoutes sets up and registers HTTP handlers on the base API muxes for the FederationAPI component.
func AddPublicRoutes(
	routers httputil.Routers,
	dendriteCfg *config.Dendrite,
	keyRing xtools.JSONVerifier,
	relayAPI api.RelayInternalAPI,
) {
	relay, ok := relayAPI.(*internal.RelayInternalAPI)
	if !ok {
		panic("relayapi.AddPublicRoutes called with a RelayInternalAPI impl which was not " +
			"RelayInternalAPI. This is a programming error.")
	}

	routing.Setup(
		routers.Federation,
		&dendriteCfg.FederationAPI,
		relay,
		keyRing,
	)
}

func NewRelayInternalAPI(
	dendriteCfg *config.Dendrite,
	cm *sqlutil.Connections,
	fedClient fclient.FederationClient,
	rsAPI rsAPI.RoomserverInternalAPI,
	keyRing *xtools.KeyRing,
	producer *producers.SyncAPIProducer,
	relayingEnabled bool,
	caches caching.FederationCache,
) api.RelayInternalAPI {
	relayDB, err := storage.NewDatabase(cm, &dendriteCfg.RelayAPI.Database, caches, dendriteCfg.Global.IsLocalServerName)
	if err != nil {
		logrus.WithError(err).Panic("failed to connect to relay db")
	}

	return internal.NewRelayInternalAPI(
		relayDB,
		fedClient,
		rsAPI,
		keyRing,
		producer,
		dendriteCfg.Global.Presence.EnableInbound,
		dendriteCfg.Global.ServerName,
		relayingEnabled,
	)
}
