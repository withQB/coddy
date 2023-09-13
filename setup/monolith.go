package setup

import (
	"github.com/withqb/coddy/apis/clientapi"
	"github.com/withqb/coddy/apis/clientapi/api"
	"github.com/withqb/coddy/apis/federationapi"
	federationAPI "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/apis/mediaapi"
	"github.com/withqb/coddy/apis/relayapi"
	relayAPI "github.com/withqb/coddy/apis/relayapi/api"
	"github.com/withqb/coddy/apis/syncapi"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/internal/transactions"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
)

// Monolith represents an instantiation of all dependencies required to build
// all components of Dendrite, for use in monolith mode.
type Monolith struct {
	Config    *config.Dendrite
	KeyRing   *xtools.KeyRing
	Client    *fclient.Client
	FedClient fclient.FederationClient

	AppserviceAPI appserviceAPI.AppServiceInternalAPI
	FederationAPI federationAPI.FederationInternalAPI
	DataframeAPI dataframeAPI.DataframeInternalAPI
	UserAPI       userapi.UserInternalAPI
	RelayAPI      relayAPI.RelayInternalAPI

	// Optional
	ExtPublicFramesProvider   api.ExtraPublicFramesProvider
	ExtUserDirectoryProvider userapi.QuerySearchProfilesAPI
}

// AddAllPublicRoutes attaches all public paths to the given router
func (m *Monolith) AddAllPublicRoutes(
	processCtx *process.ProcessContext,
	cfg *config.Dendrite,
	routers httputil.Routers,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	caches *caching.Caches,
	enableMetrics bool,
) {
	userDirectoryProvider := m.ExtUserDirectoryProvider
	if userDirectoryProvider == nil {
		userDirectoryProvider = m.UserAPI
	}
	clientapi.AddPublicRoutes(
		processCtx, routers, cfg, natsInstance, m.FedClient, m.DataframeAPI, m.AppserviceAPI, transactions.New(),
		m.FederationAPI, m.UserAPI, userDirectoryProvider,
		m.ExtPublicFramesProvider, enableMetrics,
	)
	federationapi.AddPublicRoutes(
		processCtx, routers, cfg, natsInstance, m.UserAPI, m.FedClient, m.KeyRing, m.DataframeAPI, m.FederationAPI, enableMetrics,
	)
	mediaapi.AddPublicRoutes(routers.Media, cm, cfg, m.UserAPI, m.Client)
	syncapi.AddPublicRoutes(processCtx, routers, cfg, cm, natsInstance, m.UserAPI, m.DataframeAPI, caches, enableMetrics)

	if m.RelayAPI != nil {
		relayapi.AddPublicRoutes(routers, cfg, m.KeyRing, m.RelayAPI)
	}
}
