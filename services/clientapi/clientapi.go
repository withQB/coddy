package clientapi

import (
	"github.com/withqb/coddy/internal/httputil"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/process"
	"github.com/withqb/xtools/fclient"

	"github.com/withqb/coddy/internal/transactions"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/clientapi/api"
	"github.com/withqb/coddy/services/clientapi/producers"
	"github.com/withqb/coddy/services/clientapi/routing"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	federationAPI "github.com/withqb/coddy/services/federationapi/api"
	"github.com/withqb/coddy/setup/jetstream"
)

// AddPublicRoutes sets up and registers HTTP handlers for the ClientAPI component.
func AddPublicRoutes(
	processContext *process.ProcessContext,
	routers httputil.Routers,
	cfg *config.Dendrite,
	natsInstance *jetstream.NATSInstance,
	federation fclient.FederationClient,
	rsAPI dataframeAPI.ClientDataframeAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	transactionsCache *transactions.Cache,
	fsAPI federationAPI.ClientFederationAPI,
	userAPI userapi.ClientUserAPI,
	userDirectoryProvider userapi.QuerySearchProfilesAPI,
	extFramesProvider api.ExtraPublicFramesProvider, enableMetrics bool,
) {
	js, natsClient := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	syncProducer := &producers.SyncAPIProducer{
		JetStream:              js,
		TopicReceiptEvent:      cfg.Global.JetStream.Prefixed(jetstream.OutputReceiptEvent),
		TopicSendToDeviceEvent: cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
		TopicTypingEvent:       cfg.Global.JetStream.Prefixed(jetstream.OutputTypingEvent),
		TopicPresenceEvent:     cfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		UserAPI:                userAPI,
		ServerName:             cfg.Global.ServerName,
	}

	routing.Setup(
		routers,
		cfg, rsAPI, asAPI,
		userAPI, userDirectoryProvider, federation,
		syncProducer, transactionsCache, fsAPI,
		extFramesProvider, natsClient, enableMetrics,
	)
}
