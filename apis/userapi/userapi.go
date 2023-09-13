package userapi

import (
	"time"

	"github.com/sirupsen/logrus"
	fedsenderapi "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/internal/pushgateway"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/process"

	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/apis/userapi/consumers"
	"github.com/withqb/coddy/apis/userapi/internal"
	"github.com/withqb/coddy/apis/userapi/producers"
	"github.com/withqb/coddy/apis/userapi/storage"
	"github.com/withqb/coddy/apis/userapi/util"
	rsapi "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/jetstream"
)

// NewInternalAPI returns a concrete implementation of the internal API. Callers
// can call functions directly on the returned API or via an HTTP interface using AddInternalRoutes.
//
// Creating a new instance of the user API requires a dataframe API with a federation API set
// using its `SetFederationAPI` method, other you may get nil-dereference errors.
func NewInternalAPI(
	processContext *process.ProcessContext,
	dendriteCfg *config.Dendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	rsAPI rsapi.UserDataframeAPI,
	fedClient fedsenderapi.KeyserverFederationAPI,
) *internal.UserInternalAPI {
	js, _ := natsInstance.Prepare(processContext, &dendriteCfg.Global.JetStream)
	appServices := dendriteCfg.Derived.ApplicationServices

	pgClient := pushgateway.NewHTTPClient(dendriteCfg.UserAPI.PushGatewayDisableTLSValidation)

	db, err := storage.NewUserDatabase(
		processContext.Context(),
		cm,
		&dendriteCfg.UserAPI.AccountDatabase,
		dendriteCfg.Global.ServerName,
		dendriteCfg.UserAPI.BCryptCost,
		dendriteCfg.UserAPI.OpenIDTokenLifetimeMS,
		api.DefaultLoginTokenLifetime,
		dendriteCfg.UserAPI.Matrix.ServerNotices.LocalPart,
	)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to accounts db")
	}

	keyDB, err := storage.NewKeyDatabase(cm, &dendriteCfg.KeyServer.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to key db")
	}

	syncProducer := producers.NewSyncAPI(
		db, js,
		// TDO: user API should handle syncs for account data. Right now,
		// it's handled by clientapi, and hence uses its topic. When user
		// API handles it for all account data, we can remove it from
		// here.
		dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputClientData),
		dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputNotificationData),
	)
	keyChangeProducer := &producers.KeyChange{
		Topic:     dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		JetStream: js,
		DB:        keyDB,
	}

	userAPI := &internal.UserInternalAPI{
		DB:                   db,
		KeyDatabase:          keyDB,
		SyncProducer:         syncProducer,
		KeyChangeProducer:    keyChangeProducer,
		Config:               &dendriteCfg.UserAPI,
		AppServices:          appServices,
		RSAPI:                rsAPI,
		DisableTLSValidation: dendriteCfg.UserAPI.PushGatewayDisableTLSValidation,
		PgClient:             pgClient,
		FedClient:            fedClient,
	}

	updater := internal.NewDeviceListUpdater(processContext, keyDB, userAPI, keyChangeProducer, fedClient, 8, rsAPI, dendriteCfg.Global.ServerName) // 8 workers TODO: configurable
	userAPI.Updater = updater
	// Remove users which we don't share a frame with anymore
	if err := updater.CleanUp(); err != nil {
		logrus.WithError(err).Error("failed to cleanup stale device lists")
	}

	go func() {
		if err := updater.Start(); err != nil {
			logrus.WithError(err).Panicf("failed to start device list updater")
		}
	}()

	dlConsumer := consumers.NewDeviceListUpdateConsumer(
		processContext, &dendriteCfg.UserAPI, js, updater,
	)
	if err := dlConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start device list consumer")
	}

	sigConsumer := consumers.NewSigningKeyUpdateConsumer(
		processContext, &dendriteCfg.UserAPI, js, userAPI,
	)
	if err := sigConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start signing key consumer")
	}

	receiptConsumer := consumers.NewOutputReceiptEventConsumer(
		processContext, &dendriteCfg.UserAPI, js, db, syncProducer, pgClient,
	)
	if err := receiptConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start user API receipt consumer")
	}

	eventConsumer := consumers.NewOutputFrameEventConsumer(
		processContext, &dendriteCfg.UserAPI, js, db, pgClient, rsAPI, syncProducer,
	)
	if err := eventConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start user API streamed event consumer")
	}

	var cleanOldNotifs func()
	cleanOldNotifs = func() {
		logrus.Infof("Cleaning old notifications")
		if err := db.DeleteOldNotifications(processContext.Context()); err != nil {
			logrus.WithError(err).Error("failed to clean old notifications")
		}
		time.AfterFunc(time.Hour, cleanOldNotifs)
	}
	time.AfterFunc(time.Minute, cleanOldNotifs)

	if dendriteCfg.Global.ReportStats.Enabled {
		go util.StartPhoneHomeCollector(time.Now(), dendriteCfg, db)
	}

	return userAPI
}
