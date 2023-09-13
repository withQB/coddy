package mediaapi

import (
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/mediaapi/routing"
	"github.com/withqb/coddy/apis/mediaapi/storage"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/fclient"
)

// AddPublicRoutes sets up and registers HTTP handlers for the MediaAPI component.
func AddPublicRoutes(
	mediaRouter *mux.Router,
	cm *sqlutil.Connections,
	cfg *config.Dendrite,
	userAPI userapi.MediaUserAPI,
	client *fclient.Client,
) {
	mediaDB, err := storage.NewMediaAPIDatasource(cm, &cfg.MediaAPI.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to media db")
	}

	routing.Setup(
		mediaRouter, cfg, mediaDB, userAPI, client,
	)
}