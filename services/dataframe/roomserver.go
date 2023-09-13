package dataframe

import (
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/internal"
	"github.com/withqb/coddy/services/dataframe/storage"
)

// NewInternalAPI returns a concrete implementation of the internal API.
//
// Many of the methods provided by this API depend on access to a federation API, and so
// you may wish to call `SetFederationAPI` on the returned struct to avoid nil-dereference errors.
func NewInternalAPI(
	processContext *process.ProcessContext,
	cfg *config.Dendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	caches caching.DataFrameCaches,
	enableMetrics bool,
) api.DataframeInternalAPI {
	dataframeDB, err := storage.Open(processContext.Context(), cm, &cfg.DataFrame.Database, caches)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to frame server db")
	}

	js, nc := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	return internal.NewDataframeAPI(
		processContext, cfg, dataframeDB, js, nc, caches, enableMetrics,
	)
}
