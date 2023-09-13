// Package mscs implements Matrix Spec Changes from https://github.com/withqb/coddy-doc
package mscs

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/setup"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/mscs/msc2836"
	"github.com/withqb/xutil"
)

// Enable MSCs - returns an error on unknown MSCs
func Enable(cfg *config.Dendrite, cm *sqlutil.Connections, routers httputil.Routers, monolith *setup.Monolith, caches *caching.Caches) error {
	for _, msc := range cfg.MSCs.MSCs {
		xutil.GetLogger(context.Background()).WithField("msc", msc).Info("Enabling MSC")
		if err := EnableMSC(cfg, cm, routers, monolith, msc, caches); err != nil {
			return err
		}
	}
	return nil
}

func EnableMSC(cfg *config.Dendrite, cm *sqlutil.Connections, routers httputil.Routers, monolith *setup.Monolith, msc string, caches *caching.Caches) error {
	switch msc {
	case "msc2836":
		return msc2836.Enable(cfg, cm, routers, monolith.DataframeAPI, monolith.FederationAPI, monolith.UserAPI, monolith.KeyRing)
	case "msc2444": // enabled inside federationapi
	case "msc2753": // enabled inside clientapi
	default:
		logrus.Warnf("EnableMSC: unknown MSC '%s', this MSC is either not supported or is natively supported by Dendrite", msc)
	}
	return nil
}
