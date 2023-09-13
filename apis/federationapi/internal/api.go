package internal

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/apis/federationapi/queue"
	"github.com/withqb/coddy/apis/federationapi/statistics"
	"github.com/withqb/coddy/apis/federationapi/storage"
	"github.com/withqb/coddy/apis/federationapi/storage/cache"
	"github.com/withqb/coddy/internal/caching"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

// FederationInternalAPI is an implementation of api.FederationInternalAPI
type FederationInternalAPI struct {
	db         storage.Database
	cfg        *config.FederationAPI
	statistics *statistics.Statistics
	rsAPI      dataframeAPI.FederationDataframeAPI
	federation fclient.FederationClient
	keyRing    *xtools.KeyRing
	queues     *queue.OutgoingQueues
	joins      sync.Map // joins currently in progress
}

func NewFederationInternalAPI(
	db storage.Database, cfg *config.FederationAPI,
	rsAPI dataframeAPI.FederationDataframeAPI,
	federation fclient.FederationClient,
	statistics *statistics.Statistics,
	caches *caching.Caches,
	queues *queue.OutgoingQueues,
	keyRing *xtools.KeyRing,
) *FederationInternalAPI {
	serverKeyDB, err := cache.NewKeyDatabase(db, caches)
	if err != nil {
		logrus.WithError(err).Panicf("failed to set up caching wrapper for server key database")
	}

	if keyRing == nil {
		keyRing = &xtools.KeyRing{
			KeyFetchers: []xtools.KeyFetcher{},
			KeyDatabase: serverKeyDB,
		}

		pubKey := cfg.Matrix.PrivateKey.Public().(ed25519.PublicKey)
		addDirectFetcher := func() {
			keyRing.KeyFetchers = append(
				keyRing.KeyFetchers,
				&xtools.DirectKeyFetcher{
					Client:            federation,
					IsLocalServerName: cfg.Matrix.IsLocalServerName,
					LocalPublicKey:    []byte(pubKey),
				},
			)
		}

		if cfg.PreferDirectFetch {
			addDirectFetcher()
		} else {
			defer addDirectFetcher()
		}

		var b64e = base64.StdEncoding.WithPadding(base64.NoPadding)
		for _, ps := range cfg.KeyPerspectives {
			perspective := &xtools.PerspectiveKeyFetcher{
				PerspectiveServerName: ps.ServerName,
				PerspectiveServerKeys: map[xtools.KeyID]ed25519.PublicKey{},
				Client:                federation,
			}

			for _, key := range ps.Keys {
				rawkey, err := b64e.DecodeString(key.PublicKey)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"server_name": ps.ServerName,
						"public_key":  key.PublicKey,
					}).Warn("Couldn't parse perspective key")
					continue
				}
				perspective.PerspectiveServerKeys[key.KeyID] = rawkey
			}

			keyRing.KeyFetchers = append(keyRing.KeyFetchers, perspective)

			logrus.WithFields(logrus.Fields{
				"server_name":     ps.ServerName,
				"num_public_keys": len(ps.Keys),
			}).Info("Enabled perspective key fetcher")
		}
	}

	return &FederationInternalAPI{
		db:         db,
		cfg:        cfg,
		rsAPI:      rsAPI,
		keyRing:    keyRing,
		federation: federation,
		statistics: statistics,
		queues:     queues,
	}
}

func (a *FederationInternalAPI) isBlacklistedOrBackingOff(s spec.ServerName) (*statistics.ServerStatistics, error) {
	stats := a.statistics.ForServer(s)
	if stats.Blacklisted() {
		return stats, &api.FederationClientError{
			Blacklisted: true,
		}
	}

	now := time.Now()
	until := stats.BackoffInfo()
	if until != nil && now.Before(*until) {
		return stats, &api.FederationClientError{
			RetryAfter: time.Until(*until),
		}
	}

	return stats, nil
}

func failBlacklistableError(err error, stats *statistics.ServerStatistics) (until time.Time, blacklisted bool) {
	if err == nil {
		return
	}
	mxerr, ok := err.(xcore.HTTPError)
	if !ok {
		return stats.Failure()
	}
	if mxerr.Code == 401 { // invalid signature in X-Matrix header
		return stats.Failure()
	}
	if mxerr.Code >= 500 && mxerr.Code < 600 { // internal server errors
		return stats.Failure()
	}
	return
}

func (a *FederationInternalAPI) doRequestIfNotBackingOffOrBlacklisted(
	s spec.ServerName, request func() (interface{}, error),
) (interface{}, error) {
	stats, err := a.isBlacklistedOrBackingOff(s)
	if err != nil {
		return nil, err
	}
	res, err := request()
	if err != nil {
		until, blacklisted := failBlacklistableError(err, stats)
		now := time.Now()
		var retryAfter time.Duration
		if until.After(now) {
			retryAfter = time.Until(until)
		}
		return res, &api.FederationClientError{
			Err:         err.Error(),
			Blacklisted: blacklisted,
			RetryAfter:  retryAfter,
		}
	}
	stats.Success(statistics.SendDirect)
	return res, nil
}

func (a *FederationInternalAPI) doRequestIfNotBlacklisted(
	s spec.ServerName, request func() (interface{}, error),
) (interface{}, error) {
	stats := a.statistics.ForServer(s)
	if blacklisted := stats.Blacklisted(); blacklisted {
		return stats, &api.FederationClientError{
			Err:         fmt.Sprintf("server %q is blacklisted", s),
			Blacklisted: true,
		}
	}
	return request()
}
