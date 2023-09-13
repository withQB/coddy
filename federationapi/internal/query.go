package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/withqb/coddy/federationapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// QueryJoinedHostServerNamesInRoom implements api.FederationInternalAPI
func (f *FederationInternalAPI) QueryJoinedHostServerNamesInRoom(
	ctx context.Context,
	request *api.QueryJoinedHostServerNamesInRoomRequest,
	response *api.QueryJoinedHostServerNamesInRoomResponse,
) (err error) {
	joinedHosts, err := f.db.GetJoinedHostsForRooms(ctx, []string{request.RoomID}, request.ExcludeSelf, request.ExcludeBlacklisted)
	if err != nil {
		return
	}
	response.ServerNames = joinedHosts

	return
}

func (a *FederationInternalAPI) fetchServerKeysDirectly(ctx context.Context, serverName spec.ServerName) (*xtools.ServerKeys, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	ires, err := a.doRequestIfNotBackingOffOrBlacklisted(serverName, func() (interface{}, error) {
		return a.federation.GetServerKeys(ctx, serverName)
	})
	if err != nil {
		return nil, err
	}
	sks := ires.(xtools.ServerKeys)
	return &sks, nil
}

func (a *FederationInternalAPI) fetchServerKeysFromCache(
	ctx context.Context, req *api.QueryServerKeysRequest,
) ([]xtools.ServerKeys, error) {
	var results []xtools.ServerKeys
	for keyID, criteria := range req.KeyIDToCriteria {
		serverKeysResponses, _ := a.db.GetNotaryKeys(ctx, req.ServerName, []xtools.KeyID{keyID})
		if len(serverKeysResponses) == 0 {
			return nil, fmt.Errorf("failed to find server key response for key ID %s", keyID)
		}
		// we should only get 1 result as we only gave 1 key ID
		sk := serverKeysResponses[0]
		xutil.GetLogger(ctx).Infof("fetchServerKeysFromCache: minvalid:%v  keys: %+v", criteria.MinimumValidUntilTS, sk)
		if criteria.MinimumValidUntilTS != 0 {
			// check if it's still valid. if they have the same value that's also valid
			if sk.ValidUntilTS < criteria.MinimumValidUntilTS {
				return nil, fmt.Errorf(
					"found server response for key ID %s but it is no longer valid, min: %v valid_until: %v",
					keyID, criteria.MinimumValidUntilTS, sk.ValidUntilTS,
				)
			}
		}
		results = append(results, sk)
	}
	return results, nil
}

func (a *FederationInternalAPI) QueryServerKeys(
	ctx context.Context, req *api.QueryServerKeysRequest, res *api.QueryServerKeysResponse,
) error {
	// attempt to satisfy the entire request from the cache first
	results, err := a.fetchServerKeysFromCache(ctx, req)
	if err == nil {
		// satisfied entirely from cache, return it
		res.ServerKeys = results
		return nil
	}
	xutil.GetLogger(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to satisfy keys request entirely from cache, hitting direct")

	serverKeys, err := a.fetchServerKeysDirectly(ctx, req.ServerName)
	if err != nil {
		// try to load as much as we can from the cache in a best effort basis
		xutil.GetLogger(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to ask server for keys, returning best effort keys")
		serverKeysResponses, dbErr := a.db.GetNotaryKeys(ctx, req.ServerName, req.KeyIDs())
		if dbErr != nil {
			return fmt.Errorf("notary: server returned %s, and db returned %s", err, dbErr)
		}
		res.ServerKeys = serverKeysResponses
		return nil
	}
	// cache it!
	if err = a.db.UpdateNotaryKeys(context.Background(), req.ServerName, *serverKeys); err != nil {
		// non-fatal, still return the response
		xutil.GetLogger(ctx).WithError(err).Warn("failed to UpdateNotaryKeys")
	}
	res.ServerKeys = []xtools.ServerKeys{*serverKeys}
	return nil
}
