package caching

import (
	"fmt"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// ServerKeyCache contains the subset of functions needed for
// a server key cache.
type ServerKeyCache interface {
	// request -> timestamp is emulating xtools.FetchKeys:
	// https://github.com/withqb/xtools/blob/f69539c86ea55d1e2cc76fd8e944e2d82d30397c/keyring.go#L95
	// The timestamp should be the timestamp of the event that is being
	// verified. We will not return keys from the cache that are not valid
	// at this timestamp.
	GetServerKey(request xtools.PublicKeyLookupRequest, timestamp spec.Timestamp) (response xtools.PublicKeyLookupResult, ok bool)

	// request -> result is emulating xtools.StoreKeys:
	// https://github.com/withqb/xtools/blob/f69539c86ea55d1e2cc76fd8e944e2d82d30397c/keyring.go#L112
	StoreServerKey(request xtools.PublicKeyLookupRequest, response xtools.PublicKeyLookupResult)
}

func (c Caches) GetServerKey(
	request xtools.PublicKeyLookupRequest,
	timestamp spec.Timestamp,
) (xtools.PublicKeyLookupResult, bool) {
	key := fmt.Sprintf("%s/%s", request.ServerName, request.KeyID)
	val, found := c.ServerKeys.Get(key)
	if found && !val.WasValidAt(timestamp, xtools.StrictValiditySignatureCheck) {
		// The key wasn't valid at the requested timestamp so don't
		// return it. The caller will have to work out what to do.
		c.ServerKeys.Unset(key)
		return xtools.PublicKeyLookupResult{}, false
	}
	return val, found
}

func (c Caches) StoreServerKey(
	request xtools.PublicKeyLookupRequest,
	response xtools.PublicKeyLookupResult,
) {
	key := fmt.Sprintf("%s/%s", request.ServerName, request.KeyID)
	c.ServerKeys.Set(key, response)
}
