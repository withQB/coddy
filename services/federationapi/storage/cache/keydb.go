package cache

import (
	"context"
	"errors"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// A Database implements xtools.KeyDatabase and is used to store
// the public keys for other coddy servers.
type KeyDatabase struct {
	inner xtools.KeyDatabase
	cache caching.ServerKeyCache
}

func NewKeyDatabase(inner xtools.KeyDatabase, cache caching.ServerKeyCache) (*KeyDatabase, error) {
	if inner == nil {
		return nil, errors.New("inner database can't be nil")
	}
	if cache == nil {
		return nil, errors.New("cache can't be nil")
	}
	return &KeyDatabase{
		inner: inner,
		cache: cache,
	}, nil
}

// FetcherName implements KeyFetcher
func (d KeyDatabase) FetcherName() string {
	return "InMemoryKeyCache"
}

// FetchKeys implements xtools.KeyDatabase
func (d *KeyDatabase) FetchKeys(
	ctx context.Context,
	requests map[xtools.PublicKeyLookupRequest]spec.Timestamp,
) (map[xtools.PublicKeyLookupRequest]xtools.PublicKeyLookupResult, error) {
	results := make(map[xtools.PublicKeyLookupRequest]xtools.PublicKeyLookupResult)
	for req, ts := range requests {
		if res, cached := d.cache.GetServerKey(req, ts); cached {
			results[req] = res
			delete(requests, req)
		}
	}
	fromDB, err := d.inner.FetchKeys(ctx, requests)
	if err != nil {
		return results, err
	}
	for req, res := range fromDB {
		results[req] = res
		d.cache.StoreServerKey(req, res)
	}
	return results, nil
}

// StoreKeys implements xtools.KeyDatabase
func (d *KeyDatabase) StoreKeys(
	ctx context.Context,
	keyMap map[xtools.PublicKeyLookupRequest]xtools.PublicKeyLookupResult,
) error {
	for req, res := range keyMap {
		d.cache.StoreServerKey(req, res)
	}
	return d.inner.StoreKeys(ctx, keyMap)
}
