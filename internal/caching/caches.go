package caching

import (
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
)

// Caches contains a set of references to caches. They may be
// different implementations as long as they satisfy the Cache
// interface.
type Caches struct {
	FrameVersions            Cache[string, xtools.FrameVersion]            // frame ID -> frame version
	ServerKeys              Cache[string, xtools.PublicKeyLookupResult]  // server name -> server keys
	DataFrameFrameNIDs      Cache[string, types.FrameNID]                 // frame ID -> frame NID
	DataFrameFrameIDs       Cache[types.FrameNID, string]                 // frame NID -> frame ID
	DataFrameEvents        Cache[int64, *types.HeaderedEvent]           // event NID -> event
	DataFrameStateKeys     Cache[types.EventStateKeyNID, string]        // eventStateKey NID -> event state key
	DataFrameStateKeyNIDs  Cache[string, types.EventStateKeyNID]        // event state key -> eventStateKey NID
	DataFrameEventTypeNIDs Cache[string, types.EventTypeNID]            // eventType -> eventType NID
	DataFrameEventTypes    Cache[types.EventTypeNID, string]            // eventType NID -> eventType
	FederationPDUs          Cache[int64, *types.HeaderedEvent]           // queue NID -> PDU
	FederationEDUs          Cache[int64, *xtools.EDU]                    // queue NID -> EDU
	FrameHierarchies         Cache[string, fclient.FrameHierarchyResponse] // frame ID -> space response
	LazyLoading             Cache[lazyLoadingCacheKey, string]           // composite key -> event ID
}

// Cache is the interface that an implementation must satisfy.
type Cache[K keyable, T any] interface {
	Get(key K) (value T, ok bool)
	Set(key K, value T)
	Unset(key K)
}

type keyable interface {
	// from https://github.com/dgraph-io/ristretto/blob/8e850b710d6df0383c375ec6a7beae4ce48fc8d5/z/z.go#L34
	~uint64 | ~string | []byte | byte | ~int | ~int32 | ~uint32 | ~int64 | lazyLoadingCacheKey
}

type costable interface {
	CacheCost() int
}
