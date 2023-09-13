package caching

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"

	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
)

const (
	frameVersionsCache byte = iota + 1
	serverKeysCache
	frameNIDsCache
	frameIDsCache
	frameEventsCache
	federationPDUsCache
	federationEDUsCache
	spaceSummaryFramesCache
	lazyLoadingCache
	eventStateKeyCache
	eventTypeCache
	eventTypeNIDCache
	eventStateKeyNIDCache
)

const (
	DisableMetrics = false
	EnableMetrics  = true
)

func NewRistrettoCache(maxCost config.DataUnit, maxAge time.Duration, enablePrometheus bool) *Caches {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64((maxCost / 1024) * 10), // 10 counters per 1KB data, affects bloom filter size
		BufferItems: 64,                           // recommended by the ristretto godocs as a sane buffer size value
		MaxCost:     int64(maxCost),               // max cost is in bytes, as per the Dendrite config
		Metrics:     true,
		KeyToHash: func(key interface{}) (uint64, uint64) {
			return z.KeyToHash(key)
		},
	})
	if err != nil {
		panic(err)
	}
	if enablePrometheus {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "dendrite",
			Subsystem: "caching_ristretto",
			Name:      "ratio",
		}, func() float64 {
			return float64(cache.Metrics.Ratio())
		})
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "dendrite",
			Subsystem: "caching_ristretto",
			Name:      "cost",
		}, func() float64 {
			return float64(cache.Metrics.CostAdded() - cache.Metrics.CostEvicted())
		})
	}
	return &Caches{
		FrameVersions: &RistrettoCachePartition[string, xtools.FrameVersion]{ // frame ID -> frame version
			cache:  cache,
			Prefix: frameVersionsCache,
			MaxAge: maxAge,
		},
		ServerKeys: &RistrettoCachePartition[string, xtools.PublicKeyLookupResult]{ // server name -> server keys
			cache:   cache,
			Prefix:  serverKeysCache,
			Mutable: true,
			MaxAge:  maxAge,
		},
		DataFrameFrameNIDs: &RistrettoCachePartition[string, types.FrameNID]{ // frame ID -> frame NID
			cache:  cache,
			Prefix: frameNIDsCache,
			MaxAge: maxAge,
		},
		DataFrameFrameIDs: &RistrettoCachePartition[types.FrameNID, string]{ // frame NID -> frame ID
			cache:  cache,
			Prefix: frameIDsCache,
			MaxAge: maxAge,
		},
		DataFrameEvents: &RistrettoCostedCachePartition[int64, *types.HeaderedEvent]{ // event NID -> event
			&RistrettoCachePartition[int64, *types.HeaderedEvent]{
				cache:   cache,
				Prefix:  frameEventsCache,
				MaxAge:  maxAge,
				Mutable: true,
			},
		},
		DataFrameStateKeys: &RistrettoCachePartition[types.EventStateKeyNID, string]{ // event NID -> event state key
			cache:  cache,
			Prefix: eventStateKeyCache,
			MaxAge: maxAge,
		},
		DataFrameStateKeyNIDs: &RistrettoCachePartition[string, types.EventStateKeyNID]{ // eventStateKey -> eventStateKey NID
			cache:  cache,
			Prefix: eventStateKeyNIDCache,
			MaxAge: maxAge,
		},
		DataFrameEventTypeNIDs: &RistrettoCachePartition[string, types.EventTypeNID]{ // eventType -> eventType NID
			cache:  cache,
			Prefix: eventTypeCache,
			MaxAge: maxAge,
		},
		DataFrameEventTypes: &RistrettoCachePartition[types.EventTypeNID, string]{ // eventType NID -> eventType
			cache:  cache,
			Prefix: eventTypeNIDCache,
			MaxAge: maxAge,
		},
		FederationPDUs: &RistrettoCostedCachePartition[int64, *types.HeaderedEvent]{ // queue NID -> PDU
			&RistrettoCachePartition[int64, *types.HeaderedEvent]{
				cache:   cache,
				Prefix:  federationPDUsCache,
				Mutable: true,
				MaxAge:  lesserOf(time.Hour/2, maxAge),
			},
		},
		FederationEDUs: &RistrettoCostedCachePartition[int64, *xtools.EDU]{ // queue NID -> EDU
			&RistrettoCachePartition[int64, *xtools.EDU]{
				cache:   cache,
				Prefix:  federationEDUsCache,
				Mutable: true,
				MaxAge:  lesserOf(time.Hour/2, maxAge),
			},
		},
		FrameHierarchies: &RistrettoCachePartition[string, fclient.FrameHierarchyResponse]{ // frame ID -> space response
			cache:   cache,
			Prefix:  spaceSummaryFramesCache,
			Mutable: true,
			MaxAge:  maxAge,
		},
		LazyLoading: &RistrettoCachePartition[lazyLoadingCacheKey, string]{ // composite key -> event ID
			cache:   cache,
			Prefix:  lazyLoadingCache,
			Mutable: true,
			MaxAge:  maxAge,
		},
	}
}

type RistrettoCostedCachePartition[k keyable, v costable] struct {
	*RistrettoCachePartition[k, v]
}

func (c *RistrettoCostedCachePartition[K, V]) Set(key K, value V) {
	cost := value.CacheCost()
	c.setWithCost(key, value, int64(cost))
}

type RistrettoCachePartition[K keyable, V any] struct {
	cache   *ristretto.Cache //nolint:all,unused
	Prefix  byte
	Mutable bool
	MaxAge  time.Duration
}

func (c *RistrettoCachePartition[K, V]) setWithCost(key K, value V, cost int64) {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	if !c.Mutable {
		if v, ok := c.cache.Get(bkey); ok && v != nil && !reflect.DeepEqual(v, value) {
			panic(fmt.Sprintf("invalid use of immutable cache tries to change value of %v from %v to %v", key, v, value))
		}
	}
	c.cache.SetWithTTL(bkey, value, int64(len(bkey))+cost, c.MaxAge)
}

func (c *RistrettoCachePartition[K, V]) Set(key K, value V) {
	var cost int64
	if cv, ok := any(value).(string); ok {
		cost = int64(len(cv))
	} else {
		cost = int64(unsafe.Sizeof(value))
	}
	c.setWithCost(key, value, cost)
}

func (c *RistrettoCachePartition[K, V]) Unset(key K) {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	if !c.Mutable {
		panic(fmt.Sprintf("invalid use of immutable cache tries to unset value of %v", key))
	}
	c.cache.Del(bkey)
}

func (c *RistrettoCachePartition[K, V]) Get(key K) (value V, ok bool) {
	bkey := fmt.Sprintf("%c%v", c.Prefix, key)
	v, ok := c.cache.Get(bkey)
	if !ok || v == nil {
		var empty V
		return empty, false
	}
	value, ok = v.(V)
	return
}

func lesserOf(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
