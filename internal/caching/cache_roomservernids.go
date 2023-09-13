package caching

import (
	"github.com/withqb/coddy/services/dataframe/types"
)

type DataFrameCaches interface {
	DataFrameNIDsCache
	FrameVersionCache
	DataFrameEventsCache
	FrameHierarchyCache
	EventStateKeyCache
	EventTypeCache
}

// DataFrameNIDsCache contains the subset of functions needed for
// a dataframe NID cache.
type DataFrameNIDsCache interface {
	GetDataFrameFrameID(frameNID types.FrameNID) (string, bool)
	// StoreDataFrameFrameID stores frameNID -> frameID and frameID -> frameNID
	StoreDataFrameFrameID(frameNID types.FrameNID, frameID string)
	GetDataFrameFrameNID(frameID string) (types.FrameNID, bool)
}

func (c Caches) GetDataFrameFrameID(frameNID types.FrameNID) (string, bool) {
	return c.DataFrameFrameIDs.Get(frameNID)
}

// StoreDataFrameFrameID stores frameNID -> frameID and frameID -> frameNID
func (c Caches) StoreDataFrameFrameID(frameNID types.FrameNID, frameID string) {
	c.DataFrameFrameNIDs.Set(frameID, frameNID)
	c.DataFrameFrameIDs.Set(frameNID, frameID)
}

func (c Caches) GetDataFrameFrameNID(frameID string) (types.FrameNID, bool) {
	return c.DataFrameFrameNIDs.Get(frameID)
}
