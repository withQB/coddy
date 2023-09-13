package caching

import (
	"github.com/withqb/coddy/servers/dataframe/types"
)

// DataFrameEventsCache contains the subset of functions needed for
// a dataframe event cache.
type DataFrameEventsCache interface {
	GetDataFrameEvent(eventNID types.EventNID) (*types.HeaderedEvent, bool)
	StoreDataFrameEvent(eventNID types.EventNID, event *types.HeaderedEvent)
	InvalidateDataFrameEvent(eventNID types.EventNID)
}

func (c Caches) GetDataFrameEvent(eventNID types.EventNID) (*types.HeaderedEvent, bool) {
	return c.DataFrameEvents.Get(int64(eventNID))
}

func (c Caches) StoreDataFrameEvent(eventNID types.EventNID, event *types.HeaderedEvent) {
	c.DataFrameEvents.Set(int64(eventNID), event)
}

func (c Caches) InvalidateDataFrameEvent(eventNID types.EventNID) {
	c.DataFrameEvents.Unset(int64(eventNID))
}
