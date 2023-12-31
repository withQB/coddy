package caching

import "github.com/withqb/coddy/services/dataframe/types"

// EventStateKeyCache contains the subset of functions needed for
// a frame event state key cache.
type EventStateKeyCache interface {
	GetEventStateKey(eventStateKeyNID types.EventStateKeyNID) (string, bool)
	StoreEventStateKey(eventStateKeyNID types.EventStateKeyNID, eventStateKey string)
	GetEventStateKeyNID(eventStateKey string) (types.EventStateKeyNID, bool)
}

func (c Caches) GetEventStateKey(eventStateKeyNID types.EventStateKeyNID) (string, bool) {
	return c.DataFrameStateKeys.Get(eventStateKeyNID)
}

func (c Caches) StoreEventStateKey(eventStateKeyNID types.EventStateKeyNID, eventStateKey string) {
	c.DataFrameStateKeys.Set(eventStateKeyNID, eventStateKey)
	c.DataFrameStateKeyNIDs.Set(eventStateKey, eventStateKeyNID)
}

func (c Caches) GetEventStateKeyNID(eventStateKey string) (types.EventStateKeyNID, bool) {
	return c.DataFrameStateKeyNIDs.Get(eventStateKey)
}

type EventTypeCache interface {
	GetEventTypeKey(eventType string) (types.EventTypeNID, bool)
	StoreEventTypeKey(eventTypeNID types.EventTypeNID, eventType string)
}

func (c Caches) StoreEventTypeKey(eventTypeNID types.EventTypeNID, eventType string) {
	c.DataFrameEventTypeNIDs.Set(eventType, eventTypeNID)
	c.DataFrameEventTypes.Set(eventTypeNID, eventType)
}

func (c Caches) GetEventTypeKey(eventType string) (types.EventTypeNID, bool) {
	return c.DataFrameEventTypeNIDs.Get(eventType)
}
