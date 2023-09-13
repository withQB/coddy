package caching

import "github.com/withqb/xtools"

// RoomVersionsCache contains the subset of functions needed for
// a room version cache.
type RoomVersionCache interface {
	GetRoomVersion(roomID string) (roomVersion xtools.RoomVersion, ok bool)
	StoreRoomVersion(roomID string, roomVersion xtools.RoomVersion)
}

func (c Caches) GetRoomVersion(roomID string) (xtools.RoomVersion, bool) {
	return c.RoomVersions.Get(roomID)
}

func (c Caches) StoreRoomVersion(roomID string, roomVersion xtools.RoomVersion) {
	c.RoomVersions.Set(roomID, roomVersion)
}
