package caching

import (
	userapi "github.com/withqb/coddy/apis/userapi/api"
)

type lazyLoadingCacheKey struct {
	UserID       string // the user we're querying on behalf of
	DeviceID     string // the user we're querying on behalf of
	FrameID       string // the frame in question
	TargetUserID string // the user whose membership we're asking about
}

type LazyLoadCache interface {
	StoreLazyLoadedUser(device *userapi.Device, frameID, userID, eventID string)
	IsLazyLoadedUserCached(device *userapi.Device, frameID, userID string) (string, bool)
	InvalidateLazyLoadedUser(device *userapi.Device, frameID, userID string)
}

func (c Caches) StoreLazyLoadedUser(device *userapi.Device, frameID, userID, eventID string) {
	c.LazyLoading.Set(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		FrameID:       frameID,
		TargetUserID: userID,
	}, eventID)
}

func (c Caches) IsLazyLoadedUserCached(device *userapi.Device, frameID, userID string) (string, bool) {
	return c.LazyLoading.Get(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		FrameID:       frameID,
		TargetUserID: userID,
	})
}

func (c Caches) InvalidateLazyLoadedUser(device *userapi.Device, frameID, userID string) {
	c.LazyLoading.Unset(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		FrameID:       frameID,
		TargetUserID: userID,
	})
}
