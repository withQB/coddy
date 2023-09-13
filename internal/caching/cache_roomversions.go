package caching

import "github.com/withqb/xtools"

// FrameVersionsCache contains the subset of functions needed for
// a frame version cache.
type FrameVersionCache interface {
	GetFrameVersion(frameID string) (frameVersion xtools.FrameVersion, ok bool)
	StoreFrameVersion(frameID string, frameVersion xtools.FrameVersion)
}

func (c Caches) GetFrameVersion(frameID string) (xtools.FrameVersion, bool) {
	return c.FrameVersions.Get(frameID)
}

func (c Caches) StoreFrameVersion(frameID string, frameVersion xtools.FrameVersion) {
	c.FrameVersions.Set(frameID, frameVersion)
}
