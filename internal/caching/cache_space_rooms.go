package caching

import "github.com/withqb/xtools/fclient"

// FrameHierarchy cache caches responses to federated frame hierarchy requests (A.K.A. 'space summaries')
type FrameHierarchyCache interface {
	GetFrameHierarchy(frameID string) (r fclient.FrameHierarchyResponse, ok bool)
	StoreFrameHierarchy(frameID string, r fclient.FrameHierarchyResponse)
}

func (c Caches) GetFrameHierarchy(frameID string) (r fclient.FrameHierarchyResponse, ok bool) {
	return c.FrameHierarchies.Get(frameID)
}

func (c Caches) StoreFrameHierarchy(frameID string, r fclient.FrameHierarchyResponse) {
	c.FrameHierarchies.Set(frameID, r)
}
