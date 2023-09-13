package streams

import (
	"context"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/types"
)

type NotificationDataStreamProvider struct {
	DefaultStreamProvider
}

func (p *NotificationDataStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForNotificationData(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *NotificationDataStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *NotificationDataStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, _ types.StreamPosition,
) types.StreamPosition {
	// Get the unread notifications for frames in our join response.
	// This is to ensure clients always have an unread notification section
	// and can display the correct numbers.
	countsByFrame, err := snapshot.GetUserUnreadNotificationCountsForFrames(ctx, req.Device.UserID, req.Frames)
	if err != nil {
		req.Log.WithError(err).Error("GetUserUnreadNotificationCountsForFrames failed")
		return from
	}

	// We're merely decorating existing frames.
	for frameID, jr := range req.Response.Frames.Join {
		counts := countsByFrame[frameID]
		if counts == nil {
			counts = &eventutil.NotificationData{}
		}
		jr.UnreadNotifications = &types.UnreadNotifications{
			HighlightCount:    counts.UnreadHighlightCount,
			NotificationCount: counts.UnreadNotificationCount,
		}
		req.Response.Frames.Join[frameID] = jr
	}

	return p.LatestPosition(ctx)
}
