package streams

import (
	"context"
	"encoding/json"

	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/xtools/spec"
)

type TypingStreamProvider struct {
	DefaultStreamProvider
	EDUCache *caching.EDUCache
}

func (p *TypingStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *TypingStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	var err error
	for frameID, membership := range req.Frames {
		if membership != spec.Join {
			continue
		}

		jr, ok := req.Response.Frames.Join[frameID]
		if !ok {
			jr = types.NewJoinResponse()
		}

		if users, updated := p.EDUCache.GetTypingUsersIfUpdatedAfter(
			frameID, int64(from),
		); updated {
			typingUsers := make([]string, 0, len(users))
			for i := range users {
				// skip ignored user events
				if _, ok := req.IgnoredUsers.List[users[i]]; !ok {
					typingUsers = append(typingUsers, users[i])
				}
			}
			ev := synctypes.ClientEvent{
				Type: spec.MTyping,
			}
			ev.Content, err = json.Marshal(map[string]interface{}{
				"user_ids": typingUsers,
			})
			if err != nil {
				req.Log.WithError(err).Error("json.Marshal failed")
				return from
			}

			jr.Ephemeral.Events = append(jr.Ephemeral.Events, ev)
			req.Response.Frames.Join[frameID] = jr
		}
	}

	return to
}
