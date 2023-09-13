package streams

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"math"
	"strconv"
	"time"

	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
)

type InviteStreamProvider struct {
	DefaultStreamProvider
	rsAPI api.SyncDataframeAPI
}

func (p *InviteStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForInvites(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *InviteStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *InviteStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	r := types.Range{
		From: from,
		To:   to,
	}

	invites, retiredInvites, maxID, err := snapshot.InviteEventsInRange(
		ctx, req.Device.UserID, r,
	)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.InviteEventsInRange failed")
		return from
	}

	eventFormat := synctypes.FormatSync
	if req.Filter.EventFormat == synctypes.EventFormatFederation {
		eventFormat = synctypes.FormatSyncFederation
	}

	for frameID, inviteEvent := range invites {
		user := spec.UserID{}
		validFrameID, err := spec.NewFrameID(inviteEvent.FrameID())
		if err != nil {
			continue
		}
		sender, err := p.rsAPI.QueryUserIDForSender(ctx, *validFrameID, inviteEvent.SenderID())
		if err == nil && sender != nil {
			user = *sender
		}

		sk := inviteEvent.StateKey()
		if sk != nil && *sk != "" {
			skUserID, err := p.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*inviteEvent.StateKey()))
			if err == nil && skUserID != nil {
				skString := skUserID.String()
				sk = &skString
			}
		}

		// skip ignored user events
		if _, ok := req.IgnoredUsers.List[user.String()]; ok {
			continue
		}
		ir := types.NewInviteResponse(inviteEvent, user, sk, eventFormat)
		req.Response.Frames.Invite[frameID] = ir
	}

	// When doing an initial sync, we don't want to add retired invites, as this
	// can add frames we were invited to, but already left.
	if from == 0 {
		return to
	}
	for frameID := range retiredInvites {
		membership, _, err := snapshot.SelectMembershipForUser(ctx, frameID, req.Device.UserID, math.MaxInt64)
		// Skip if the user is an existing member of the frame.
		// Otherwise, the NewLeaveResponse will eject the user from the frame unintentionally
		if membership == spec.Join ||
			err != nil {
			continue
		}

		lr := types.NewLeaveResponse()
		h := sha256.Sum256(append([]byte(frameID), []byte(strconv.FormatInt(int64(to), 10))...))
		lr.Timeline.Events = append(lr.Timeline.Events, synctypes.ClientEvent{
			// fake event ID which muxes in the to position
			EventID:        "$" + base64.RawURLEncoding.EncodeToString(h[:]),
			OriginServerTS: spec.AsTimestamp(time.Now()),
			FrameID:         frameID,
			Sender:         req.Device.UserID,
			StateKey:       &req.Device.UserID,
			Type:           "m.frame.member",
			Content:        spec.RawJSON(`{"membership":"leave"}`),
		})
		req.Response.Frames.Leave[frameID] = lr
	}

	return maxID
}
