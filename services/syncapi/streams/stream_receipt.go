package streams

import (
	"context"
	"encoding/json"

	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
)

type ReceiptStreamProvider struct {
	DefaultStreamProvider
}

func (p *ReceiptStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForReceipts(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *ReceiptStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *ReceiptStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	var joinedFrames []string
	for frameID, membership := range req.Frames {
		if membership == spec.Join {
			joinedFrames = append(joinedFrames, frameID)
		}
	}

	lastPos, receipts, err := snapshot.FrameReceiptsAfter(ctx, joinedFrames, from)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.FrameReceiptsAfter failed")
		return from
	}

	if len(receipts) == 0 || lastPos == 0 {
		return to
	}

	// Group receipts by frame, so we can create one ClientEvent for every frame
	receiptsByFrame := make(map[string][]types.OutputReceiptEvent)
	for _, receipt := range receipts {
		// skip ignored user events
		if _, ok := req.IgnoredUsers.List[receipt.UserID]; ok {
			continue
		}
		// Don't send private read receipts to other users
		if receipt.Type == "m.read.private" && req.Device.UserID != receipt.UserID {
			continue
		}
		receiptsByFrame[receipt.FrameID] = append(receiptsByFrame[receipt.FrameID], receipt)
	}

	for frameID, receipts := range receiptsByFrame {
		// For a complete sync, make sure we're only including this frame if
		// that frame was present in the joined frames.
		if from == 0 && !req.IsFramePresent(frameID) {
			continue
		}

		jr, ok := req.Response.Frames.Join[frameID]
		if !ok {
			jr = types.NewJoinResponse()
		}

		ev := synctypes.ClientEvent{
			Type: spec.MReceipt,
		}
		content := make(map[string]ReceiptMRead)
		for _, receipt := range receipts {
			read, ok := content[receipt.EventID]
			if !ok {
				read = ReceiptMRead{
					User: make(map[string]ReceiptTS),
				}
			}
			read.User[receipt.UserID] = ReceiptTS{TS: receipt.Timestamp}
			content[receipt.EventID] = read
		}
		ev.Content, err = json.Marshal(content)
		if err != nil {
			req.Log.WithError(err).Error("json.Marshal failed")
			return from
		}

		jr.Ephemeral.Events = append(jr.Ephemeral.Events, ev)
		req.Response.Frames.Join[frameID] = jr
	}

	return lastPos
}

type ReceiptMRead struct {
	User map[string]ReceiptTS `json:"m.read"`
}

type ReceiptTS struct {
	TS spec.Timestamp `json:"ts"`
}
