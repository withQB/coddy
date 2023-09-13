package streams

import (
	"context"

	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/xtools/spec"
)

type AccountDataStreamProvider struct {
	DefaultStreamProvider
	userAPI userapi.SyncUserAPI
}

func (p *AccountDataStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForAccountData(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *AccountDataStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *AccountDataStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	r := types.Range{
		From: from,
		To:   to,
	}

	dataTypes, pos, err := snapshot.GetAccountDataInRange(
		ctx, req.Device.UserID, r, &req.Filter.AccountData,
	)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.GetAccountDataInRange failed")
		return from
	}

	// Iterate over the frames
	for frameID, dataTypes := range dataTypes {
		// For a complete sync, make sure we're only including this frame if
		// that frame was present in the joined frames.
		if from == 0 && frameID != "" && !req.IsFramePresent(frameID) {
			continue
		}

		// Request the missing data from the database
		for _, dataType := range dataTypes {
			dataReq := userapi.QueryAccountDataRequest{
				UserID:   req.Device.UserID,
				FrameID:   frameID,
				DataType: dataType,
			}
			dataRes := userapi.QueryAccountDataResponse{}
			err = p.userAPI.QueryAccountData(ctx, &dataReq, &dataRes)
			if err != nil {
				req.Log.WithError(err).Error("p.userAPI.QueryAccountData failed")
				continue
			}
			if frameID == "" {
				if globalData, ok := dataRes.GlobalAccountData[dataType]; ok {
					req.Response.AccountData.Events = append(
						req.Response.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(globalData),
						},
					)
				}
			} else {
				if frameData, ok := dataRes.FrameAccountData[frameID][dataType]; ok {
					joinData, ok := req.Response.Frames.Join[frameID]
					if !ok {
						joinData = types.NewJoinResponse()
					}
					joinData.AccountData.Events = append(
						joinData.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(frameData),
						},
					)
					req.Response.Frames.Join[frameID] = joinData
				}
			}
		}
	}

	return pos
}
