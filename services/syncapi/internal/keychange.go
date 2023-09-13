package internal

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	keytypes "github.com/withqb/coddy/services/userapi/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/coddy/services/userapi/api"
)

// DeviceOTKCounts adds one-time key counts to the /sync response
func DeviceOTKCounts(ctx context.Context, keyAPI api.SyncKeyAPI, userID, deviceID string, res *types.Response) error {
	var queryRes api.QueryOneTimeKeysResponse
	_ = keyAPI.QueryOneTimeKeys(ctx, &api.QueryOneTimeKeysRequest{
		UserID:   userID,
		DeviceID: deviceID,
	}, &queryRes)
	if queryRes.Error != nil {
		return queryRes.Error
	}
	res.DeviceListsOTKCount = queryRes.Count.KeyCount
	return nil
}

// DeviceListCatchup fills in the given response for the given user ID to bring it up-to-date with device lists. hasNew=true if the response
// was filled in, else false if there are no new device list changes because there is nothing to catch up on. The response MUST
// be already filled in with join/leave information.
func DeviceListCatchup(
	ctx context.Context, db storage.SharedUsers, userAPI api.SyncKeyAPI, rsAPI dataframeAPI.SyncDataframeAPI,
	userID string, res *types.Response, from, to types.StreamPosition,
) (newPos types.StreamPosition, hasNew bool, err error) {

	// Track users who we didn't track before but now do by virtue of sharing a frame with them, or not.
	newlyJoinedFrames := joinedFrames(res, userID)
	newlyLeftFrames := leftFrames(res)
	if len(newlyJoinedFrames) > 0 || len(newlyLeftFrames) > 0 {
		changed, left, err := TrackChangedUsers(ctx, rsAPI, userID, newlyJoinedFrames, newlyLeftFrames)
		if err != nil {
			return to, false, err
		}
		res.DeviceLists.Changed = changed
		res.DeviceLists.Left = left
		hasNew = len(changed) > 0 || len(left) > 0
	}

	// now also track users who we already share frames with but who have updated their devices between the two tokens
	offset := keytypes.OffsetOldest
	toOffset := keytypes.OffsetNewest
	if to > 0 && to > from {
		toOffset = int64(to)
	}
	if from > 0 {
		offset = int64(from)
	}
	var queryRes api.QueryKeyChangesResponse
	_ = userAPI.QueryKeyChanges(ctx, &api.QueryKeyChangesRequest{
		Offset:   offset,
		ToOffset: toOffset,
	}, &queryRes)
	if queryRes.Error != nil {
		// don't fail the catchup because we may have got useful information by tracking membership
		xutil.GetLogger(ctx).WithError(queryRes.Error).Error("QueryKeyChanges failed")
		return to, hasNew, nil
	}

	// Work out which user IDs we care about — that includes those in the original request,
	// the response from QueryKeyChanges (which includes ALL users who have changed keys)
	// as well as every user who has a join or leave event in the current sync response. We
	// will request information about which frames these users are joined to, so that we can
	// see if we still share any frames with them.
	joinUserIDs, leaveUserIDs := membershipEvents(res)
	queryRes.UserIDs = append(queryRes.UserIDs, joinUserIDs...)
	queryRes.UserIDs = append(queryRes.UserIDs, leaveUserIDs...)
	queryRes.UserIDs = xutil.UniqueStrings(queryRes.UserIDs)
	sharedUsersMap := filterSharedUsers(ctx, db, userID, queryRes.UserIDs)
	userSet := make(map[string]bool)
	for _, userID := range res.DeviceLists.Changed {
		userSet[userID] = true
	}
	for userID, count := range sharedUsersMap {
		if !userSet[userID] && count > 0 {
			res.DeviceLists.Changed = append(res.DeviceLists.Changed, userID)
			hasNew = true
			userSet[userID] = true
		}
	}
	// Finally, add in users who have joined or left.
	// TDO: This is sub-optimal because we will add users to `changed` even if we already shared a frame with them.
	for _, userID := range joinUserIDs {
		if !userSet[userID] && sharedUsersMap[userID] > 0 {
			res.DeviceLists.Changed = append(res.DeviceLists.Changed, userID)
			hasNew = true
			userSet[userID] = true
		}
	}
	for _, userID := range leaveUserIDs {
		if sharedUsersMap[userID] == 0 {
			// we no longer share a frame with this user when they left, so add to left list.
			res.DeviceLists.Left = append(res.DeviceLists.Left, userID)
		}
	}

	xutil.GetLogger(ctx).WithFields(logrus.Fields{
		"user_id":         userID,
		"from":            offset,
		"to":              toOffset,
		"response_offset": queryRes.Offset,
	}).Tracef("QueryKeyChanges request result: %+v", res.DeviceLists)

	return types.StreamPosition(queryRes.Offset), hasNew, nil
}

// TrackChangedUsers calculates the values of device_lists.changed|left in the /sync response.
func TrackChangedUsers(
	ctx context.Context, rsAPI dataframeAPI.SyncDataframeAPI, userID string, newlyJoinedFrames, newlyLeftFrames []string,
) (changed, left []string, err error) {
	// process leaves first, then joins afterwards so if we join/leave/join/leave we err on the side of including users.

	// Leave algorithm:
	// - Get set of users and number of times they appear in frames prior to leave. - QuerySharedUsersRequest with 'IncludeFrameID'.
	// - Get users in newly left frame. - QueryCurrentState
	// - Loop set of users and decrement by 1 for each user in newly left frame.
	// - If count=0 then they share no more frames so inform BOTH parties of this via 'left'=[...] in /sync.
	var queryRes dataframeAPI.QuerySharedUsersResponse
	var stateRes dataframeAPI.QueryBulkStateContentResponse
	if len(newlyLeftFrames) > 0 {
		err = rsAPI.QuerySharedUsers(ctx, &dataframeAPI.QuerySharedUsersRequest{
			UserID:         userID,
			IncludeFrameIDs: newlyLeftFrames,
		}, &queryRes)
		if err != nil {
			return nil, nil, err
		}

		err = rsAPI.QueryBulkStateContent(ctx, &dataframeAPI.QueryBulkStateContentRequest{
			FrameIDs: newlyLeftFrames,
			StateTuples: []xtools.StateKeyTuple{
				{
					EventType: spec.MFrameMember,
					StateKey:  "*",
				},
			},
			AllowWildcards: true,
		}, &stateRes)
		if err != nil {
			return nil, nil, err
		}
		for frameID, state := range stateRes.Frames {
			validFrameID, frameErr := spec.NewFrameID(frameID)
			if frameErr != nil {
				continue
			}
			for tuple, membership := range state {
				if membership != spec.Join {
					continue
				}
				user, queryErr := rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(tuple.StateKey))
				if queryErr != nil || user == nil {
					continue
				}
				queryRes.UserIDsToCount[user.String()]--
			}
		}

		for userID, count := range queryRes.UserIDsToCount {
			if count <= 0 {
				left = append(left, userID) // left is returned
			}
		}
	}

	// Join algorithm:
	// - Get the set of all joined users prior to joining frame - QuerySharedUsersRequest with 'ExcludeFrameID'.
	// - Get users in newly joined frame - QueryCurrentState
	// - Loop set of users in newly joined frame, do they appear in the set of users prior to joining?
	// - If yes: then they already shared a frame in common, do nothing.
	// - If no: then they are a brand new user so inform BOTH parties of this via 'changed=[...]'
	err = rsAPI.QuerySharedUsers(ctx, &dataframeAPI.QuerySharedUsersRequest{
		UserID:         userID,
		ExcludeFrameIDs: newlyJoinedFrames,
	}, &queryRes)
	if err != nil {
		return nil, left, err
	}
	err = rsAPI.QueryBulkStateContent(ctx, &dataframeAPI.QueryBulkStateContentRequest{
		FrameIDs: newlyJoinedFrames,
		StateTuples: []xtools.StateKeyTuple{
			{
				EventType: spec.MFrameMember,
				StateKey:  "*",
			},
		},
		AllowWildcards: true,
	}, &stateRes)
	if err != nil {
		return nil, left, err
	}
	for frameID, state := range stateRes.Frames {
		validFrameID, err := spec.NewFrameID(frameID)
		if err != nil {
			continue
		}
		for tuple, membership := range state {
			if membership != spec.Join {
				continue
			}
			// new user who we weren't previously sharing frames with
			if _, ok := queryRes.UserIDsToCount[tuple.StateKey]; !ok {
				user, err := rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(tuple.StateKey))
				if err != nil || user == nil {
					continue
				}
				changed = append(changed, user.String()) // changed is returned
			}
		}
	}
	return changed, left, nil
}

// filterSharedUsers takes a list of remote users whose keys have changed and filters
// it down to include only users who the requesting user shares a frame with.
func filterSharedUsers(
	ctx context.Context, db storage.SharedUsers, userID string, usersWithChangedKeys []string,
) map[string]int {
	sharedUsersMap := make(map[string]int, len(usersWithChangedKeys))
	for _, changedUserID := range usersWithChangedKeys {
		sharedUsersMap[changedUserID] = 0
		if changedUserID == userID {
			// We forcibly put ourselves in this list because we should be notified about our own device updates
			// and if we are in 0 frames then we don't technically share any frame with ourselves so we wouldn't
			// be notified about key changes.
			sharedUsersMap[userID] = 1
		}
	}
	sharedUsers, err := db.SharedUsers(ctx, userID, usersWithChangedKeys)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Errorf("db.SharedUsers failed: %s", err)
		// default to all users so we do needless queries rather than miss some important device update
		return sharedUsersMap
	}
	for _, userID := range sharedUsers {
		sharedUsersMap[userID]++
	}
	return sharedUsersMap
}

func joinedFrames(res *types.Response, userID string) []string {
	var frameIDs []string
	for frameID, join := range res.Frames.Join {
		// we would expect to see our join event somewhere if we newly joined the frame.
		// Normal events get put in the join section so it's not enough to know the frame ID is present in 'join'.
		newlyJoined := membershipEventPresent(join.State.Events, userID)
		if newlyJoined {
			frameIDs = append(frameIDs, frameID)
			continue
		}
		newlyJoined = membershipEventPresent(join.Timeline.Events, userID)
		if newlyJoined {
			frameIDs = append(frameIDs, frameID)
		}
	}
	return frameIDs
}

func leftFrames(res *types.Response) []string {
	frameIDs := make([]string, len(res.Frames.Leave))
	i := 0
	for frameID := range res.Frames.Leave {
		frameIDs[i] = frameID
		i++
	}
	return frameIDs
}

func membershipEventPresent(events []synctypes.ClientEvent, userID string) bool {
	for _, ev := range events {
		// it's enough to know that we have our member event here, don't need to check membership content
		// as it's implied by being in the respective section of the sync response.
		if ev.Type == spec.MFrameMember && ev.StateKey != nil && *ev.StateKey == userID {
			// ignore e.g. join -> join changes
			if gjson.GetBytes(ev.Unsigned, "prev_content.membership").Str == gjson.GetBytes(ev.Content, "membership").Str {
				continue
			}
			return true
		}
	}
	return false
}

// returns the user IDs of anyone joining or leaving a frame in this response. These users will be added to
// the 'changed' property
// "For optimal performance, Alice should be added to changed in Bob's sync only when she adds a new device,
// or when Alice and Bob now share a frame but didn't share any frame previously. However, for the sake of simpler
// logic, a server may add Alice to changed when Alice and Bob share a new frame, even if they previously already shared a frame."
func membershipEvents(res *types.Response) (joinUserIDs, leaveUserIDs []string) {
	for _, frame := range res.Frames.Join {
		for _, ev := range frame.Timeline.Events {
			if ev.Type == spec.MFrameMember && ev.StateKey != nil {
				if strings.Contains(string(ev.Content), `"join"`) {
					joinUserIDs = append(joinUserIDs, *ev.StateKey)
				} else if strings.Contains(string(ev.Content), `"invite"`) {
					joinUserIDs = append(joinUserIDs, *ev.StateKey)
				} else if strings.Contains(string(ev.Content), `"leave"`) {
					leaveUserIDs = append(leaveUserIDs, *ev.StateKey)
				} else if strings.Contains(string(ev.Content), `"ban"`) {
					leaveUserIDs = append(leaveUserIDs, *ev.StateKey)
				}
			}
		}
	}
	return
}
