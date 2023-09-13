package perform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/internal/input"
	"github.com/withqb/coddy/roomserver/internal/query"
	"github.com/withqb/coddy/roomserver/storage"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

type Admin struct {
	DB      storage.Database
	Cfg     *config.RoomServer
	Queryer *query.Queryer
	Inputer *input.Inputer
	Leaver  *Leaver
}

// PerformAdminEvacuateRoom will remove all local users from the given room.
func (r *Admin) PerformAdminEvacuateRoom(
	ctx context.Context,
	roomID string,
) (affected []string, err error) {
	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return nil, eventutil.ErrRoomNoExists{}
	}

	memberNIDs, err := r.DB.GetMembershipEventNIDsForRoom(ctx, roomInfo.RoomNID, true, true)
	if err != nil {
		return nil, err
	}

	memberEvents, err := r.DB.Events(ctx, roomInfo.RoomVersion, memberNIDs)
	if err != nil {
		return nil, err
	}

	inputEvents := make([]api.InputRoomEvent, 0, len(memberEvents))
	affected = make([]string, 0, len(memberEvents))
	latestReq := &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
	}
	latestRes := &api.QueryLatestEventsAndStateResponse{}
	if err = r.Queryer.QueryLatestEventsAndState(ctx, latestReq, latestRes); err != nil {
		return nil, err
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, err
	}

	prevEvents := latestRes.LatestEvents
	var senderDomain spec.ServerName
	var eventsNeeded xtools.StateNeeded
	var identity *fclient.SigningIdentity
	var event *types.HeaderedEvent
	for _, memberEvent := range memberEvents {
		if memberEvent.StateKey() == nil {
			continue
		}

		var memberContent xtools.MemberContent
		if err = json.Unmarshal(memberEvent.Content(), &memberContent); err != nil {
			return nil, err
		}
		memberContent.Membership = spec.Leave

		stateKey := *memberEvent.StateKey()
		fledglingEvent := &xtools.ProtoEvent{
			RoomID:     roomID,
			Type:       spec.MRoomMember,
			StateKey:   &stateKey,
			SenderID:   stateKey,
			PrevEvents: prevEvents,
		}

		userID, err := r.Queryer.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(fledglingEvent.SenderID))
		if err != nil || userID == nil {
			continue
		}
		senderDomain = userID.Domain()

		if fledglingEvent.Content, err = json.Marshal(memberContent); err != nil {
			return nil, err
		}

		eventsNeeded, err = xtools.StateNeededForProtoEvent(fledglingEvent)
		if err != nil {
			return nil, err
		}

		identity, err = r.Cfg.Matrix.SigningIdentityFor(senderDomain)
		if err != nil {
			continue
		}

		event, err = eventutil.BuildEvent(ctx, fledglingEvent, identity, time.Now(), &eventsNeeded, latestRes)
		if err != nil {
			return nil, err
		}

		inputEvents = append(inputEvents, api.InputRoomEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       senderDomain,
			SendAsServer: string(senderDomain),
		})
		affected = append(affected, stateKey)
		prevEvents = []string{event.EventID()}
	}

	inputReq := &api.InputRoomEventsRequest{
		InputRoomEvents: inputEvents,
		Asynchronous:    false,
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, inputReq, inputRes)
	return affected, nil
}

// PerformAdminEvacuateUser will remove the given user from all rooms.
func (r *Admin) PerformAdminEvacuateUser(
	ctx context.Context,
	userID string,
) (affected []string, err error) {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return nil, err
	}
	if !r.Cfg.Matrix.IsLocalServerName(fullUserID.Domain()) {
		return nil, fmt.Errorf("can only evacuate local users using this endpoint")
	}

	roomIDs, err := r.DB.GetRoomsByMembership(ctx, *fullUserID, spec.Join)
	if err != nil {
		return nil, err
	}

	inviteRoomIDs, err := r.DB.GetRoomsByMembership(ctx, *fullUserID, spec.Invite)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	allRooms := append(roomIDs, inviteRoomIDs...)
	affected = make([]string, 0, len(allRooms))
	for _, roomID := range allRooms {
		leaveReq := &api.PerformLeaveRequest{
			RoomID: roomID,
			Leaver: *fullUserID,
		}
		leaveRes := &api.PerformLeaveResponse{}
		outputEvents, err := r.Leaver.PerformLeave(ctx, leaveReq, leaveRes)
		if err != nil {
			return nil, err
		}
		affected = append(affected, roomID)
		if len(outputEvents) == 0 {
			continue
		}
		if err := r.Inputer.OutputProducer.ProduceRoomEvents(roomID, outputEvents); err != nil {
			return nil, err
		}
	}
	return affected, nil
}

// PerformAdminPurgeRoom removes all traces for the given room from the database.
func (r *Admin) PerformAdminPurgeRoom(
	ctx context.Context,
	roomID string,
) error {
	// Validate we actually got a room ID and nothing else
	if _, _, err := xtools.SplitID('!', roomID); err != nil {
		return err
	}

	logrus.WithField("room_id", roomID).Warn("Purging room from roomserver")
	if err := r.DB.PurgeRoom(ctx, roomID); err != nil {
		logrus.WithField("room_id", roomID).WithError(err).Warn("Failed to purge room from roomserver")
		return err
	}

	logrus.WithField("room_id", roomID).Warn("Room purged from roomserver, informing other components")

	return r.Inputer.OutputProducer.ProduceRoomEvents(roomID, []api.OutputEvent{
		{
			Type: api.OutputTypePurgeRoom,
			PurgeRoom: &api.OutputPurgeRoom{
				RoomID: roomID,
			},
		},
	})
}

func (r *Admin) PerformAdminDownloadState(
	ctx context.Context,
	roomID, userID string, serverName spec.ServerName,
) error {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return err
	}
	senderDomain := fullUserID.Domain()

	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return err
	}

	if roomInfo == nil || roomInfo.IsStub() {
		return eventutil.ErrRoomNoExists{}
	}

	fwdExtremities, _, depth, err := r.DB.LatestEventIDs(ctx, roomInfo.RoomNID)
	if err != nil {
		return err
	}

	authEventMap := map[string]xtools.PDU{}
	stateEventMap := map[string]xtools.PDU{}

	for _, fwdExtremity := range fwdExtremities {
		var state xtools.StateResponse
		state, err = r.Inputer.FSAPI.LookupState(ctx, r.Inputer.ServerName, serverName, roomID, fwdExtremity, roomInfo.RoomVersion)
		if err != nil {
			return fmt.Errorf("r.Inputer.FSAPI.LookupState (%q): %s", fwdExtremity, err)
		}
		for _, authEvent := range state.GetAuthEvents().UntrustedEvents(roomInfo.RoomVersion) {
			if err = xtools.VerifyEventSignatures(ctx, authEvent, r.Inputer.KeyRing, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
			}); err != nil {
				continue
			}
			authEventMap[authEvent.EventID()] = authEvent
		}
		for _, stateEvent := range state.GetStateEvents().UntrustedEvents(roomInfo.RoomVersion) {
			if err = xtools.VerifyEventSignatures(ctx, stateEvent, r.Inputer.KeyRing, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
			}); err != nil {
				continue
			}
			stateEventMap[stateEvent.EventID()] = stateEvent
		}
	}

	authEvents := make([]*types.HeaderedEvent, 0, len(authEventMap))
	stateEvents := make([]*types.HeaderedEvent, 0, len(stateEventMap))
	stateIDs := make([]string, 0, len(stateEventMap))

	for _, authEvent := range authEventMap {
		authEvents = append(authEvents, &types.HeaderedEvent{PDU: authEvent})
	}
	for _, stateEvent := range stateEventMap {
		stateEvents = append(stateEvents, &types.HeaderedEvent{PDU: stateEvent})
		stateIDs = append(stateIDs, stateEvent.EventID())
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}
	senderID, err := r.Queryer.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return err
	} else if senderID == nil {
		return fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validRoomID)
	}
	proto := &xtools.ProtoEvent{
		Type:     "org.matrix.dendrite.state_download",
		SenderID: string(*senderID),
		RoomID:   roomID,
		Content:  spec.RawJSON("{}"),
	}

	eventsNeeded, err := xtools.StateNeededForProtoEvent(proto)
	if err != nil {
		return fmt.Errorf("xtools.StateNeededForProtoEvent: %w", err)
	}

	queryRes := &api.QueryLatestEventsAndStateResponse{
		RoomExists:   true,
		RoomVersion:  roomInfo.RoomVersion,
		LatestEvents: fwdExtremities,
		StateEvents:  stateEvents,
		Depth:        depth,
	}

	identity, err := r.Cfg.Matrix.SigningIdentityFor(senderDomain)
	if err != nil {
		return err
	}

	ev, err := eventutil.BuildEvent(ctx, proto, identity, time.Now(), &eventsNeeded, queryRes)
	if err != nil {
		return fmt.Errorf("eventutil.BuildEvent: %w", err)
	}

	inputReq := &api.InputRoomEventsRequest{
		Asynchronous: false,
	}
	inputRes := &api.InputRoomEventsResponse{}

	for _, authEvent := range append(authEvents, stateEvents...) {
		inputReq.InputRoomEvents = append(inputReq.InputRoomEvents, api.InputRoomEvent{
			Kind:  api.KindOutlier,
			Event: authEvent,
		})
	}

	inputReq.InputRoomEvents = append(inputReq.InputRoomEvents, api.InputRoomEvent{
		Kind:          api.KindNew,
		Event:         ev,
		Origin:        r.Cfg.Matrix.ServerName,
		HasState:      true,
		StateEventIDs: stateIDs,
		SendAsServer:  string(r.Cfg.Matrix.ServerName),
	})

	r.Inputer.InputRoomEvents(ctx, inputReq, inputRes)

	if inputRes.ErrMsg != "" {
		return inputRes.Err()
	}

	return nil
}
