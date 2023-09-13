package perform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/input"
	"github.com/withqb/coddy/servers/dataframe/internal/query"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
)

type Admin struct {
	DB      storage.Database
	Cfg     *config.DataFrame
	Queryer *query.Queryer
	Inputer *input.Inputer
	Leaver  *Leaver
}

// PerformAdminEvacuateFrame will remove all local users from the given frame.
func (r *Admin) PerformAdminEvacuateFrame(
	ctx context.Context,
	frameID string,
) (affected []string, err error) {
	frameInfo, err := r.DB.FrameInfo(ctx, frameID)
	if err != nil {
		return nil, err
	}
	if frameInfo == nil || frameInfo.IsStub() {
		return nil, eventutil.ErrFrameNoExists{}
	}

	memberNIDs, err := r.DB.GetMembershipEventNIDsForFrame(ctx, frameInfo.FrameNID, true, true)
	if err != nil {
		return nil, err
	}

	memberEvents, err := r.DB.Events(ctx, frameInfo.FrameVersion, memberNIDs)
	if err != nil {
		return nil, err
	}

	inputEvents := make([]api.InputFrameEvent, 0, len(memberEvents))
	affected = make([]string, 0, len(memberEvents))
	latestReq := &api.QueryLatestEventsAndStateRequest{
		FrameID: frameID,
	}
	latestRes := &api.QueryLatestEventsAndStateResponse{}
	if err = r.Queryer.QueryLatestEventsAndState(ctx, latestReq, latestRes); err != nil {
		return nil, err
	}
	validFrameID, err := spec.NewFrameID(frameID)
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
			FrameID:     frameID,
			Type:       spec.MFrameMember,
			StateKey:   &stateKey,
			SenderID:   stateKey,
			PrevEvents: prevEvents,
		}

		userID, err := r.Queryer.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(fledglingEvent.SenderID))
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

		inputEvents = append(inputEvents, api.InputFrameEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       senderDomain,
			SendAsServer: string(senderDomain),
		})
		affected = append(affected, stateKey)
		prevEvents = []string{event.EventID()}
	}

	inputReq := &api.InputFrameEventsRequest{
		InputFrameEvents: inputEvents,
		Asynchronous:    false,
	}
	inputRes := &api.InputFrameEventsResponse{}
	r.Inputer.InputFrameEvents(ctx, inputReq, inputRes)
	return affected, nil
}

// PerformAdminEvacuateUser will remove the given user from all frames.
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

	frameIDs, err := r.DB.GetFramesByMembership(ctx, *fullUserID, spec.Join)
	if err != nil {
		return nil, err
	}

	inviteFrameIDs, err := r.DB.GetFramesByMembership(ctx, *fullUserID, spec.Invite)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	allFrames := append(frameIDs, inviteFrameIDs...)
	affected = make([]string, 0, len(allFrames))
	for _, frameID := range allFrames {
		leaveReq := &api.PerformLeaveRequest{
			FrameID: frameID,
			Leaver: *fullUserID,
		}
		leaveRes := &api.PerformLeaveResponse{}
		outputEvents, err := r.Leaver.PerformLeave(ctx, leaveReq, leaveRes)
		if err != nil {
			return nil, err
		}
		affected = append(affected, frameID)
		if len(outputEvents) == 0 {
			continue
		}
		if err := r.Inputer.OutputProducer.ProduceFrameEvents(frameID, outputEvents); err != nil {
			return nil, err
		}
	}
	return affected, nil
}

// PerformAdminPurgeFrame removes all traces for the given frame from the database.
func (r *Admin) PerformAdminPurgeFrame(
	ctx context.Context,
	frameID string,
) error {
	// Validate we actually got a frame ID and nothing else
	if _, _, err := xtools.SplitID('!', frameID); err != nil {
		return err
	}

	logrus.WithField("frame_id", frameID).Warn("Purging frame from dataframe")
	if err := r.DB.PurgeFrame(ctx, frameID); err != nil {
		logrus.WithField("frame_id", frameID).WithError(err).Warn("Failed to purge frame from dataframe")
		return err
	}

	logrus.WithField("frame_id", frameID).Warn("Frame purged from dataframe, informing other components")

	return r.Inputer.OutputProducer.ProduceFrameEvents(frameID, []api.OutputEvent{
		{
			Type: api.OutputTypePurgeFrame,
			PurgeFrame: &api.OutputPurgeFrame{
				FrameID: frameID,
			},
		},
	})
}

func (r *Admin) PerformAdminDownloadState(
	ctx context.Context,
	frameID, userID string, serverName spec.ServerName,
) error {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return err
	}
	senderDomain := fullUserID.Domain()

	frameInfo, err := r.DB.FrameInfo(ctx, frameID)
	if err != nil {
		return err
	}

	if frameInfo == nil || frameInfo.IsStub() {
		return eventutil.ErrFrameNoExists{}
	}

	fwdExtremities, _, depth, err := r.DB.LatestEventIDs(ctx, frameInfo.FrameNID)
	if err != nil {
		return err
	}

	authEventMap := map[string]xtools.PDU{}
	stateEventMap := map[string]xtools.PDU{}

	for _, fwdExtremity := range fwdExtremities {
		var state xtools.StateResponse
		state, err = r.Inputer.FSAPI.LookupState(ctx, r.Inputer.ServerName, serverName, frameID, fwdExtremity, frameInfo.FrameVersion)
		if err != nil {
			return fmt.Errorf("r.Inputer.FSAPI.LookupState (%q): %s", fwdExtremity, err)
		}
		for _, authEvent := range state.GetAuthEvents().UntrustedEvents(frameInfo.FrameVersion) {
			if err = xtools.VerifyEventSignatures(ctx, authEvent, r.Inputer.KeyRing, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
			}); err != nil {
				continue
			}
			authEventMap[authEvent.EventID()] = authEvent
		}
		for _, stateEvent := range state.GetStateEvents().UntrustedEvents(frameInfo.FrameVersion) {
			if err = xtools.VerifyEventSignatures(ctx, stateEvent, r.Inputer.KeyRing, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, frameID, senderID)
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

	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return err
	}
	senderID, err := r.Queryer.QuerySenderIDForUser(ctx, *validFrameID, *fullUserID)
	if err != nil {
		return err
	} else if senderID == nil {
		return fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validFrameID)
	}
	proto := &xtools.ProtoEvent{
		Type:     "org.coddy.dendrite.state_download",
		SenderID: string(*senderID),
		FrameID:   frameID,
		Content:  spec.RawJSON("{}"),
	}

	eventsNeeded, err := xtools.StateNeededForProtoEvent(proto)
	if err != nil {
		return fmt.Errorf("xtools.StateNeededForProtoEvent: %w", err)
	}

	queryRes := &api.QueryLatestEventsAndStateResponse{
		FrameExists:   true,
		FrameVersion:  frameInfo.FrameVersion,
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

	inputReq := &api.InputFrameEventsRequest{
		Asynchronous: false,
	}
	inputRes := &api.InputFrameEventsResponse{}

	for _, authEvent := range append(authEvents, stateEvents...) {
		inputReq.InputFrameEvents = append(inputReq.InputFrameEvents, api.InputFrameEvent{
			Kind:  api.KindOutlier,
			Event: authEvent,
		})
	}

	inputReq.InputFrameEvents = append(inputReq.InputFrameEvents, api.InputFrameEvent{
		Kind:          api.KindNew,
		Event:         ev,
		Origin:        r.Cfg.Matrix.ServerName,
		HasState:      true,
		StateEventIDs: stateIDs,
		SendAsServer:  string(r.Cfg.Matrix.ServerName),
	})

	r.Inputer.InputFrameEvents(ctx, inputReq, inputRes)

	if inputRes.ErrMsg != "" {
		return inputRes.Err()
	}

	return nil
}
