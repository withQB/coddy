package perform

import (
	"context"
	"crypto/ed25519"
	"fmt"

	federationAPI "github.com/withqb/coddy/federationapi/api"
	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/internal/helpers"
	"github.com/withqb/coddy/roomserver/internal/input"
	"github.com/withqb/coddy/roomserver/state"
	"github.com/withqb/coddy/roomserver/storage"
	"github.com/withqb/coddy/roomserver/storage/shared"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type QueryState struct {
	storage.Database
	querier api.QuerySenderIDAPI
}

func (q *QueryState) GetAuthEvents(ctx context.Context, event xtools.PDU) (xtools.AuthEventProvider, error) {
	return helpers.GetAuthEvents(ctx, q.Database, event.Version(), event, event.AuthEventIDs())
}

func (q *QueryState) GetState(ctx context.Context, roomID spec.RoomID, stateWanted []xtools.StateKeyTuple) ([]xtools.PDU, error) {
	info, err := q.Database.RoomInfo(ctx, roomID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to load RoomInfo: %w", err)
	}
	if info != nil {
		roomState := state.NewStateResolution(q.Database, info, q.querier)
		stateEntries, err := roomState.LoadStateAtSnapshotForStringTuples(
			ctx, info.StateSnapshotNID(), stateWanted,
		)
		if err != nil {
			return nil, nil
		}
		stateNIDs := []types.EventNID{}
		for _, stateNID := range stateEntries {
			stateNIDs = append(stateNIDs, stateNID.EventNID)
		}
		stateEvents, err := q.Database.Events(ctx, info.RoomVersion, stateNIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to obtain required events: %w", err)
		}

		events := []xtools.PDU{}
		for _, event := range stateEvents {
			events = append(events, event.PDU)
		}
		return events, nil
	}

	return nil, nil
}

type Inviter struct {
	DB      storage.Database
	Cfg     *config.RoomServer
	FSAPI   federationAPI.RoomserverFederationAPI
	RSAPI   api.RoomserverInternalAPI
	Inputer *input.Inputer
}

func (r *Inviter) IsKnownRoom(ctx context.Context, roomID spec.RoomID) (bool, error) {
	info, err := r.DB.RoomInfo(ctx, roomID.String())
	if err != nil {
		return false, fmt.Errorf("failed to load RoomInfo: %w", err)
	}
	return (info != nil && !info.IsStub()), nil
}

func (r *Inviter) StateQuerier() xtools.StateQuerier {
	return &QueryState{Database: r.DB}
}

func (r *Inviter) ProcessInviteMembership(
	ctx context.Context, inviteEvent *types.HeaderedEvent,
) ([]api.OutputEvent, error) {
	var outputUpdates []api.OutputEvent
	var updater *shared.MembershipUpdater

	validRoomID, err := spec.NewRoomID(inviteEvent.RoomID())
	if err != nil {
		return nil, err
	}
	userID, err := r.RSAPI.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(*inviteEvent.StateKey()))
	if err != nil {
		return nil, api.ErrInvalidID{Err: fmt.Errorf("the user ID %s is invalid", *inviteEvent.StateKey())}
	}
	isTargetLocal := r.Cfg.Matrix.IsLocalServerName(userID.Domain())
	if updater, err = r.DB.MembershipUpdater(ctx, inviteEvent.RoomID(), *inviteEvent.StateKey(), isTargetLocal, inviteEvent.Version()); err != nil {
		return nil, fmt.Errorf("r.DB.MembershipUpdater: %w", err)
	}
	outputUpdates, err = helpers.UpdateToInviteMembership(updater, &types.Event{
		EventNID: 0,
		PDU:      inviteEvent.PDU,
	}, outputUpdates, inviteEvent.Version())
	if err != nil {
		return nil, fmt.Errorf("updateToInviteMembership: %w", err)
	}
	if err = updater.Commit(); err != nil {
		return nil, fmt.Errorf("updater.Commit: %w", err)
	}
	return outputUpdates, nil
}

// nolint:gocyclo
func (r *Inviter) PerformInvite(
	ctx context.Context,
	req *api.PerformInviteRequest,
) error {
	senderID, err := r.RSAPI.QuerySenderIDForUser(ctx, req.InviteInput.RoomID, req.InviteInput.Inviter)
	if err != nil {
		return err
	} else if senderID == nil {
		return fmt.Errorf("sender ID not found for %s in %s", req.InviteInput.Inviter, req.InviteInput.RoomID)
	}
	info, err := r.DB.RoomInfo(ctx, req.InviteInput.RoomID.String())
	if err != nil {
		return err
	}

	proto := xtools.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.InviteInput.RoomID.String(),
		Type:     "m.room.member",
	}

	content := xtools.MemberContent{
		Membership:  spec.Invite,
		DisplayName: req.InviteInput.DisplayName,
		AvatarURL:   req.InviteInput.AvatarURL,
		Reason:      req.InviteInput.Reason,
		IsDirect:    req.InviteInput.IsDirect,
	}

	if err = proto.SetContent(content); err != nil {
		return err
	}

	if !r.Cfg.Matrix.IsLocalServerName(req.InviteInput.Inviter.Domain()) {
		return api.ErrInvalidID{Err: fmt.Errorf("the invite must be from a local user")}
	}

	isTargetLocal := r.Cfg.Matrix.IsLocalServerName(req.InviteInput.Invitee.Domain())

	signingKey := req.InviteInput.PrivateKey
	if info.RoomVersion == xtools.RoomVersionPseudoIDs {
		signingKey, err = r.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, req.InviteInput.Inviter, req.InviteInput.RoomID)
		if err != nil {
			return err
		}
	}

	input := xtools.PerformInviteInput{
		RoomID:            req.InviteInput.RoomID,
		RoomVersion:       info.RoomVersion,
		Inviter:           req.InviteInput.Inviter,
		Invitee:           req.InviteInput.Invitee,
		IsTargetLocal:     isTargetLocal,
		EventTemplate:     proto,
		StrippedState:     req.InviteRoomState,
		KeyID:             req.InviteInput.KeyID,
		SigningKey:        signingKey,
		EventTime:         req.InviteInput.EventTime,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: r.RSAPI},
		StateQuerier:      &QueryState{r.DB, r.RSAPI},
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.RSAPI.QueryUserIDForSender(ctx, roomID, senderID)
		},
		SenderIDQuerier: func(roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
			return r.RSAPI.QuerySenderIDForUser(ctx, roomID, userID)
		},
		SenderIDCreator: func(ctx context.Context, userID spec.UserID, roomID spec.RoomID, roomVersion string) (spec.SenderID, ed25519.PrivateKey, error) {
			key, keyErr := r.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
			if keyErr != nil {
				return "", nil, keyErr
			}

			return spec.SenderIDFromPseudoIDKey(key), key, nil
		},
		EventQuerier: func(ctx context.Context, roomID spec.RoomID, eventsNeeded []xtools.StateKeyTuple) (xtools.LatestEvents, error) {
			req := api.QueryLatestEventsAndStateRequest{RoomID: roomID.String(), StateToFetch: eventsNeeded}
			res := api.QueryLatestEventsAndStateResponse{}
			err = r.RSAPI.QueryLatestEventsAndState(ctx, &req, &res)
			if err != nil {
				return xtools.LatestEvents{}, nil
			}

			stateEvents := []xtools.PDU{}
			for _, event := range res.StateEvents {
				stateEvents = append(stateEvents, event.PDU)
			}
			return xtools.LatestEvents{
				RoomExists:   res.RoomExists,
				StateEvents:  stateEvents,
				PrevEventIDs: res.LatestEvents,
				Depth:        res.Depth,
			}, nil
		},
		StoreSenderIDFromPublicID: func(ctx context.Context, senderID spec.SenderID, userIDRaw string, roomID spec.RoomID) error {
			storeUserID, userErr := spec.NewUserID(userIDRaw, true)
			if userErr != nil {
				return userErr
			}
			return r.RSAPI.StoreUserRoomPublicKey(ctx, senderID, *storeUserID, roomID)
		},
	}

	inviteEvent, err := xtools.PerformInvite(ctx, input, r.FSAPI)
	if err != nil {
		switch e := err.(type) {
		case spec.MatrixError:
			if e.ErrCode == spec.ErrorForbidden {
				return api.ErrNotAllowed{Err: fmt.Errorf("%s", e.Err)}
			}
		}
		return err
	}

	// Send the invite event to the roomserver input stream. This will
	// notify existing users in the room about the invite, update the
	// membership table and ensure that the event is ready and available
	// to use as an auth event when accepting the invite.
	// It will NOT notify the invitee of this invite.
	inputReq := &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{
			{
				Kind:         api.KindNew,
				Event:        &types.HeaderedEvent{PDU: inviteEvent},
				Origin:       req.InviteInput.Inviter.Domain(),
				SendAsServer: req.SendAsServer,
			},
		},
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(context.Background(), inputReq, inputRes)
	if err := inputRes.Err(); err != nil {
		xutil.GetLogger(ctx).WithField("event_id", inviteEvent.EventID()).Error("r.InputRoomEvents failed")
		return api.ErrNotAllowed{Err: err}
	}

	return nil
}
