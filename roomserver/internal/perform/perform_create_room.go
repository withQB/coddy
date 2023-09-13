// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perform

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/storage"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

const (
	historyVisibilityShared = "shared"
)

type Creator struct {
	DB    storage.Database
	Cfg   *config.RoomServer
	RSAPI api.RoomserverInternalAPI
}

// PerformCreateRoom handles all the steps necessary to create a new room.
// nolint: gocyclo
func (c *Creator) PerformCreateRoom(ctx context.Context, userID spec.UserID, roomID spec.RoomID, createRequest *api.PerformCreateRoomRequest) (string, *xutil.JSONResponse) {
	verImpl, err := xtools.GetRoomVersion(createRequest.RoomVersion)
	if err != nil {
		return "", &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("unknown room version"),
		}
	}

	createContent := map[string]interface{}{}
	if len(createRequest.CreationContent) > 0 {
		if err = json.Unmarshal(createRequest.CreationContent, &createContent); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("json.Unmarshal for creation_content failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("invalid create content"),
			}
		}
	}

	_, err = c.DB.AssignRoomNID(ctx, roomID, createRequest.RoomVersion)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to assign roomNID")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var senderID spec.SenderID
	if createRequest.RoomVersion == xtools.RoomVersionPseudoIDs {
		// create user room key if needed
		key, keyErr := c.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
		if keyErr != nil {
			xutil.GetLogger(ctx).WithError(keyErr).Error("GetOrCreateUserRoomPrivateKey failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		senderID = spec.SenderIDFromPseudoIDKey(key)
	} else {
		senderID = spec.SenderID(userID.String())
	}
	createContent["creator"] = senderID
	createContent["room_version"] = createRequest.RoomVersion
	powerLevelContent := eventutil.InitialPowerLevelsContent(string(senderID))
	joinRuleContent := xtools.JoinRuleContent{
		JoinRule: spec.Invite,
	}
	historyVisibilityContent := xtools.HistoryVisibilityContent{
		HistoryVisibility: historyVisibilityShared,
	}

	if createRequest.PowerLevelContentOverride != nil {
		// Merge powerLevelContentOverride fields by unmarshalling it atop the defaults
		err = json.Unmarshal(createRequest.PowerLevelContentOverride, &powerLevelContent)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("json.Unmarshal for power_level_content_override failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("malformed power_level_content_override"),
			}
		}
	}

	var guestsCanJoin bool
	switch createRequest.StatePreset {
	case spec.PresetPrivateChat:
		joinRuleContent.JoinRule = spec.Invite
		historyVisibilityContent.HistoryVisibility = historyVisibilityShared
		guestsCanJoin = true
	case spec.PresetTrustedPrivateChat:
		joinRuleContent.JoinRule = spec.Invite
		historyVisibilityContent.HistoryVisibility = historyVisibilityShared
		for _, invitee := range createRequest.InvitedUsers {
			powerLevelContent.Users[invitee] = 100
		}
		guestsCanJoin = true
	case spec.PresetPublicChat:
		joinRuleContent.JoinRule = spec.Public
		historyVisibilityContent.HistoryVisibility = historyVisibilityShared
	}

	createEvent := xtools.FledglingEvent{
		Type:    spec.MRoomCreate,
		Content: createContent,
	}
	powerLevelEvent := xtools.FledglingEvent{
		Type:    spec.MRoomPowerLevels,
		Content: powerLevelContent,
	}
	joinRuleEvent := xtools.FledglingEvent{
		Type:    spec.MRoomJoinRules,
		Content: joinRuleContent,
	}
	historyVisibilityEvent := xtools.FledglingEvent{
		Type:    spec.MRoomHistoryVisibility,
		Content: historyVisibilityContent,
	}
	membershipEvent := xtools.FledglingEvent{
		Type:     spec.MRoomMember,
		StateKey: string(senderID),
	}

	memberContent := xtools.MemberContent{
		Membership:  spec.Join,
		DisplayName: createRequest.UserDisplayName,
		AvatarURL:   createRequest.UserAvatarURL,
	}

	// get the signing identity
	identity, err := c.Cfg.Matrix.SigningIdentityFor(userID.Domain()) // we MUST use the server signing mxid_mapping
	if err != nil {
		logrus.WithError(err).WithField("domain", userID.Domain()).Error("unable to find signing identity for domain")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// If we are creating a room with pseudo IDs, create and sign the MXIDMapping
	if createRequest.RoomVersion == xtools.RoomVersionPseudoIDs {
		var pseudoIDKey ed25519.PrivateKey
		pseudoIDKey, err = c.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("GetOrCreateUserRoomPrivateKey failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		mapping := &xtools.MXIDMapping{
			UserRoomKey: spec.SenderIDFromPseudoIDKey(pseudoIDKey),
			UserID:      userID.String(),
		}

		// Sign the mapping with the server identity
		if err = mapping.Sign(identity.ServerName, identity.KeyID, identity.PrivateKey); err != nil {
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		memberContent.MXIDMapping = mapping

		// sign all events with the pseudo ID key
		identity = &fclient.SigningIdentity{
			ServerName: spec.ServerName(spec.SenderIDFromPseudoIDKey(pseudoIDKey)),
			KeyID:      "ed25519:1",
			PrivateKey: pseudoIDKey,
		}
	}
	membershipEvent.Content = memberContent

	var nameEvent *xtools.FledglingEvent
	var topicEvent *xtools.FledglingEvent
	var guestAccessEvent *xtools.FledglingEvent
	var aliasEvent *xtools.FledglingEvent

	if createRequest.RoomName != "" {
		nameEvent = &xtools.FledglingEvent{
			Type: spec.MRoomName,
			Content: eventutil.NameContent{
				Name: createRequest.RoomName,
			},
		}
	}

	if createRequest.Topic != "" {
		topicEvent = &xtools.FledglingEvent{
			Type: spec.MRoomTopic,
			Content: eventutil.TopicContent{
				Topic: createRequest.Topic,
			},
		}
	}

	if guestsCanJoin {
		guestAccessEvent = &xtools.FledglingEvent{
			Type: spec.MRoomGuestAccess,
			Content: eventutil.GuestAccessContent{
				GuestAccess: "can_join",
			},
		}
	}

	var roomAlias string
	if createRequest.RoomAliasName != "" {
		roomAlias = fmt.Sprintf("#%s:%s", createRequest.RoomAliasName, userID.Domain())
		// check it's free
		// TODO: This races but is better than nothing
		hasAliasReq := api.GetRoomIDForAliasRequest{
			Alias:              roomAlias,
			IncludeAppservices: false,
		}

		var aliasResp api.GetRoomIDForAliasResponse
		err = c.RSAPI.GetRoomIDForAlias(ctx, &hasAliasReq, &aliasResp)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("aliasAPI.GetRoomIDForAlias failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if aliasResp.RoomID != "" {
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.RoomInUse("Room ID already exists."),
			}
		}

		aliasEvent = &xtools.FledglingEvent{
			Type: spec.MRoomCanonicalAlias,
			Content: eventutil.CanonicalAlias{
				Alias: roomAlias,
			},
		}
	}

	var initialStateEvents []xtools.FledglingEvent
	for i := range createRequest.InitialState {
		if createRequest.InitialState[i].StateKey != "" {
			initialStateEvents = append(initialStateEvents, createRequest.InitialState[i])
			continue
		}

		switch createRequest.InitialState[i].Type {
		case spec.MRoomCreate:
			continue

		case spec.MRoomPowerLevels:
			powerLevelEvent = createRequest.InitialState[i]

		case spec.MRoomJoinRules:
			joinRuleEvent = createRequest.InitialState[i]

		case spec.MRoomHistoryVisibility:
			historyVisibilityEvent = createRequest.InitialState[i]

		case spec.MRoomGuestAccess:
			guestAccessEvent = &createRequest.InitialState[i]

		case spec.MRoomName:
			nameEvent = &createRequest.InitialState[i]

		case spec.MRoomTopic:
			topicEvent = &createRequest.InitialState[i]

		default:
			initialStateEvents = append(initialStateEvents, createRequest.InitialState[i])
		}
	}

	// send events into the room in order of:
	//  1- m.room.create
	//  2- room creator join member
	//  3- m.room.power_levels
	//  4- m.room.join_rules
	//  5- m.room.history_visibility
	//  6- m.room.canonical_alias (opt)
	//  7- m.room.guest_access (opt)
	//  8- other initial state items
	//  9- m.room.name (opt)
	//  10- m.room.topic (opt)
	//  11- invite events (opt) - with is_direct flag if applicable TODO
	//  12- 3pid invite events (opt) TODO
	// This differs from Synapse slightly. Synapse would vary the ordering of 3-7
	// depending on if those events were in "initial_state" or not. This made it
	// harder to reason about, hence sticking to a strict static ordering.
	// TODO: Synapse has txn/token ID on each event. Do we need to do this here?
	eventsToMake := []xtools.FledglingEvent{
		createEvent, membershipEvent, powerLevelEvent, joinRuleEvent, historyVisibilityEvent,
	}
	if guestAccessEvent != nil {
		eventsToMake = append(eventsToMake, *guestAccessEvent)
	}
	eventsToMake = append(eventsToMake, initialStateEvents...)
	if nameEvent != nil {
		eventsToMake = append(eventsToMake, *nameEvent)
	}
	if topicEvent != nil {
		eventsToMake = append(eventsToMake, *topicEvent)
	}
	if aliasEvent != nil {
		// TODO: bit of a chicken and egg problem here as the alias doesn't exist and cannot until we have made the room.
		// This means we might fail creating the alias but say the canonical alias is something that doesn't exist.
		eventsToMake = append(eventsToMake, *aliasEvent)
	}

	// TODO: invite events
	// TODO: 3pid invite events

	var builtEvents []*types.HeaderedEvent
	authEvents := xtools.NewAuthEvents(nil)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("rsapi.QuerySenderIDForUser failed")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	for i, e := range eventsToMake {
		depth := i + 1 // depth starts at 1

		builder := verImpl.NewEventBuilderFromProtoEvent(&xtools.ProtoEvent{
			SenderID: string(senderID),
			RoomID:   roomID.String(),
			Type:     e.Type,
			StateKey: &e.StateKey,
			Depth:    int64(depth),
		})
		err = builder.SetContent(e.Content)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("builder.SetContent failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if i > 0 {
			builder.PrevEvents = []string{builtEvents[i-1].EventID()}
		}
		var ev xtools.PDU
		if err = builder.AddAuthEvents(&authEvents); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("AddAuthEvents failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		ev, err = builder.Build(createRequest.EventTime, identity.ServerName, identity.KeyID, identity.PrivateKey)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("buildEvent failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		if err = xtools.Allowed(ev, &authEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return c.RSAPI.QueryUserIDForSender(ctx, roomID, senderID)
		}); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("xtools.Allowed failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		// Add the event to the list of auth events
		builtEvents = append(builtEvents, &types.HeaderedEvent{PDU: ev})
		err = authEvents.AddEvent(ev)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("authEvents.AddEvent failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	inputs := make([]api.InputRoomEvent, 0, len(builtEvents))
	for _, event := range builtEvents {
		inputs = append(inputs, api.InputRoomEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       userID.Domain(),
			SendAsServer: api.DoNotSendToOtherServers,
		})
	}

	// send the events to the roomserver
	if err = api.SendInputRoomEvents(ctx, c.RSAPI, userID.Domain(), inputs, false); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("roomserverAPI.SendInputRoomEvents failed")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// TODO(#269): Reserve room alias while we create the room. This stops us
	// from creating the room but still failing due to the alias having already
	// been taken.
	if roomAlias != "" {
		aliasAlreadyExists, aliasErr := c.RSAPI.SetRoomAlias(ctx, senderID, roomID, roomAlias)
		if aliasErr != nil {
			xutil.GetLogger(ctx).WithError(aliasErr).Error("aliasAPI.SetRoomAlias failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		if aliasAlreadyExists {
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.RoomInUse("Room alias already exists."),
			}
		}
	}

	// If this is a direct message then we should invite the participants.
	if len(createRequest.InvitedUsers) > 0 {
		// Build some stripped state for the invite.
		var globalStrippedState []xtools.InviteStrippedState
		for _, event := range builtEvents {
			// Chosen events from the spec:
			// https://spec.matrix.org/v1.3/client-server-api/#stripped-state
			switch event.Type() {
			case spec.MRoomCreate:
				fallthrough
			case spec.MRoomName:
				fallthrough
			case spec.MRoomAvatar:
				fallthrough
			case spec.MRoomTopic:
				fallthrough
			case spec.MRoomCanonicalAlias:
				fallthrough
			case spec.MRoomEncryption:
				fallthrough
			case spec.MRoomMember:
				fallthrough
			case spec.MRoomJoinRules:
				ev := event.PDU
				globalStrippedState = append(
					globalStrippedState,
					xtools.NewInviteStrippedState(ev),
				)
			}
		}

		// Process the invites.
		for _, invitee := range createRequest.InvitedUsers {
			inviteeUserID, userIDErr := spec.NewUserID(invitee, true)
			if userIDErr != nil {
				xutil.GetLogger(ctx).WithError(userIDErr).Error("invalid UserID")
				return "", &xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			err = c.RSAPI.PerformInvite(ctx, &api.PerformInviteRequest{
				InviteInput: api.InviteInput{
					RoomID:      roomID,
					Inviter:     userID,
					Invitee:     *inviteeUserID,
					DisplayName: createRequest.UserDisplayName,
					AvatarURL:   createRequest.UserAvatarURL,
					Reason:      "",
					IsDirect:    createRequest.IsDirect,
					KeyID:       createRequest.KeyID,
					PrivateKey:  createRequest.PrivateKey,
					EventTime:   createRequest.EventTime,
				},
				InviteRoomState: globalStrippedState,
				SendAsServer:    string(userID.Domain()),
			})
			switch e := err.(type) {
			case api.ErrInvalidID:
				return "", &xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.Unknown(e.Error()),
				}
			case api.ErrNotAllowed:
				return "", &xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden(e.Error()),
				}
			case nil:
			default:
				xutil.GetLogger(ctx).WithError(err).Error("PerformInvite failed")
				sentry.CaptureException(err)
				return "", &xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
	}

	if createRequest.Visibility == spec.Public {
		// expose this room in the published room list
		if err = c.RSAPI.PerformPublish(ctx, &api.PerformPublishRequest{
			RoomID:     roomID.String(),
			Visibility: spec.Public,
		}); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("failed to publish room")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// TODO: visibility/presets/raw initial state
	// TODO: Create room alias association
	// Make sure this doesn't fall into an application service's namespace though!

	return roomAlias, nil
}
