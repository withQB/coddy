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
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
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
	Cfg   *config.DataFrame
	RSAPI api.DataframeInternalAPI
}

// PerformCreateFrame handles all the steps necessary to create a new frame.
// nolint: gocyclo
func (c *Creator) PerformCreateFrame(ctx context.Context, userID spec.UserID, frameID spec.FrameID, createRequest *api.PerformCreateFrameRequest) (string, *xutil.JSONResponse) {
	verImpl, err := xtools.GetFrameVersion(createRequest.FrameVersion)
	if err != nil {
		return "", &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("unknown frame version"),
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

	_, err = c.DB.AssignFrameNID(ctx, frameID, createRequest.FrameVersion)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed to assign frameNID")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var senderID spec.SenderID
	if createRequest.FrameVersion == xtools.FrameVersionPseudoIDs {
		// create user frame key if needed
		key, keyErr := c.RSAPI.GetOrCreateUserFramePrivateKey(ctx, userID, frameID)
		if keyErr != nil {
			xutil.GetLogger(ctx).WithError(keyErr).Error("GetOrCreateUserFramePrivateKey failed")
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
	createContent["frame_version"] = createRequest.FrameVersion
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
		Type:    spec.MFrameCreate,
		Content: createContent,
	}
	powerLevelEvent := xtools.FledglingEvent{
		Type:    spec.MFramePowerLevels,
		Content: powerLevelContent,
	}
	joinRuleEvent := xtools.FledglingEvent{
		Type:    spec.MFrameJoinRules,
		Content: joinRuleContent,
	}
	historyVisibilityEvent := xtools.FledglingEvent{
		Type:    spec.MFrameHistoryVisibility,
		Content: historyVisibilityContent,
	}
	membershipEvent := xtools.FledglingEvent{
		Type:     spec.MFrameMember,
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

	// If we are creating a frame with pseudo IDs, create and sign the MXIDMapping
	if createRequest.FrameVersion == xtools.FrameVersionPseudoIDs {
		var pseudoIDKey ed25519.PrivateKey
		pseudoIDKey, err = c.RSAPI.GetOrCreateUserFramePrivateKey(ctx, userID, frameID)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("GetOrCreateUserFramePrivateKey failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		mapping := &xtools.MXIDMapping{
			UserFrameKey: spec.SenderIDFromPseudoIDKey(pseudoIDKey),
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

	if createRequest.FrameName != "" {
		nameEvent = &xtools.FledglingEvent{
			Type: spec.MFrameName,
			Content: eventutil.NameContent{
				Name: createRequest.FrameName,
			},
		}
	}

	if createRequest.Topic != "" {
		topicEvent = &xtools.FledglingEvent{
			Type: spec.MFrameTopic,
			Content: eventutil.TopicContent{
				Topic: createRequest.Topic,
			},
		}
	}

	if guestsCanJoin {
		guestAccessEvent = &xtools.FledglingEvent{
			Type: spec.MFrameGuestAccess,
			Content: eventutil.GuestAccessContent{
				GuestAccess: "can_join",
			},
		}
	}

	var frameAlias string
	if createRequest.FrameAliasName != "" {
		frameAlias = fmt.Sprintf("#%s:%s", createRequest.FrameAliasName, userID.Domain())
		// check it's free
		// TDO: This races but is better than nothing
		hasAliasReq := api.GetFrameIDForAliasRequest{
			Alias:              frameAlias,
			IncludeAppservices: false,
		}

		var aliasResp api.GetFrameIDForAliasResponse
		err = c.RSAPI.GetFrameIDForAlias(ctx, &hasAliasReq, &aliasResp)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("aliasAPI.GetFrameIDForAlias failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if aliasResp.FrameID != "" {
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.FrameInUse("Frame ID already exists."),
			}
		}

		aliasEvent = &xtools.FledglingEvent{
			Type: spec.MFrameCanonicalAlias,
			Content: eventutil.CanonicalAlias{
				Alias: frameAlias,
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
		case spec.MFrameCreate:
			continue

		case spec.MFramePowerLevels:
			powerLevelEvent = createRequest.InitialState[i]

		case spec.MFrameJoinRules:
			joinRuleEvent = createRequest.InitialState[i]

		case spec.MFrameHistoryVisibility:
			historyVisibilityEvent = createRequest.InitialState[i]

		case spec.MFrameGuestAccess:
			guestAccessEvent = &createRequest.InitialState[i]

		case spec.MFrameName:
			nameEvent = &createRequest.InitialState[i]

		case spec.MFrameTopic:
			topicEvent = &createRequest.InitialState[i]

		default:
			initialStateEvents = append(initialStateEvents, createRequest.InitialState[i])
		}
	}

	// send events into the frame in order of:
	//  1- m.frame.create
	//  2- frame creator join member
	//  3- m.frame.power_levels
	//  4- m.frame.join_rules
	//  5- m.frame.history_visibility
	//  6- m.frame.canonical_alias (opt)
	//  7- m.frame.guest_access (opt)
	//  8- other initial state items
	//  9- m.frame.name (opt)
	//  10- m.frame.topic (opt)
	//  11- invite events (opt) - with is_direct flag if applicable TODO
	//  12- 3pid invite events (opt) TODO
	// This differs from Synapse slightly. Synapse would vary the ordering of 3-7
	// depending on if those events were in "initial_state" or not. This made it
	// harder to reason about, hence sticking to a strict static ordering.
	// TDO: Synapse has txn/token ID on each event. Do we need to do this here?
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
		// TDO: bit of a chicken and egg problem here as the alias doesn't exist and cannot until we have made the frame.
		// This means we might fail creating the alias but say the canonical alias is something that doesn't exist.
		eventsToMake = append(eventsToMake, *aliasEvent)
	}

	// TDO: invite events
	// TDO: 3pid invite events

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
			FrameID:   frameID.String(),
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

		if err = xtools.Allowed(ev, &authEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return c.RSAPI.QueryUserIDForSender(ctx, frameID, senderID)
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

	inputs := make([]api.InputFrameEvent, 0, len(builtEvents))
	for _, event := range builtEvents {
		inputs = append(inputs, api.InputFrameEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       userID.Domain(),
			SendAsServer: api.DoNotSendToOtherServers,
		})
	}

	// send the events to the dataframe
	if err = api.SendInputFrameEvents(ctx, c.RSAPI, userID.Domain(), inputs, false); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("dataframeAPI.SendInputFrameEvents failed")
		return "", &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// TODO(#269): Reserve frame alias while we create the frame. This stops us
	// from creating the frame but still failing due to the alias having already
	// been taken.
	if frameAlias != "" {
		aliasAlreadyExists, aliasErr := c.RSAPI.SetFrameAlias(ctx, senderID, frameID, frameAlias)
		if aliasErr != nil {
			xutil.GetLogger(ctx).WithError(aliasErr).Error("aliasAPI.SetFrameAlias failed")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		if aliasAlreadyExists {
			return "", &xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.FrameInUse("Frame alias already exists."),
			}
		}
	}

	// If this is a direct message then we should invite the participants.
	if len(createRequest.InvitedUsers) > 0 {
		// Build some stripped state for the invite.
		var globalStrippedState []xtools.InviteStrippedState
		for _, event := range builtEvents {
			// Chosen events from the spec:
			switch event.Type() {
			case spec.MFrameCreate:
				fallthrough
			case spec.MFrameName:
				fallthrough
			case spec.MFrameAvatar:
				fallthrough
			case spec.MFrameTopic:
				fallthrough
			case spec.MFrameCanonicalAlias:
				fallthrough
			case spec.MFrameEncryption:
				fallthrough
			case spec.MFrameMember:
				fallthrough
			case spec.MFrameJoinRules:
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
					FrameID:      frameID,
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
				InviteFrameState: globalStrippedState,
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
		// expose this frame in the published frame list
		if err = c.RSAPI.PerformPublish(ctx, &api.PerformPublishRequest{
			FrameID:     frameID.String(),
			Visibility: spec.Public,
		}); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("failed to publish frame")
			return "", &xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// TDO: visibility/presets/raw initial state
	// TDO: Create frame alias association
	// Make sure this doesn't fall into an application service's namespace though!

	return frameAlias, nil
}
