package perform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type Upgrader struct {
	Cfg    *config.DataFrame
	URSAPI api.DataframeInternalAPI
}

// PerformFrameUpgrade upgrades a frame from one version to another
func (r *Upgrader) PerformFrameUpgrade(
	ctx context.Context,
	frameID string, userID spec.UserID, frameVersion xtools.FrameVersion,
) (newFrameID string, err error) {
	return r.performFrameUpgrade(ctx, frameID, userID, frameVersion)
}

func (r *Upgrader) performFrameUpgrade(
	ctx context.Context,
	frameID string, userID spec.UserID, frameVersion xtools.FrameVersion,
) (string, error) {
	evTime := time.Now()

	// Return an immediate error if the frame does not exist
	if err := r.validateFrameExists(ctx, frameID); err != nil {
		return "", err
	}

	fullFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return "", err
	}
	senderID, err := r.URSAPI.QuerySenderIDForUser(ctx, *fullFrameID, userID)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("failed getting senderID for user")
		return "", err
	} else if senderID == nil {
		xutil.GetLogger(ctx).WithField("userID", userID).WithField("frameID", *fullFrameID).Error("No senderID for user")
		return "", fmt.Errorf("no sender ID for %s in %s", userID, *fullFrameID)
	}

	// 1. Check if the user is authorized to actually perform the upgrade (can send m.frame.tombstone)
	if !r.userIsAuthorized(ctx, *senderID, frameID) {
		return "", api.ErrNotAllowed{Err: fmt.Errorf("you don't have permission to upgrade the frame, power level too low")}
	}

	// TODO (#267): Check frame ID doesn't clash with an existing one, and we
	//              probably shouldn't be using pseudo-random strings, maybe GUIDs?
	newFrameID := fmt.Sprintf("!%s:%s", xutil.RandomString(16), userID.Domain())

	// Get the existing frame state for the old frame.
	oldFrameReq := &api.QueryLatestEventsAndStateRequest{
		FrameID: frameID,
	}
	oldFrameRes := &api.QueryLatestEventsAndStateResponse{}
	if err := r.URSAPI.QueryLatestEventsAndState(ctx, oldFrameReq, oldFrameRes); err != nil {
		return "", fmt.Errorf("failed to get latest state: %s", err)
	}

	// Make the tombstone event
	tombstoneEvent, pErr := r.makeTombstoneEvent(ctx, evTime, *senderID, userID.Domain(), frameID, newFrameID)
	if pErr != nil {
		return "", pErr
	}

	// Generate the initial events we need to send into the new frame. This includes copied state events and bans
	// as well as the power level events needed to set up the frame
	eventsToMake, pErr := r.generateInitialEvents(ctx, oldFrameRes, *senderID, frameID, frameVersion, tombstoneEvent)
	if pErr != nil {
		return "", pErr
	}

	// Send the setup events to the new frame
	if pErr = r.sendInitialEvents(ctx, evTime, *senderID, userID.Domain(), newFrameID, frameVersion, eventsToMake); pErr != nil {
		return "", pErr
	}

	// 5. Send the tombstone event to the old frame
	if pErr = r.sendHeaderedEvent(ctx, userID.Domain(), tombstoneEvent, string(userID.Domain())); pErr != nil {
		return "", pErr
	}

	// If the old frame was public, make sure the new one is too
	if pErr = r.publishIfOldFrameWasPublic(ctx, frameID, newFrameID); pErr != nil {
		return "", pErr
	}

	// If the old frame had a canonical alias event, it should be deleted in the old frame
	if pErr = r.clearOldCanonicalAliasEvent(ctx, oldFrameRes, evTime, *senderID, userID.Domain(), frameID); pErr != nil {
		return "", pErr
	}

	// 4. Move local aliases to the new frame
	if pErr = moveLocalAliases(ctx, frameID, newFrameID, *senderID, r.URSAPI); pErr != nil {
		return "", pErr
	}

	// 6. Restrict power levels in the old frame
	if pErr = r.restrictOldFramePowerLevels(ctx, evTime, *senderID, userID.Domain(), frameID); pErr != nil {
		return "", pErr
	}

	return newFrameID, nil
}

func (r *Upgrader) getFramePowerLevels(ctx context.Context, frameID string) (*xtools.PowerLevelContent, error) {
	oldPowerLevelsEvent := api.GetStateEvent(ctx, r.URSAPI, frameID, xtools.StateKeyTuple{
		EventType: spec.MFramePowerLevels,
		StateKey:  "",
	})
	return oldPowerLevelsEvent.PowerLevels()
}

func (r *Upgrader) restrictOldFramePowerLevels(ctx context.Context, evTime time.Time, senderID spec.SenderID, userDomain spec.ServerName, frameID string) error {
	restrictedPowerLevelContent, pErr := r.getFramePowerLevels(ctx, frameID)
	if pErr != nil {
		return pErr
	}

	// If possible, the power levels in the old frame should also be modified to
	// prevent sending of events and inviting new users. For example, setting
	// events_default and invite to the greater of 50 and users_default + 1.
	restrictedDefaultPowerLevel := int64(50)
	if restrictedPowerLevelContent.UsersDefault+1 > restrictedDefaultPowerLevel {
		restrictedDefaultPowerLevel = restrictedPowerLevelContent.UsersDefault + 1
	}
	restrictedPowerLevelContent.EventsDefault = restrictedDefaultPowerLevel
	restrictedPowerLevelContent.Invite = restrictedDefaultPowerLevel

	restrictedPowerLevelsHeadered, resErr := r.makeHeaderedEvent(ctx, evTime, senderID, userDomain, frameID, xtools.FledglingEvent{
		Type:     spec.MFramePowerLevels,
		StateKey: "",
		Content:  restrictedPowerLevelContent,
	})

	switch resErr.(type) {
	case api.ErrNotAllowed:
		xutil.GetLogger(ctx).WithField(logrus.ErrorKey, resErr).Warn("UpgradeFrame: Could not restrict power levels in old frame")
	case nil:
		return r.sendHeaderedEvent(ctx, userDomain, restrictedPowerLevelsHeadered, api.DoNotSendToOtherServers)
	default:
		return resErr
	}
	return nil
}

func moveLocalAliases(ctx context.Context,
	frameID, newFrameID string, senderID spec.SenderID,
	URSAPI api.DataframeInternalAPI,
) (err error) {

	aliasReq := api.GetAliasesForFrameIDRequest{FrameID: frameID}
	aliasRes := api.GetAliasesForFrameIDResponse{}
	if err = URSAPI.GetAliasesForFrameID(ctx, &aliasReq, &aliasRes); err != nil {
		return fmt.Errorf("failed to get old frame aliases: %w", err)
	}

	// TDO: this should be spec.FrameID further up the call stack
	parsedNewFrameID, err := spec.NewFrameID(newFrameID)
	if err != nil {
		return err
	}

	for _, alias := range aliasRes.Aliases {
		aliasFound, aliasRemoved, err := URSAPI.RemoveFrameAlias(ctx, senderID, alias)
		if err != nil {
			return fmt.Errorf("failed to remove old frame alias: %w", err)
		} else if !aliasFound {
			return fmt.Errorf("failed to remove old frame alias: alias not found, possible race")
		} else if !aliasRemoved {
			return fmt.Errorf("failed to remove old alias")
		}

		aliasAlreadyExists, err := URSAPI.SetFrameAlias(ctx, senderID, *parsedNewFrameID, alias)
		if err != nil {
			return fmt.Errorf("failed to set new frame alias: %w", err)
		} else if aliasAlreadyExists {
			return fmt.Errorf("failed to set new frame alias: alias exists when it should have just been removed")
		}
	}
	return nil
}

func (r *Upgrader) clearOldCanonicalAliasEvent(ctx context.Context, oldFrame *api.QueryLatestEventsAndStateResponse, evTime time.Time, senderID spec.SenderID, userDomain spec.ServerName, frameID string) error {
	for _, event := range oldFrame.StateEvents {
		if event.Type() != spec.MFrameCanonicalAlias || !event.StateKeyEquals("") {
			continue
		}
		var aliasContent struct {
			Alias      string   `json:"alias"`
			AltAliases []string `json:"alt_aliases"`
		}
		if err := json.Unmarshal(event.Content(), &aliasContent); err != nil {
			return fmt.Errorf("failed to unmarshal canonical aliases: %w", err)
		}
		if aliasContent.Alias == "" && len(aliasContent.AltAliases) == 0 {
			// There are no canonical aliases to clear, therefore do nothing.
			return nil
		}
	}

	emptyCanonicalAliasEvent, resErr := r.makeHeaderedEvent(ctx, evTime, senderID, userDomain, frameID, xtools.FledglingEvent{
		Type:    spec.MFrameCanonicalAlias,
		Content: map[string]interface{}{},
	})
	switch resErr.(type) {
	case api.ErrNotAllowed:
		xutil.GetLogger(ctx).WithField(logrus.ErrorKey, resErr).Warn("UpgradeFrame: Could not set empty canonical alias event in old frame")
	case nil:
		return r.sendHeaderedEvent(ctx, userDomain, emptyCanonicalAliasEvent, api.DoNotSendToOtherServers)
	default:
		return resErr
	}
	return nil
}

func (r *Upgrader) publishIfOldFrameWasPublic(ctx context.Context, frameID, newFrameID string) error {
	// check if the old frame was published
	var pubQueryRes api.QueryPublishedFramesResponse
	err := r.URSAPI.QueryPublishedFrames(ctx, &api.QueryPublishedFramesRequest{
		FrameID: frameID,
	}, &pubQueryRes)
	if err != nil {
		return err
	}

	// if the old frame is published (was public), publish the new frame
	if len(pubQueryRes.FrameIDs) == 1 {
		publishNewFrameAndUnpublishOldFrame(ctx, r.URSAPI, frameID, newFrameID)
	}
	return nil
}

func publishNewFrameAndUnpublishOldFrame(
	ctx context.Context,
	URSAPI api.DataframeInternalAPI,
	oldFrameID, newFrameID string,
) {
	// expose this frame in the published frame list
	if err := URSAPI.PerformPublish(ctx, &api.PerformPublishRequest{
		FrameID:     newFrameID,
		Visibility: spec.Public,
	}); err != nil {
		// treat as non-fatal since the frame is already made by this point
		xutil.GetLogger(ctx).WithError(err).Error("failed to publish frame")
	}

	// remove the old frame from the published frame list
	if err := URSAPI.PerformPublish(ctx, &api.PerformPublishRequest{
		FrameID:     oldFrameID,
		Visibility: "private",
	}); err != nil {
		// treat as non-fatal since the frame is already made by this point
		xutil.GetLogger(ctx).WithError(err).Error("failed to un-publish frame")
	}
}

func (r *Upgrader) validateFrameExists(ctx context.Context, frameID string) error {
	if _, err := r.URSAPI.QueryFrameVersionForFrame(ctx, frameID); err != nil {
		return eventutil.ErrFrameNoExists{}
	}
	return nil
}

func (r *Upgrader) userIsAuthorized(ctx context.Context, senderID spec.SenderID, frameID string,
) bool {
	plEvent := api.GetStateEvent(ctx, r.URSAPI, frameID, xtools.StateKeyTuple{
		EventType: spec.MFramePowerLevels,
		StateKey:  "",
	})
	if plEvent == nil {
		return false
	}
	pl, err := plEvent.PowerLevels()
	if err != nil {
		return false
	}
	// Check for power level required to send tombstone event (marks the current frame as obsolete),
	// if not found, use the StateDefault power level
	return pl.UserLevel(senderID) >= pl.EventLevel("m.frame.tombstone", true)
}

// nolint:gocyclo
func (r *Upgrader) generateInitialEvents(ctx context.Context, oldFrame *api.QueryLatestEventsAndStateResponse, senderID spec.SenderID, frameID string, newVersion xtools.FrameVersion, tombstoneEvent *types.HeaderedEvent) ([]xtools.FledglingEvent, error) {
	state := make(map[xtools.StateKeyTuple]*types.HeaderedEvent, len(oldFrame.StateEvents))
	for _, event := range oldFrame.StateEvents {
		if event.StateKey() == nil {
			// This shouldn't ever happen, but better to be safe than sorry.
			continue
		}
		if event.Type() == spec.MFrameMember && !event.StateKeyEquals(string(senderID)) {
			// With the exception of bans which we do want to copy, we
			// should ignore membership events that aren't our own, as event auth will
			// prevent us from being able to create membership events on behalf of other
			// users anyway unless they are invites or bans.
			membership, err := event.Membership()
			if err != nil {
				continue
			}
			switch membership {
			case spec.Ban:
			default:
				continue
			}
		}
		// skip events that rely on a specific user being present
		// TDO: What to do here for pseudoIDs? It's checking non-member events for state keys with userIDs.
		sKey := *event.StateKey()
		if event.Type() != spec.MFrameMember && len(sKey) > 0 && sKey[:1] == "@" {
			continue
		}
		state[xtools.StateKeyTuple{EventType: event.Type(), StateKey: *event.StateKey()}] = event
	}

	// The following events are ones that we are going to override manually
	// in the following section.
	override := map[xtools.StateKeyTuple]struct{}{
		{EventType: spec.MFrameCreate, StateKey: ""}:               {},
		{EventType: spec.MFrameMember, StateKey: string(senderID)}: {},
		{EventType: spec.MFramePowerLevels, StateKey: ""}:          {},
		{EventType: spec.MFrameJoinRules, StateKey: ""}:            {},
	}

	// The overridden events are essential events that must be present in the
	// old frame state. Check that they are there.
	for tuple := range override {
		if _, ok := state[tuple]; !ok {
			return nil, fmt.Errorf("essential event of type %q state key %q is missing", tuple.EventType, tuple.StateKey)
		}
	}

	oldCreateEvent := state[xtools.StateKeyTuple{EventType: spec.MFrameCreate, StateKey: ""}]
	oldMembershipEvent := state[xtools.StateKeyTuple{EventType: spec.MFrameMember, StateKey: string(senderID)}]
	oldPowerLevelsEvent := state[xtools.StateKeyTuple{EventType: spec.MFramePowerLevels, StateKey: ""}]
	oldJoinRulesEvent := state[xtools.StateKeyTuple{EventType: spec.MFrameJoinRules, StateKey: ""}]

	// Create the new frame create event. Using a map here instead of CreateContent
	// means that we preserve any other interesting fields that might be present
	// in the create event (such as for the frame types MSC).
	newCreateContent := map[string]interface{}{}
	_ = json.Unmarshal(oldCreateEvent.Content(), &newCreateContent)
	newCreateContent["creator"] = string(senderID)
	newCreateContent["frame_version"] = newVersion
	newCreateContent["predecessor"] = xtools.PreviousFrame{
		EventID: tombstoneEvent.EventID(),
		FrameID:  frameID,
	}
	newCreateEvent := xtools.FledglingEvent{
		Type:     spec.MFrameCreate,
		StateKey: "",
		Content:  newCreateContent,
	}

	// Now create the new membership event. Same rules apply as above, so
	// that we preserve fields we don't otherwise know about. We'll always
	// set the membership to join though, because that is necessary to auth
	// the events after it.
	newMembershipContent := map[string]interface{}{}
	_ = json.Unmarshal(oldMembershipEvent.Content(), &newMembershipContent)
	newMembershipContent["membership"] = spec.Join
	newMembershipEvent := xtools.FledglingEvent{
		Type:     spec.MFrameMember,
		StateKey: string(senderID),
		Content:  newMembershipContent,
	}

	// We might need to temporarily give ourselves a higher power level
	// than we had in the old frame in order to be able to send all of
	// the relevant state events. This function will return whether we
	// had to override the power level events or not — if we did, we
	// need to send the original power levels again later on.
	powerLevelContent, err := oldPowerLevelsEvent.PowerLevels()
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error()
		return nil, fmt.Errorf("power level event content was invalid")
	}

	tempPowerLevelsEvent, powerLevelsOverridden := createTemporaryPowerLevels(powerLevelContent, senderID)

	// Now do the join rules event, same as the create and membership
	// events. We'll set a sane default of "invite" so that if the
	// existing join rules contains garbage, the frame can still be
	// upgraded.
	newJoinRulesContent := map[string]interface{}{
		"join_rule": spec.Invite, // sane default
	}
	_ = json.Unmarshal(oldJoinRulesEvent.Content(), &newJoinRulesContent)
	newJoinRulesEvent := xtools.FledglingEvent{
		Type:     spec.MFrameJoinRules,
		StateKey: "",
		Content:  newJoinRulesContent,
	}

	eventsToMake := make([]xtools.FledglingEvent, 0, len(state))
	eventsToMake = append(
		eventsToMake, newCreateEvent, newMembershipEvent,
		tempPowerLevelsEvent, newJoinRulesEvent,
	)

	// For some reason Sytest expects there to be a guest access event.
	// Create one if it doesn't exist.
	if _, ok := state[xtools.StateKeyTuple{EventType: spec.MFrameGuestAccess, StateKey: ""}]; !ok {
		eventsToMake = append(eventsToMake, xtools.FledglingEvent{
			Type: spec.MFrameGuestAccess,
			Content: map[string]string{
				"guest_access": "forbidden",
			},
		})
	}

	// Duplicate all of the old state events into the new frame.
	for tuple, event := range state {
		if _, ok := override[tuple]; ok {
			// Don't duplicate events we have overridden already. They
			// are already in `eventsToMake`.
			continue
		}
		newEvent := xtools.FledglingEvent{
			Type:     tuple.EventType,
			StateKey: tuple.StateKey,
		}
		if err = json.Unmarshal(event.Content(), &newEvent.Content); err != nil {
			logrus.WithError(err).Error("failed to unmarshal old event")
			continue
		}
		eventsToMake = append(eventsToMake, newEvent)
	}

	// If we sent a temporary power level event into the frame before,
	// override that now by restoring the original power levels.
	if powerLevelsOverridden {
		eventsToMake = append(eventsToMake, xtools.FledglingEvent{
			Type:    spec.MFramePowerLevels,
			Content: powerLevelContent,
		})
	}
	return eventsToMake, nil
}

func (r *Upgrader) sendInitialEvents(ctx context.Context, evTime time.Time, senderID spec.SenderID, userDomain spec.ServerName, newFrameID string, newVersion xtools.FrameVersion, eventsToMake []xtools.FledglingEvent) error {
	var err error
	var builtEvents []*types.HeaderedEvent
	authEvents := xtools.NewAuthEvents(nil)
	for i, e := range eventsToMake {
		depth := i + 1 // depth starts at 1

		proto := xtools.ProtoEvent{
			SenderID: string(senderID),
			FrameID:   newFrameID,
			Type:     e.Type,
			StateKey: &e.StateKey,
			Depth:    int64(depth),
		}
		err = proto.SetContent(e.Content)
		if err != nil {
			return fmt.Errorf("failed to set content of new %q event: %w", proto.Type, err)
		}
		if i > 0 {
			proto.PrevEvents = []string{builtEvents[i-1].EventID()}
		}

		var verImpl xtools.IFrameVersion
		verImpl, err = xtools.GetFrameVersion(newVersion)
		if err != nil {
			return err
		}
		builder := verImpl.NewEventBuilderFromProtoEvent(&proto)
		if err = builder.AddAuthEvents(&authEvents); err != nil {
			return err
		}

		var event xtools.PDU
		event, err = builder.Build(evTime, userDomain, r.Cfg.Matrix.KeyID, r.Cfg.Matrix.PrivateKey)
		if err != nil {
			return fmt.Errorf("failed to build new %q event: %w", builder.Type, err)

		}

		if err = xtools.Allowed(event, &authEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.URSAPI.QueryUserIDForSender(ctx, frameID, senderID)
		}); err != nil {
			return fmt.Errorf("failed to auth new %q event: %w", builder.Type, err)
		}

		// Add the event to the list of auth events
		builtEvents = append(builtEvents, &types.HeaderedEvent{PDU: event})
		err = authEvents.AddEvent(event)
		if err != nil {
			return fmt.Errorf("failed to add new %q event to auth set: %w", builder.Type, err)
		}
	}

	inputs := make([]api.InputFrameEvent, 0, len(builtEvents))
	for _, event := range builtEvents {
		inputs = append(inputs, api.InputFrameEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       userDomain,
			SendAsServer: api.DoNotSendToOtherServers,
		})
	}
	if err = api.SendInputFrameEvents(ctx, r.URSAPI, userDomain, inputs, false); err != nil {
		return fmt.Errorf("failed to send new frame %q to dataframe: %w", newFrameID, err)
	}
	return nil
}

func (r *Upgrader) makeTombstoneEvent(
	ctx context.Context,
	evTime time.Time,
	senderID spec.SenderID, senderDomain spec.ServerName, frameID, newFrameID string,
) (*types.HeaderedEvent, error) {
	content := map[string]interface{}{
		"body":             "This frame has been replaced",
		"replacement_frame": newFrameID,
	}
	event := xtools.FledglingEvent{
		Type:    "m.frame.tombstone",
		Content: content,
	}
	return r.makeHeaderedEvent(ctx, evTime, senderID, senderDomain, frameID, event)
}

func (r *Upgrader) makeHeaderedEvent(ctx context.Context, evTime time.Time, senderID spec.SenderID, senderDomain spec.ServerName, frameID string, event xtools.FledglingEvent) (*types.HeaderedEvent, error) {
	proto := xtools.ProtoEvent{
		SenderID: string(senderID),
		FrameID:   frameID,
		Type:     event.Type,
		StateKey: &event.StateKey,
	}
	err := proto.SetContent(event.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to set new %q event content: %w", proto.Type, err)
	}
	// Get the sender domain.
	identity, err := r.Cfg.Matrix.SigningIdentityFor(senderDomain)
	if err != nil {
		return nil, fmt.Errorf("failed to get signing identity for %q: %w", senderDomain, err)
	}
	var queryRes api.QueryLatestEventsAndStateResponse
	headeredEvent, err := eventutil.QueryAndBuildEvent(ctx, &proto, identity, evTime, r.URSAPI, &queryRes)
	switch e := err.(type) {
	case nil:
	case eventutil.ErrFrameNoExists:
		return nil, e
	case xtools.BadJSONError:
		return nil, e
	case xtools.EventValidationError:
		return nil, e
	default:
		return nil, fmt.Errorf("failed to build new %q event: %w", proto.Type, err)
	}

	// check to see if this user can perform this operation
	stateEvents := make([]xtools.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider := xtools.NewAuthEvents(stateEvents)
	if err = xtools.Allowed(headeredEvent.PDU, &provider, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.URSAPI.QueryUserIDForSender(ctx, frameID, senderID)
	}); err != nil {
		return nil, api.ErrNotAllowed{Err: fmt.Errorf("failed to auth new %q event: %w", proto.Type, err)} // TDO: Is this error string comprehensible to the client?
	}

	return headeredEvent, nil
}

func createTemporaryPowerLevels(powerLevelContent *xtools.PowerLevelContent, senderID spec.SenderID) (xtools.FledglingEvent, bool) {
	// Work out what power level we need in order to be able to send events
	// of all types into the frame.
	neededPowerLevel := powerLevelContent.StateDefault
	for _, powerLevel := range powerLevelContent.Events {
		if powerLevel > neededPowerLevel {
			neededPowerLevel = powerLevel
		}
	}

	// Make a copy of the existing power level content.
	tempPowerLevelContent := *powerLevelContent
	powerLevelsOverridden := false

	// At this point, the "Users", "Events" and "Notifications" keys are all
	// pointing to the map of the original PL content, so we will specifically
	// override the users map with a new one and duplicate the values deeply,
	// so that we can modify them without modifying the original.
	tempPowerLevelContent.Users = make(map[string]int64, len(powerLevelContent.Users))
	for key, value := range powerLevelContent.Users {
		tempPowerLevelContent.Users[key] = value
	}

	// If the user who is upgrading the frame doesn't already have sufficient
	// power, then elevate their power levels.
	if tempPowerLevelContent.UserLevel(senderID) < neededPowerLevel {
		tempPowerLevelContent.Users[string(senderID)] = neededPowerLevel
		powerLevelsOverridden = true
	}

	// Then return the temporary power levels event.
	return xtools.FledglingEvent{
		Type:    spec.MFramePowerLevels,
		Content: tempPowerLevelContent,
	}, powerLevelsOverridden
}

func (r *Upgrader) sendHeaderedEvent(
	ctx context.Context,
	serverName spec.ServerName,
	headeredEvent *types.HeaderedEvent,
	sendAsServer string,
) error {
	var inputs []api.InputFrameEvent
	inputs = append(inputs, api.InputFrameEvent{
		Kind:         api.KindNew,
		Event:        headeredEvent,
		Origin:       serverName,
		SendAsServer: sendAsServer,
	})
	return api.SendInputFrameEvents(ctx, r.URSAPI, serverName, inputs, false)
}
