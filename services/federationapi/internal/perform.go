package internal

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/dataframe/version"
	"github.com/withqb/coddy/services/federationapi/api"
	"github.com/withqb/coddy/services/federationapi/consumers"
	"github.com/withqb/coddy/services/federationapi/statistics"
)

// PerformLeaveRequest implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformDirectoryLookup(
	ctx context.Context,
	request *api.PerformDirectoryLookupRequest,
	response *api.PerformDirectoryLookupResponse,
) (err error) {
	if !r.shouldAttemptDirectFederation(request.ServerName) {
		return fmt.Errorf("relay servers have no meaningful response for directory lookup.")
	}

	dir, err := r.federation.LookupFrameAlias(
		ctx,
		r.cfg.Coddy.ServerName,
		request.ServerName,
		request.FrameAlias,
	)
	if err != nil {
		r.statistics.ForServer(request.ServerName).Failure()
		return err
	}
	response.FrameID = dir.FrameID
	response.ServerNames = dir.Servers
	r.statistics.ForServer(request.ServerName).Success(statistics.SendDirect)
	return nil
}

type federatedJoin struct {
	UserID string
	FrameID string
}

// PerformJoin implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformJoin(
	ctx context.Context,
	request *api.PerformJoinRequest,
	response *api.PerformJoinResponse,
) {
	// Check that a join isn't already in progress for this user/frame.
	j := federatedJoin{request.UserID, request.FrameID}
	if _, found := r.joins.Load(j); found {
		response.LastError = &xcore.HTTPError{
			Code: 429,
			Message: `{
				"errcode": "M_LIMIT_EXCEEDED",
				"error": "There is already a federated join to this frame in progress. Please wait for it to finish."
			}`, // TDO: Why do none of our error types play nicely with each other?
		}
		return
	}
	r.joins.Store(j, nil)
	defer r.joins.Delete(j)

	// Deduplicate the server names we were provided but keep the ordering
	// as this encodes useful information about which servers are most likely
	// to respond.
	seenSet := make(map[spec.ServerName]bool)
	var uniqueList []spec.ServerName
	for _, srv := range request.ServerNames {
		if seenSet[srv] || r.cfg.Coddy.IsLocalServerName(srv) {
			continue
		}
		seenSet[srv] = true
		uniqueList = append(uniqueList, srv)
	}
	request.ServerNames = uniqueList

	// Try each server that we were provided until we land on one that
	// successfully completes the make-join send-join dance.
	var lastErr error
	for _, serverName := range request.ServerNames {
		if err := r.performJoinUsingServer(
			ctx,
			request.FrameID,
			request.UserID,
			request.Content,
			serverName,
			request.Unsigned,
		); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"server_name": serverName,
				"frame_id":     request.FrameID,
			}).Warnf("failed to join frame through server")
			lastErr = err
			continue
		}

		// We're all good.
		response.JoinedVia = serverName
		return
	}

	// If we reach here then we didn't complete a join for some reason.
	var httpErr xcore.HTTPError
	if ok := errors.As(lastErr, &httpErr); ok {
		httpErr.Message = string(httpErr.Contents)
		response.LastError = &httpErr
	} else {
		response.LastError = &xcore.HTTPError{
			Code:         0,
			WrappedError: nil,
			Message:      "Unknown HTTP error",
		}
		if lastErr != nil {
			response.LastError.Message = lastErr.Error()
		}
	}

	logrus.Errorf(
		"failed to join user %q to frame %q through %d server(s): last error %s",
		request.UserID, request.FrameID, len(request.ServerNames), lastErr,
	)
}

func (r *FederationInternalAPI) performJoinUsingServer(
	ctx context.Context,
	frameID, userID string,
	content map[string]interface{},
	serverName spec.ServerName,
	unsigned map[string]interface{},
) error {
	if !r.shouldAttemptDirectFederation(serverName) {
		return fmt.Errorf("relay servers have no meaningful response for join.")
	}

	user, err := spec.NewUserID(userID, true)
	if err != nil {
		return err
	}
	frame, err := spec.NewFrameID(frameID)
	if err != nil {
		return err
	}

	joinInput := xtools.PerformJoinInput{
		UserID:     user,
		FrameID:     frame,
		ServerName: serverName,
		Content:    content,
		Unsigned:   unsigned,
		PrivateKey: r.cfg.Coddy.PrivateKey,
		KeyID:      r.cfg.Coddy.KeyID,
		KeyRing:    r.keyRing,
		EventProvider: federatedEventProvider(ctx, r.federation, r.keyRing, user.Domain(), serverName, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		}),
		UserIDQuerier: func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		},
		GetOrCreateSenderID: func(ctx context.Context, userID spec.UserID, frameID spec.FrameID, frameVersion string) (spec.SenderID, ed25519.PrivateKey, error) {
			// assign a frameNID, otherwise we can't create a private key for the user
			_, nidErr := r.rsAPI.AssignFrameNID(ctx, frameID, xtools.FrameVersion(frameVersion))
			if nidErr != nil {
				return "", nil, nidErr
			}
			key, keyErr := r.rsAPI.GetOrCreateUserFramePrivateKey(ctx, userID, frameID)
			if keyErr != nil {
				return "", nil, keyErr
			}
			return spec.SenderIDFromPseudoIDKey(key), key, nil
		},
		StoreSenderIDFromPublicID: func(ctx context.Context, senderID spec.SenderID, userIDRaw string, frameID spec.FrameID) error {
			storeUserID, userErr := spec.NewUserID(userIDRaw, true)
			if userErr != nil {
				return userErr
			}
			return r.rsAPI.StoreUserFramePublicKey(ctx, senderID, *storeUserID, frameID)
		},
	}
	response, joinErr := xtools.PerformJoin(ctx, r, joinInput)

	if joinErr != nil {
		if !joinErr.Reachable {
			r.statistics.ForServer(joinErr.ServerName).Failure()
		} else {
			r.statistics.ForServer(joinErr.ServerName).Success(statistics.SendDirect)
		}
		return joinErr.Err
	}
	r.statistics.ForServer(serverName).Success(statistics.SendDirect)
	if response == nil {
		return fmt.Errorf("Received nil response from xtools.PerformJoin")
	}

	// We need to immediately update our list of joined hosts for this frame now as we are technically
	// joined. We must do this synchronously: we cannot rely on the dataframe output events as they
	// will happen asyncly. If we don't update this table, you can end up with bad failure modes like
	// joining a frame, waiting for 200 OK then changing device keys and have those keys not be sent
	// to other servers (this was a cause of a flakey sytest "Local device key changes get to remote servers")
	// The events are trusted now as we performed auth checks above.
	joinedHosts, err := consumers.JoinedHostsFromEvents(ctx, response.StateSnapshot.GetStateEvents().TrustedEvents(response.JoinEvent.Version(), false), r.rsAPI)
	if err != nil {
		return fmt.Errorf("JoinedHostsFromEvents: failed to get joined hosts: %s", err)
	}

	logrus.WithField("frame", frameID).Infof("Joined federated frame with %d hosts", len(joinedHosts))
	if _, err = r.db.UpdateFrame(context.Background(), frameID, joinedHosts, nil, true); err != nil {
		return fmt.Errorf("UpdatedFrame: failed to update frame with joined hosts: %s", err)
	}

	// TDO: Can I change this to not take respState but instead just take an opaque list of events?
	if err = dataframeAPI.SendEventWithState(
		context.Background(),
		r.rsAPI,
		user.Domain(),
		dataframeAPI.KindNew,
		response.StateSnapshot,
		&types.HeaderedEvent{PDU: response.JoinEvent},
		serverName,
		nil,
		false,
	); err != nil {
		return fmt.Errorf("dataframeAPI.SendEventWithState: %w", err)
	}
	return nil
}

// PerformOutboundPeekRequest implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformOutboundPeek(
	ctx context.Context,
	request *api.PerformOutboundPeekRequest,
	response *api.PerformOutboundPeekResponse,
) error {
	// Look up the supported frame versions.
	var supportedVersions []xtools.FrameVersion
	for version := range version.SupportedFrameVersions() {
		supportedVersions = append(supportedVersions, version)
	}

	// Deduplicate the server names we were provided but keep the ordering
	// as this encodes useful information about which servers are most likely
	// to respond.
	seenSet := make(map[spec.ServerName]bool)
	var uniqueList []spec.ServerName
	for _, srv := range request.ServerNames {
		if seenSet[srv] {
			continue
		}
		seenSet[srv] = true
		uniqueList = append(uniqueList, srv)
	}
	request.ServerNames = uniqueList

	// See if there's an existing outbound peek for this frame ID with
	// one of the specified servers.
	if peeks, err := r.db.GetOutboundPeeks(ctx, request.FrameID); err == nil {
		for _, peek := range peeks {
			if _, ok := seenSet[peek.ServerName]; ok {
				return nil
			}
		}
	}

	// Try each server that we were provided until we land on one that
	// successfully completes the peek
	var lastErr error
	for _, serverName := range request.ServerNames {
		if err := r.performOutboundPeekUsingServer(
			ctx,
			request.FrameID,
			serverName,
			supportedVersions,
		); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"server_name": serverName,
				"frame_id":     request.FrameID,
			}).Warnf("failed to peek frame through server")
			lastErr = err
			continue
		}

		// We're all good.
		return nil
	}

	// If we reach here then we didn't complete a peek for some reason.
	var httpErr xcore.HTTPError
	if ok := errors.As(lastErr, &httpErr); ok {
		httpErr.Message = string(httpErr.Contents)
		response.LastError = &httpErr
	} else {
		response.LastError = &xcore.HTTPError{
			Code:         0,
			WrappedError: nil,
			Message:      lastErr.Error(),
		}
	}

	logrus.Errorf(
		"failed to peek frame %q through %d server(s): last error %s",
		request.FrameID, len(request.ServerNames), lastErr,
	)

	return lastErr
}

func (r *FederationInternalAPI) performOutboundPeekUsingServer(
	ctx context.Context,
	frameID string,
	serverName spec.ServerName,
	supportedVersions []xtools.FrameVersion,
) error {
	if !r.shouldAttemptDirectFederation(serverName) {
		return fmt.Errorf("relay servers have no meaningful response for outbound peek.")
	}

	// create a unique ID for this peek.
	// for now we just use the frame ID again. In future, if we ever
	// support concurrent peeks to the same frame with different filters
	// then we would need to disambiguate further.
	peekID := frameID

	// check whether we're peeking already to try to avoid needlessly
	// re-peeking on the server. we don't need a transaction for this,
	// given this is a nice-to-have.
	outboundPeek, err := r.db.GetOutboundPeek(ctx, serverName, frameID, peekID)
	if err != nil {
		return err
	}
	renewing := false
	if outboundPeek != nil {
		nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
		if nowMilli > outboundPeek.RenewedTimestamp+outboundPeek.RenewalInterval {
			logrus.Infof("stale outbound peek to %s for %s already exists; renewing", serverName, frameID)
			renewing = true
		} else {
			logrus.Infof("live outbound peek to %s for %s already exists", serverName, frameID)
			return nil
		}
	}

	// Try to perform an outbound /peek using the information supplied in the
	// request.
	respPeek, err := r.federation.Peek(
		ctx,
		r.cfg.Coddy.ServerName,
		serverName,
		frameID,
		peekID,
		supportedVersions,
	)
	if err != nil {
		r.statistics.ForServer(serverName).Failure()
		return fmt.Errorf("r.federation.Peek: %w", err)
	}
	r.statistics.ForServer(serverName).Success(statistics.SendDirect)

	// Work out if we support the frame version that has been supplied in
	// the peek response.
	if respPeek.FrameVersion == "" {
		respPeek.FrameVersion = xtools.FrameVersionV1
	}
	if !xtools.KnownFrameVersion(respPeek.FrameVersion) {
		return fmt.Errorf("unknown frame version: %s", respPeek.FrameVersion)
	}

	// we have the peek state now so let's process regardless of whether upstream gives up
	ctx = context.Background()

	// authenticate the state returned (check its auth events etc)
	// the equivalent of CheckSendJoinResponse()
	userIDProvider := func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
	}
	authEvents, stateEvents, err := xtools.CheckStateResponse(
		ctx, &respPeek, respPeek.FrameVersion, r.keyRing, federatedEventProvider(ctx, r.federation, r.keyRing, r.cfg.Coddy.ServerName, serverName, userIDProvider), userIDProvider,
	)
	if err != nil {
		return fmt.Errorf("error checking state returned from peeking: %w", err)
	}
	if err = checkEventsContainCreateEvent(authEvents); err != nil {
		return fmt.Errorf("sanityCheckAuthChain: %w", err)
	}

	// If we've got this far, the remote server is peeking.
	if renewing {
		if err = r.db.RenewOutboundPeek(ctx, serverName, frameID, peekID, respPeek.RenewalInterval); err != nil {
			return err
		}
	} else {
		if err = r.db.AddOutboundPeek(ctx, serverName, frameID, peekID, respPeek.RenewalInterval); err != nil {
			return err
		}
	}

	// logrus.Warnf("got respPeek %#v", respPeek)
	// Send the newly returned state to the dataframe to update our local view.
	if err = dataframeAPI.SendEventWithState(
		ctx, r.rsAPI, r.cfg.Coddy.ServerName,
		dataframeAPI.KindNew,
		// use the authorized state from CheckStateResponse
		&fclient.RespState{
			StateEvents: xtools.NewEventJSONsFromEvents(stateEvents),
			AuthEvents:  xtools.NewEventJSONsFromEvents(authEvents),
		},
		&types.HeaderedEvent{PDU: respPeek.LatestEvent},
		serverName,
		nil,
		false,
	); err != nil {
		return fmt.Errorf("r.producer.SendEventWithState: %w", err)
	}

	return nil
}

// PerformLeaveRequest implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformLeave(
	ctx context.Context,
	request *api.PerformLeaveRequest,
	response *api.PerformLeaveResponse,
) (err error) {
	userID, err := spec.NewUserID(request.UserID, true)
	if err != nil {
		return err
	}

	// Deduplicate the server names we were provided.
	xutil.SortAndUnique(request.ServerNames)

	// Try each server that we were provided until we land on one that
	// successfully completes the make-leave send-leave dance.
	for _, serverName := range request.ServerNames {
		if !r.shouldAttemptDirectFederation(serverName) {
			continue
		}

		// Try to perform a make_leave using the information supplied in the
		// request.
		respMakeLeave, err := r.federation.MakeLeave(
			ctx,
			userID.Domain(),
			serverName,
			request.FrameID,
			request.UserID,
		)
		if err != nil {
			// TDO: Check if the user was not allowed to leave the frame.
			logrus.WithError(err).Warnf("r.federation.MakeLeave failed")
			r.statistics.ForServer(serverName).Failure()
			continue
		}

		// Work out if we support the frame version that has been supplied in
		// the make_leave response.
		verImpl, err := xtools.GetFrameVersion(respMakeLeave.FrameVersion)
		if err != nil {
			return err
		}

		// Set all the fields to be what they should be, this should be a no-op
		// but it's possible that the remote server returned us something "odd"
		frameID, err := spec.NewFrameID(request.FrameID)
		if err != nil {
			return err
		}
		senderID, err := r.rsAPI.QuerySenderIDForUser(ctx, *frameID, *userID)
		if err != nil {
			return err
		} else if senderID == nil {
			return fmt.Errorf("sender ID not found for %s in %s", *userID, *frameID)
		}
		senderIDString := string(*senderID)
		respMakeLeave.LeaveEvent.Type = spec.MFrameMember
		respMakeLeave.LeaveEvent.SenderID = senderIDString
		respMakeLeave.LeaveEvent.StateKey = &senderIDString
		respMakeLeave.LeaveEvent.FrameID = request.FrameID
		respMakeLeave.LeaveEvent.Redacts = ""
		leaveEB := verImpl.NewEventBuilderFromProtoEvent(&respMakeLeave.LeaveEvent)

		if respMakeLeave.LeaveEvent.Content == nil {
			content := map[string]interface{}{
				"membership": "leave",
			}
			if err = leaveEB.SetContent(content); err != nil {
				logrus.WithError(err).Warnf("respMakeLeave.LeaveEvent.SetContent failed")
				continue
			}
		}
		if err = leaveEB.SetUnsigned(struct{}{}); err != nil {
			logrus.WithError(err).Warnf("respMakeLeave.LeaveEvent.SetUnsigned failed")
			continue
		}

		// Build the leave event.
		event, err := leaveEB.Build(
			time.Now(),
			userID.Domain(),
			r.cfg.Coddy.KeyID,
			r.cfg.Coddy.PrivateKey,
		)
		if err != nil {
			logrus.WithError(err).Warnf("respMakeLeave.LeaveEvent.Build failed")
			continue
		}

		// Try to perform a send_leave using the newly built event.
		err = r.federation.SendLeave(
			ctx,
			userID.Domain(),
			serverName,
			event,
		)
		if err != nil {
			logrus.WithError(err).Warnf("r.federation.SendLeave failed")
			r.statistics.ForServer(serverName).Failure()
			continue
		}

		r.statistics.ForServer(serverName).Success(statistics.SendDirect)
		return nil
	}

	// If we reach here then we didn't complete a leave for some reason.
	return fmt.Errorf(
		"failed to leave frame %q through %d server(s)",
		request.FrameID, len(request.ServerNames),
	)
}

// SendInvite implements api.FederationInternalAPI
func (r *FederationInternalAPI) SendInvite(
	ctx context.Context,
	event xtools.PDU,
	strippedState []xtools.InviteStrippedState,
) (xtools.PDU, error) {
	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return nil, err
	}
	inviter, err := r.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
	if err != nil {
		return nil, err
	}

	if event.StateKey() == nil {
		return nil, errors.New("invite must be a state event")
	}

	_, destination, err := xtools.SplitID('@', *event.StateKey())
	if err != nil {
		return nil, fmt.Errorf("xtools.SplitID: %w", err)
	}

	// TODO (devon): This should be allowed via a relay. Currently only transactions
	// can be sent to relays. Would need to extend relays to handle invites.
	if !r.shouldAttemptDirectFederation(destination) {
		return nil, fmt.Errorf("relay servers have no meaningful response for invite.")
	}

	logrus.WithFields(logrus.Fields{
		"event_id":     event.EventID(),
		"user_id":      *event.StateKey(),
		"frame_id":      event.FrameID(),
		"frame_version": event.Version(),
		"destination":  destination,
	}).Info("Sending invite")

	inviteReq, err := fclient.NewInviteV2Request(event, strippedState)
	if err != nil {
		return nil, fmt.Errorf("xtools.NewInviteV2Request: %w", err)
	}

	inviteRes, err := r.federation.SendInviteV2(ctx, inviter.Domain(), destination, inviteReq)
	if err != nil {
		return nil, fmt.Errorf("r.federation.SendInviteV2: failed to send invite: %w", err)
	}
	verImpl, err := xtools.GetFrameVersion(event.Version())
	if err != nil {
		return nil, err
	}

	inviteEvent, err := verImpl.NewEventFromUntrustedJSON(inviteRes.Event)
	if err != nil {
		return nil, fmt.Errorf("r.federation.SendInviteV2 failed to decode event response: %w", err)
	}
	return inviteEvent, nil
}

// SendInviteV3 implements api.FederationInternalAPI
func (r *FederationInternalAPI) SendInviteV3(
	ctx context.Context,
	event xtools.ProtoEvent,
	invitee spec.UserID,
	version xtools.FrameVersion,
	strippedState []xtools.InviteStrippedState,
) (xtools.PDU, error) {
	validFrameID, err := spec.NewFrameID(event.FrameID)
	if err != nil {
		return nil, err
	}
	verImpl, err := xtools.GetFrameVersion(version)
	if err != nil {
		return nil, err
	}

	inviter, err := r.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(event.SenderID))
	if err != nil {
		return nil, err
	}

	// TODO (devon): This should be allowed via a relay. Currently only transactions
	// can be sent to relays. Would need to extend relays to handle invites.
	if !r.shouldAttemptDirectFederation(invitee.Domain()) {
		return nil, fmt.Errorf("relay servers have no meaningful response for invite.")
	}

	logrus.WithFields(logrus.Fields{
		"user_id":      invitee.String(),
		"frame_id":      event.FrameID,
		"frame_version": version,
		"destination":  invitee.Domain(),
	}).Info("Sending invite")

	inviteReq, err := fclient.NewInviteV3Request(event, version, strippedState)
	if err != nil {
		return nil, fmt.Errorf("xtools.NewInviteV3Request: %w", err)
	}

	inviteRes, err := r.federation.SendInviteV3(ctx, inviter.Domain(), invitee.Domain(), inviteReq, invitee)
	if err != nil {
		return nil, fmt.Errorf("r.federation.SendInviteV3: failed to send invite: %w", err)
	}

	inviteEvent, err := verImpl.NewEventFromUntrustedJSON(inviteRes.Event)
	if err != nil {
		return nil, fmt.Errorf("r.federation.SendInviteV3 failed to decode event response: %w", err)
	}
	return inviteEvent, nil
}

// PerformServersAlive implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformBroadcastEDU(
	ctx context.Context,
	request *api.PerformBroadcastEDURequest,
	response *api.PerformBroadcastEDUResponse,
) (err error) {
	destinations, err := r.db.GetAllJoinedHosts(ctx)
	if err != nil {
		return fmt.Errorf("r.db.GetAllJoinedHosts: %w", err)
	}
	if len(destinations) == 0 {
		return nil
	}

	logrus.WithContext(ctx).Infof("Sending wake-up EDU to %d destination(s)", len(destinations))

	edu := &xtools.EDU{
		Type:   "org.coddy.dendrite.wakeup",
		Origin: string(r.cfg.Coddy.ServerName),
	}
	if err = r.queues.SendEDU(edu, r.cfg.Coddy.ServerName, destinations); err != nil {
		return fmt.Errorf("r.queues.SendEDU: %w", err)
	}
	r.MarkServersAlive(destinations)

	return nil
}

// PerformWakeupServers implements api.FederationInternalAPI
func (r *FederationInternalAPI) PerformWakeupServers(
	ctx context.Context,
	request *api.PerformWakeupServersRequest,
	response *api.PerformWakeupServersResponse,
) (err error) {
	r.MarkServersAlive(request.ServerNames)
	return nil
}

func (r *FederationInternalAPI) MarkServersAlive(destinations []spec.ServerName) {
	for _, srv := range destinations {
		wasBlacklisted := r.statistics.ForServer(srv).MarkServerAlive()
		r.queues.RetryServer(srv, wasBlacklisted)
	}
}

func checkEventsContainCreateEvent(events []xtools.PDU) error {
	// sanity check we have a create event and it has a known frame version
	for _, ev := range events {
		if ev.Type() == spec.MFrameCreate && ev.StateKeyEquals("") {
			// make sure the frame version is known
			content := ev.Content()
			verBody := struct {
				Version string `json:"frame_version"`
			}{}
			err := json.Unmarshal(content, &verBody)
			if err != nil {
				return err
			}
			if verBody.Version == "" {
				// The version of the frame. Defaults to "1" if the key does not exist.
				verBody.Version = "1"
			}
			knownVersions := xtools.FrameVersions()
			if _, ok := knownVersions[xtools.FrameVersion(verBody.Version)]; !ok {
				return fmt.Errorf("m.frame.create event has an unknown frame version: %s", verBody.Version)
			}
			return nil
		}
	}
	return fmt.Errorf("response is missing m.frame.create event")
}

// federatedEventProvider is an event provider which fetches events from the server provided
func federatedEventProvider(
	ctx context.Context, federation fclient.FederationClient,
	keyRing xtools.JSONVerifier, origin, server spec.ServerName,
	userIDForSender spec.UserIDForSender,
) xtools.EventProvider {
	// A list of events that we have retried, if they were not included in
	// the auth events supplied in the send_join.
	retries := map[string][]xtools.PDU{}

	// Define a function which we can pass to Check to retrieve missing
	// auth events inline. This greatly increases our chances of not having
	// to repeat the entire set of checks just for a missing event or two.
	return func(frameVersion xtools.FrameVersion, eventIDs []string) ([]xtools.PDU, error) {
		returning := []xtools.PDU{}
		verImpl, err := xtools.GetFrameVersion(frameVersion)
		if err != nil {
			return nil, err
		}

		// See if we have retry entries for each of the supplied event IDs.
		for _, eventID := range eventIDs {
			// If we've already satisfied a request for this event ID before then
			// just append the results. We won't retry the request.
			if retry, ok := retries[eventID]; ok {
				if retry == nil {
					return nil, fmt.Errorf("missingAuth: not retrying failed event ID %q", eventID)
				}
				returning = append(returning, retry...)
				continue
			}

			// Make a note of the fact that we tried to do something with this
			// event ID, even if we don't succeed.
			retries[eventID] = nil

			// Try to retrieve the event from the server that sent us the send
			// join response.
			tx, txerr := federation.GetEvent(ctx, origin, server, eventID)
			if txerr != nil {
				return nil, fmt.Errorf("missingAuth r.federation.GetEvent: %w", txerr)
			}

			// For each event returned, add it to the set of return events. We
			// also will populate the retries, in case someone asks for this
			// event ID again.
			for _, pdu := range tx.PDUs {
				// Try to parse the event.
				ev, everr := verImpl.NewEventFromUntrustedJSON(pdu)
				if everr != nil {
					return nil, fmt.Errorf("missingAuth xtools.NewEventFromUntrustedJSON: %w", everr)
				}

				// Check the signatures of the event.
				if err := xtools.VerifyEventSignatures(ctx, ev, keyRing, userIDForSender); err != nil {
					return nil, fmt.Errorf("missingAuth VerifyEventSignatures: %w", err)
				}

				// If the event is OK then add it to the results and the retry map.
				returning = append(returning, ev)
				retries[ev.EventID()] = append(retries[ev.EventID()], ev)
			}
		}
		return returning, nil
	}
}

// P2PQueryRelayServers implements api.FederationInternalAPI
func (r *FederationInternalAPI) P2PQueryRelayServers(
	ctx context.Context,
	request *api.P2PQueryRelayServersRequest,
	response *api.P2PQueryRelayServersResponse,
) error {
	logrus.Infof("Getting relay servers for: %s", request.Server)
	relayServers, err := r.db.P2PGetRelayServersForServer(ctx, request.Server)
	if err != nil {
		return err
	}

	response.RelayServers = relayServers
	return nil
}

// P2PAddRelayServers implements api.FederationInternalAPI
func (r *FederationInternalAPI) P2PAddRelayServers(
	ctx context.Context,
	request *api.P2PAddRelayServersRequest,
	response *api.P2PAddRelayServersResponse,
) error {
	logrus.Infof("Adding relay servers for: %s", request.Server)
	err := r.db.P2PAddRelayServersForServer(ctx, request.Server, request.RelayServers)
	if err != nil {
		return err
	}

	return nil
}

// P2PRemoveRelayServers implements api.FederationInternalAPI
func (r *FederationInternalAPI) P2PRemoveRelayServers(
	ctx context.Context,
	request *api.P2PRemoveRelayServersRequest,
	response *api.P2PRemoveRelayServersResponse,
) error {
	logrus.Infof("Adding relay servers for: %s", request.Server)
	err := r.db.P2PRemoveRelayServersForServer(ctx, request.Server, request.RelayServers)
	if err != nil {
		return err
	}

	return nil
}

func (r *FederationInternalAPI) shouldAttemptDirectFederation(
	destination spec.ServerName,
) bool {
	var shouldRelay bool
	stats := r.statistics.ForServer(destination)
	if stats.AssumedOffline() && len(stats.KnownRelayServers()) > 0 {
		shouldRelay = true
	}

	return !shouldRelay
}
