package perform

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/services/dataframe/api"
	rsAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/internal/helpers"
	"github.com/withqb/coddy/services/dataframe/internal/input"
	"github.com/withqb/coddy/services/dataframe/internal/query"
	"github.com/withqb/coddy/services/dataframe/storage"
	"github.com/withqb/coddy/services/dataframe/types"
	fsAPI "github.com/withqb/coddy/services/federationapi/api"
	"github.com/withqb/coddy/setup/config"
)

type Joiner struct {
	Cfg   *config.DataFrame
	FSAPI fsAPI.DataframeFederationAPI
	RSAPI rsAPI.DataframeInternalAPI
	DB    storage.Database

	Inputer *input.Inputer
	Queryer *query.Queryer
}

// PerformJoin handles joining coddy frames, including over federation by talking to the federationapi.
func (r *Joiner) PerformJoin(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (frameID string, joinedVia spec.ServerName, err error) {
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"frame_id": req.FrameIDOrAlias,
		"user_id": req.UserID,
		"servers": req.ServerNames,
	})
	logger.Info("User requested to frame join")
	frameID, joinedVia, err = r.performJoin(context.Background(), req)
	if err != nil {
		logger.WithError(err).Error("failed to join frame")
		sentry.CaptureException(err)
		return "", "", err
	}
	logger.Info("User joined frame successfully")

	return frameID, joinedVia, nil
}

func (r *Joiner) performJoin(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	_, domain, err := xtools.SplitID('@', req.UserID)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("supplied user ID %q in incorrect format", req.UserID)}
	}
	if !r.Cfg.Coddy.IsLocalServerName(domain) {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", req.UserID)}
	}
	if strings.HasPrefix(req.FrameIDOrAlias, "!") {
		return r.performJoinFrameByID(ctx, req)
	}
	if strings.HasPrefix(req.FrameIDOrAlias, "#") {
		return r.performJoinFrameByAlias(ctx, req)
	}
	return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("frame ID or alias %q is invalid", req.FrameIDOrAlias)}
}

func (r *Joiner) performJoinFrameByAlias(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	// Get the domain part of the frame alias.
	_, domain, err := xtools.SplitID('#', req.FrameIDOrAlias)
	if err != nil {
		return "", "", fmt.Errorf("alias %q is not in the correct format", req.FrameIDOrAlias)
	}
	req.ServerNames = append(req.ServerNames, domain)

	// Check if this alias matches our own server configuration. If it
	// doesn't then we'll need to try a federated join.
	var frameID string
	if !r.Cfg.Coddy.IsLocalServerName(domain) {
		// The alias isn't owned by us, so we will need to try joining using
		// a remote server.
		dirReq := fsAPI.PerformDirectoryLookupRequest{
			FrameAlias:  req.FrameIDOrAlias, // the frame alias to lookup
			ServerName: domain,            // the server to ask
		}
		dirRes := fsAPI.PerformDirectoryLookupResponse{}
		err = r.FSAPI.PerformDirectoryLookup(ctx, &dirReq, &dirRes)
		if err != nil {
			logrus.WithError(err).Errorf("error looking up alias %q", req.FrameIDOrAlias)
			return "", "", fmt.Errorf("looking up alias %q over federation failed: %w", req.FrameIDOrAlias, err)
		}
		frameID = dirRes.FrameID
		req.ServerNames = append(req.ServerNames, dirRes.ServerNames...)
	} else {
		var getFrameReq = rsAPI.GetFrameIDForAliasRequest{
			Alias:              req.FrameIDOrAlias,
			IncludeAppservices: true,
		}
		var getFrameRes = rsAPI.GetFrameIDForAliasResponse{}
		// Otherwise, look up if we know this frame alias locally.
		err = r.RSAPI.GetFrameIDForAlias(ctx, &getFrameReq, &getFrameRes)
		if err != nil {
			return "", "", fmt.Errorf("lookup frame alias %q failed: %w", req.FrameIDOrAlias, err)
		}
		frameID = getFrameRes.FrameID
	}

	// If the frame ID is empty then we failed to look up the alias.
	if frameID == "" {
		return "", "", fmt.Errorf("alias %q not found", req.FrameIDOrAlias)
	}

	// If we do, then pluck out the frame ID and continue the join.
	req.FrameIDOrAlias = frameID
	return r.performJoinFrameByID(ctx, req)
}

// TDO: Break this function up a bit & move to GMSL
// nolint:gocyclo
func (r *Joiner) performJoinFrameByID(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	// The original client request ?server_name=... may include this HS so filter that out so we
	// don't attempt to make_join with ourselves
	for i := 0; i < len(req.ServerNames); i++ {
		if r.Cfg.Coddy.IsLocalServerName(req.ServerNames[i]) {
			// delete this entry
			req.ServerNames = append(req.ServerNames[:i], req.ServerNames[i+1:]...)
			i--
		}
	}

	// Get the domain part of the frame ID.
	frameID, err := spec.NewFrameID(req.FrameIDOrAlias)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("frame ID %q is invalid: %w", req.FrameIDOrAlias, err)}
	}

	// If the server name in the frame ID isn't ours then it's a
	// possible candidate for finding the frame via federation. Add
	// it to the list of servers to try.
	if !r.Cfg.Coddy.IsLocalServerName(frameID.Domain()) {
		req.ServerNames = append(req.ServerNames, frameID.Domain())
	}

	// Force a federated join if we aren't in the frame and we've been
	// given some server names to try joining by.
	inFrameReq := &rsAPI.QueryServerJoinedToFrameRequest{
		FrameID: req.FrameIDOrAlias,
	}
	inFrameRes := &rsAPI.QueryServerJoinedToFrameResponse{}
	if err = r.Queryer.QueryServerJoinedToFrame(ctx, inFrameReq, inFrameRes); err != nil {
		return "", "", fmt.Errorf("r.Queryer.QueryServerJoinedToFrame: %w", err)
	}
	serverInFrame := inFrameRes.IsInFrame
	forceFederatedJoin := len(req.ServerNames) > 0 && !serverInFrame

	userID, err := spec.NewUserID(req.UserID, true)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("user ID %q is invalid: %w", req.UserID, err)}
	}

	// Look up the frame NID for the supplied frame ID.
	var senderID spec.SenderID
	checkInvitePending := false
	info, err := r.DB.FrameInfo(ctx, req.FrameIDOrAlias)
	if err == nil && info != nil {
		switch info.FrameVersion {
		case xtools.FrameVersionPseudoIDs:
			senderIDPtr, queryErr := r.Queryer.QuerySenderIDForUser(ctx, *frameID, *userID)
			if queryErr == nil {
				checkInvitePending = true
			}
			if senderIDPtr == nil {
				// create user frame key if needed
				key, keyErr := r.RSAPI.GetOrCreateUserFramePrivateKey(ctx, *userID, *frameID)
				if keyErr != nil {
					xutil.GetLogger(ctx).WithError(keyErr).Error("GetOrCreateUserFramePrivateKey failed")
					return "", "", fmt.Errorf("GetOrCreateUserFramePrivateKey failed: %w", keyErr)
				}
				senderID = spec.SenderIDFromPseudoIDKey(key)
			} else {
				senderID = *senderIDPtr
			}
		default:
			checkInvitePending = true
			senderID = spec.SenderID(userID.String())
		}
	}

	userDomain := userID.Domain()

	// Force a federated join if we're dealing with a pending invite
	// and we aren't in the frame.
	if checkInvitePending {
		isInvitePending, inviteSender, _, inviteEvent, inviteErr := helpers.IsInvitePending(ctx, r.DB, req.FrameIDOrAlias, senderID)
		if inviteErr == nil && !serverInFrame && isInvitePending {
			inviter, queryErr := r.RSAPI.QueryUserIDForSender(ctx, *frameID, inviteSender)
			if queryErr != nil {
				return "", "", fmt.Errorf("r.RSAPI.QueryUserIDForSender: %w", queryErr)
			}

			// If we were invited by someone from another server then we can
			// assume they are in the frame so we can join via them.
			if inviter != nil && !r.Cfg.Coddy.IsLocalServerName(inviter.Domain()) {
				req.ServerNames = append(req.ServerNames, inviter.Domain())
				forceFederatedJoin = true
				memberEvent := gjson.Parse(string(inviteEvent.JSON()))
				// only set unsigned if we've got a content.membership, which we _should_
				if memberEvent.Get("content.membership").Exists() {
					req.Unsigned = map[string]interface{}{
						"prev_sender": memberEvent.Get("sender").Str,
						"prev_content": map[string]interface{}{
							"is_direct":  memberEvent.Get("content.is_direct").Bool(),
							"membership": memberEvent.Get("content.membership").Str,
						},
					}
				}
			}
		}
	}

	// If a guest is trying to join a frame, check that the frame has a m.frame.guest_access event
	if req.IsGuest {
		var guestAccessEvent *types.HeaderedEvent
		guestAccess := "forbidden"
		guestAccessEvent, err = r.DB.GetStateEvent(ctx, req.FrameIDOrAlias, spec.MFrameGuestAccess, "")
		if (err != nil && !errors.Is(err, sql.ErrNoRows)) || guestAccessEvent == nil {
			logrus.WithError(err).Warn("unable to get m.frame.guest_access event, defaulting to 'forbidden'")
		}
		if guestAccessEvent != nil {
			guestAccess = gjson.GetBytes(guestAccessEvent.Content(), "guest_access").String()
		}

		// Servers MUST only allow guest users to join frames if the m.frame.guest_access state event
		// is present on the frame and has the guest_access value can_join.
		if guestAccess != "can_join" {
			return "", "", rsAPI.ErrNotAllowed{Err: fmt.Errorf("guest access is forbidden")}
		}
	}

	// If we should do a forced federated join then do that.
	var joinedVia spec.ServerName
	if forceFederatedJoin {
		joinedVia, err = r.performFederatedJoinFrameByID(ctx, req)
		return req.FrameIDOrAlias, joinedVia, err
	}

	// Try to construct an actual join event from the template.
	// If this succeeds then it is a sign that the frame already exists
	// locally on the homeserver.
	// TDO: Check what happens if the frame exists on the server
	// but everyone has since left. I suspect it does the wrong thing.

	var buildRes rsAPI.QueryLatestEventsAndStateResponse
	identity := r.Cfg.Coddy.SigningIdentity

	// at this point we know we have an existing frame
	if inFrameRes.FrameVersion == xtools.FrameVersionPseudoIDs {
		var pseudoIDKey ed25519.PrivateKey
		pseudoIDKey, err = r.RSAPI.GetOrCreateUserFramePrivateKey(ctx, *userID, *frameID)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("GetOrCreateUserFramePrivateKey failed")
			return "", "", err
		}

		mapping := &xtools.MXIDMapping{
			UserFrameKey: spec.SenderIDFromPseudoIDKey(pseudoIDKey),
			UserID:      userID.String(),
		}

		// Sign the mapping with the server identity
		if err = mapping.Sign(identity.ServerName, identity.KeyID, identity.PrivateKey); err != nil {
			return "", "", err
		}
		req.Content["mxid_mapping"] = mapping

		// sign the event with the pseudo ID key
		identity = fclient.SigningIdentity{
			ServerName: spec.ServerName(spec.SenderIDFromPseudoIDKey(pseudoIDKey)),
			KeyID:      "ed25519:1",
			PrivateKey: pseudoIDKey,
		}
	}

	senderIDString := string(senderID)

	// Prepare the template for the join event.
	proto := xtools.ProtoEvent{
		Type:     spec.MFrameMember,
		SenderID: senderIDString,
		StateKey: &senderIDString,
		FrameID:   req.FrameIDOrAlias,
		Redacts:  "",
	}
	if err = proto.SetUnsigned(struct{}{}); err != nil {
		return "", "", fmt.Errorf("eb.SetUnsigned: %w", err)
	}

	// It is possible for the request to include some "content" for the
	// event. We'll always overwrite the "membership" key, but the rest,
	// like "display_name" or "avatar_url", will be kept if supplied.
	if req.Content == nil {
		req.Content = map[string]interface{}{}
	}
	req.Content["membership"] = spec.Join
	if authorisedVia, aerr := r.populateAuthorisedViaUserForRestrictedJoin(ctx, req, senderID); aerr != nil {
		return "", "", aerr
	} else if authorisedVia != "" {
		req.Content["join_authorised_via_users_server"] = authorisedVia
	}
	if err = proto.SetContent(req.Content); err != nil {
		return "", "", fmt.Errorf("eb.SetContent: %w", err)
	}
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, time.Now(), r.RSAPI, &buildRes)

	switch err.(type) {
	case nil:
		// The frame join is local. Send the new join event into the
		// dataframe. First of all check that the user isn't already
		// a member of the frame. This is best-effort (as in we won't
		// fail if we can't find the existing membership) because there
		// is really no harm in just sending another membership event.
		membershipRes := &api.QueryMembershipForUserResponse{}
		_ = r.Queryer.QueryMembershipForSenderID(ctx, *frameID, senderID, membershipRes)

		// If we haven't already joined the frame then send an event
		// into the frame changing our membership status.
		if !membershipRes.FrameExists || !membershipRes.IsInFrame {
			inputReq := rsAPI.InputFrameEventsRequest{
				InputFrameEvents: []rsAPI.InputFrameEvent{
					{
						Kind:         rsAPI.KindNew,
						Event:        event,
						SendAsServer: string(userDomain),
					},
				},
			}
			inputRes := rsAPI.InputFrameEventsResponse{}
			r.Inputer.InputFrameEvents(ctx, &inputReq, &inputRes)
			if err = inputRes.Err(); err != nil {
				return "", "", rsAPI.ErrNotAllowed{Err: err}
			}
		}

	case eventutil.ErrFrameNoExists:
		// The frame doesn't exist locally. If the frame ID looks like it should
		// be ours then this probably means that we've nuked our database at
		// some point.
		if r.Cfg.Coddy.IsLocalServerName(frameID.Domain()) {
			// If there are no more server names to try then give up here.
			// Otherwise we'll try a federated join as normal, since it's quite
			// possible that the frame still exists on other servers.
			if len(req.ServerNames) == 0 {
				return "", "", eventutil.ErrFrameNoExists{}
			}
		}

		// Perform a federated frame join.
		joinedVia, err = r.performFederatedJoinFrameByID(ctx, req)
		return req.FrameIDOrAlias, joinedVia, err

	default:
		// Something else went wrong.
		return "", "", fmt.Errorf("error joining local frame: %q", err)
	}

	// By this point, if req.FrameIDOrAlias contained an alias, then
	// it will have been overwritten with a frame ID by performJoinFrameByAlias.
	// We should now include this in the response so that the CS API can
	// return the right frame ID.
	return req.FrameIDOrAlias, userDomain, nil
}

func (r *Joiner) performFederatedJoinFrameByID(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (spec.ServerName, error) {
	// Try joining by all of the supplied server names.
	fedReq := fsAPI.PerformJoinRequest{
		FrameID:      req.FrameIDOrAlias, // the frame ID to try and join
		UserID:      req.UserID,        // the user ID joining the frame
		ServerNames: req.ServerNames,   // the server to try joining with
		Content:     req.Content,       // the membership event content
		Unsigned:    req.Unsigned,      // the unsigned event content, if any
	}
	fedRes := fsAPI.PerformJoinResponse{}
	r.FSAPI.PerformJoin(ctx, &fedReq, &fedRes)
	if fedRes.LastError != nil {
		return "", fedRes.LastError
	}
	return fedRes.JoinedVia, nil
}

func (r *Joiner) populateAuthorisedViaUserForRestrictedJoin(
	ctx context.Context,
	joinReq *rsAPI.PerformJoinRequest,
	senderID spec.SenderID,
) (string, error) {
	frameID, err := spec.NewFrameID(joinReq.FrameIDOrAlias)
	if err != nil {
		return "", err
	}

	return r.Queryer.QueryRestrictedJoinAllowed(ctx, *frameID, senderID)
}
