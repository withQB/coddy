package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/clientapi/httputil"
	"github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/types"
	"github.com/withqb/coddy/setup/config"
	userapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type invite struct {
	MXID   string                              `json:"mxid"`
	RoomID string                              `json:"room_id"`
	Sender string                              `json:"sender"`
	Token  string                              `json:"token"`
	Signed xtools.MemberThirdPartyInviteSigned `json:"signed"`
}

type invites struct {
	Medium  string   `json:"medium"`
	Address string   `json:"address"`
	MXID    string   `json:"mxid"`
	Invites []invite `json:"invites"`
}

var (
	errNotLocalUser = errors.New("the user is not from this server")
	errNotInRoom    = errors.New("the server isn't currently in the room")
)

// CreateInvitesFrom3PIDInvites implements POST /_matrix/federation/v1/3pid/onbind
func CreateInvitesFrom3PIDInvites(
	req *http.Request, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	federation fclient.FederationClient,
	userAPI userapi.FederationUserAPI,
) xutil.JSONResponse {
	var body invites
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	evs := []*types.HeaderedEvent{}
	for _, inv := range body.Invites {
		_, err := rsAPI.QueryRoomVersionForRoom(req.Context(), inv.RoomID)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.UnsupportedRoomVersion(err.Error()),
			}
		}

		event, err := createInviteFrom3PIDInvite(
			req.Context(), rsAPI, cfg, inv, federation, userAPI,
		)
		if err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("createInviteFrom3PIDInvite failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if event != nil {
			evs = append(evs, &types.HeaderedEvent{PDU: event})
		}
	}

	// Send all the events
	if err := api.SendEvents(
		req.Context(),
		rsAPI,
		api.KindNew,
		evs,
		cfg.Matrix.ServerName, // TODO: which virtual host?
		"TODO",
		cfg.Matrix.ServerName,
		nil,
		false,
	); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("SendEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// ExchangeThirdPartyInvite implements PUT /_matrix/federation/v1/exchange_third_party_invite/{roomID}
func ExchangeThirdPartyInvite(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID string,
	rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	federation fclient.FederationClient,
) xutil.JSONResponse {
	var proto xtools.ProtoEvent
	if err := json.Unmarshal(request.Content(), &proto); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}

	// Check that the room ID is correct.
	if proto.RoomID != roomID {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The room ID in the request path must match the room ID in the invite event JSON"),
		}
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid room ID"),
		}
	}
	userID, err := rsAPI.QueryUserIDForSender(httpReq.Context(), *validRoomID, spec.SenderID(proto.SenderID))
	if err != nil || userID == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid sender ID"),
		}
	}
	senderDomain := userID.Domain()

	// Check that the state key is correct.
	targetUserID, err := rsAPI.QueryUserIDForSender(httpReq.Context(), *validRoomID, spec.SenderID(*proto.StateKey))
	if err != nil || targetUserID == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event's state key isn't a Matrix user ID"),
		}
	}
	targetDomain := targetUserID.Domain()

	// Check that the target user is from the requesting homeserver.
	if targetDomain != request.Origin() {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event's state key doesn't have the same domain as the request's origin"),
		}
	}

	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}

	// Auth and build the event from what the remote server sent us
	event, err := buildMembershipEvent(httpReq.Context(), &proto, rsAPI, cfg)
	if err == errNotInRoom {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Unknown room " + roomID),
		}
	} else if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("buildMembershipEvent failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Ask the requesting server to sign the newly created event so we know it
	// acknowledged it
	inviteReq, err := fclient.NewInviteV2Request(event, nil)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("failed to make invite v2 request")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	signedEvent, err := federation.SendInviteV2(httpReq.Context(), senderDomain, request.Origin(), inviteReq)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("federation.SendInvite failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	verImpl, err := xtools.GetRoomVersion(roomVersion)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Errorf("unknown room version: %s", roomVersion)
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	inviteEvent, err := verImpl.NewEventFromUntrustedJSON(signedEvent.Event)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("federation.SendInvite failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Send the event to the Roomserver
	if err = api.SendEvents(
		httpReq.Context(), rsAPI,
		api.KindNew,
		[]*types.HeaderedEvent{
			{PDU: inviteEvent},
		},
		request.Destination(),
		request.Origin(),
		cfg.Matrix.ServerName,
		nil,
		false,
	); err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("SendEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// createInviteFrom3PIDInvite processes an invite provided by the identity server
// and creates a m.room.member event (with "invite" membership) from it.
// Returns an error if there was a problem building the event or fetching the
// necessary data to do so.
func createInviteFrom3PIDInvite(
	ctx context.Context, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
	inv invite, federation fclient.FederationClient,
	userAPI userapi.FederationUserAPI,
) (xtools.PDU, error) {
	_, server, err := xtools.SplitID('@', inv.MXID)
	if err != nil {
		return nil, err
	}

	if server != cfg.Matrix.ServerName {
		return nil, errNotLocalUser
	}

	// Build the event
	proto := &xtools.ProtoEvent{
		Type:     "m.room.member",
		SenderID: inv.Sender,
		RoomID:   inv.RoomID,
		StateKey: &inv.MXID,
	}

	profile, err := userAPI.QueryProfile(ctx, inv.MXID)
	if err != nil {
		return nil, err
	}

	content := xtools.MemberContent{
		AvatarURL:   profile.AvatarURL,
		DisplayName: profile.DisplayName,
		Membership:  spec.Invite,
		ThirdPartyInvite: &xtools.MemberThirdPartyInvite{
			Signed: inv.Signed,
		},
	}

	if err = proto.SetContent(content); err != nil {
		return nil, err
	}

	event, err := buildMembershipEvent(ctx, proto, rsAPI, cfg)
	if err == errNotInRoom {
		return nil, sendToRemoteServer(ctx, inv, federation, cfg, *proto)
	}
	if err != nil {
		return nil, err
	}

	return event, nil
}

// buildMembershipEvent uses a builder for a m.room.member invite event derived
// from a third-party invite to auth and build the said event. Returns the said
// event.
// Returns errNotInRoom if the server is not in the room the invite is for.
// Returns an error if something failed during the process.
func buildMembershipEvent(
	ctx context.Context,
	protoEvent *xtools.ProtoEvent, rsAPI api.FederationRoomserverAPI,
	cfg *config.FederationAPI,
) (xtools.PDU, error) {
	eventsNeeded, err := xtools.StateNeededForProtoEvent(protoEvent)
	if err != nil {
		return nil, err
	}

	if len(eventsNeeded.Tuples()) == 0 {
		return nil, errors.New("expecting state tuples for event builder, got none")
	}

	// Ask the Roomserver for information about this room
	queryReq := api.QueryLatestEventsAndStateRequest{
		RoomID:       protoEvent.RoomID,
		StateToFetch: eventsNeeded.Tuples(),
	}
	var queryRes api.QueryLatestEventsAndStateResponse
	if err = rsAPI.QueryLatestEventsAndState(ctx, &queryReq, &queryRes); err != nil {
		return nil, err
	}

	if !queryRes.RoomExists {
		// Use federation to auth the event
		return nil, errNotInRoom
	}

	// Auth the event locally
	protoEvent.Depth = queryRes.Depth
	protoEvent.PrevEvents = queryRes.LatestEvents

	authEvents := xtools.NewAuthEvents(nil)

	for i := range queryRes.StateEvents {
		err = authEvents.AddEvent(queryRes.StateEvents[i].PDU)
		if err != nil {
			return nil, err
		}
	}

	if err = fillDisplayName(protoEvent, authEvents); err != nil {
		return nil, err
	}

	refs, err := eventsNeeded.AuthEventReferences(&authEvents)
	if err != nil {
		return nil, err
	}
	protoEvent.AuthEvents = refs

	verImpl, err := xtools.GetRoomVersion(queryRes.RoomVersion)
	if err != nil {
		return nil, err
	}
	builder := verImpl.NewEventBuilderFromProtoEvent(protoEvent)

	event, err := builder.Build(
		time.Now(), cfg.Matrix.ServerName, cfg.Matrix.KeyID,
		cfg.Matrix.PrivateKey,
	)

	return event, err
}

// sendToRemoteServer uses federation to send an invite provided by an identity
// server to a remote server in case the current server isn't in the room the
// invite is for.
// Returns an error if it couldn't get the server names to reach or if all of
// them responded with an error.
func sendToRemoteServer(
	ctx context.Context, inv invite,
	federation fclient.FederationClient, cfg *config.FederationAPI,
	proto xtools.ProtoEvent,
) (err error) {
	remoteServers := make([]spec.ServerName, 2)
	_, remoteServers[0], err = xtools.SplitID('@', inv.Sender)
	if err != nil {
		return
	}
	// Fallback to the room's server if the sender's domain is the same as
	// the current server's
	_, remoteServers[1], err = xtools.SplitID('!', inv.RoomID)
	if err != nil {
		return
	}

	for _, server := range remoteServers {
		err = federation.ExchangeThirdPartyInvite(ctx, cfg.Matrix.ServerName, server, proto)
		if err == nil {
			return
		}
		logrus.WithError(err).Warnf("failed to send 3PID invite via %s", server)
	}

	return errors.New("failed to send 3PID invite via any server")
}

// fillDisplayName looks in a list of auth events for a m.room.third_party_invite
// event with the state key matching a given m.room.member event's content's token.
// If such an event is found, fills the "display_name" attribute of the
// "third_party_invite" structure in the m.room.member event with the display_name
// from the m.room.third_party_invite event.
// Returns an error if there was a problem parsing the m.room.third_party_invite
// event's content or updating the m.room.member event's content.
// Returns nil if no m.room.third_party_invite with a matching token could be
// found. Returning an error isn't necessary in this case as the event will be
// rejected by xtools.
func fillDisplayName(
	builder *xtools.ProtoEvent, authEvents xtools.AuthEvents,
) error {
	var content xtools.MemberContent
	if err := json.Unmarshal(builder.Content, &content); err != nil {
		return err
	}

	// Look for the m.room.third_party_invite event
	thirdPartyInviteEvent, _ := authEvents.ThirdPartyInvite(content.ThirdPartyInvite.Signed.Token)

	if thirdPartyInviteEvent == nil {
		// If the third party invite event doesn't exist then we can't use it to set the display name.
		return nil
	}

	var thirdPartyInviteContent xtools.ThirdPartyInviteContent
	if err := json.Unmarshal(thirdPartyInviteEvent.Content(), &thirdPartyInviteContent); err != nil {
		return err
	}

	// Use the m.room.third_party_invite event to fill the "displayname" and
	// update the m.room.member event's content with it
	content.ThirdPartyInvite.DisplayName = thirdPartyInviteContent.DisplayName
	return builder.SetContent(content)
}
