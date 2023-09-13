package routing

import (
	"fmt"
	"net/http"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/clientapi/httputil"
	federationAPI "github.com/withqb/coddy/federationapi/api"
	roomserverAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/setup/config"
	userapi "github.com/withqb/coddy/userapi/api"
)

type roomDirectoryResponse struct {
	RoomID  string   `json:"room_id"`
	Servers []string `json:"servers"`
}

func (r *roomDirectoryResponse) fillServers(servers []spec.ServerName) {
	r.Servers = make([]string, len(servers))
	for i, s := range servers {
		r.Servers[i] = string(s)
	}
}

// DirectoryRoom looks up a room alias
func DirectoryRoom(
	req *http.Request,
	roomAlias string,
	federation fclient.FederationClient,
	cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	fedSenderAPI federationAPI.ClientFederationAPI,
) xutil.JSONResponse {
	_, domain, err := xtools.SplitID('#', roomAlias)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Room alias must be in the form '#localpart:domain'"),
		}
	}

	var res roomDirectoryResponse

	// Query the roomserver API to check if the alias exists locally.
	queryReq := &roomserverAPI.GetRoomIDForAliasRequest{
		Alias:              roomAlias,
		IncludeAppservices: true,
	}
	queryRes := &roomserverAPI.GetRoomIDForAliasResponse{}
	if err = rsAPI.GetRoomIDForAlias(req.Context(), queryReq, queryRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.GetRoomIDForAlias failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	res.RoomID = queryRes.RoomID

	if res.RoomID == "" {
		// If we don't know it locally, do a federation query.
		// But don't send the query to ourselves.
		if !cfg.Matrix.IsLocalServerName(domain) {
			fedRes, fedErr := federation.LookupRoomAlias(req.Context(), cfg.Matrix.ServerName, domain, roomAlias)
			if fedErr != nil {
				// TODO: Return 502 if the remote server errored.
				// TODO: Return 504 if the remote server timed out.
				xutil.GetLogger(req.Context()).WithError(fedErr).Error("federation.LookupRoomAlias failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			res.RoomID = fedRes.RoomID
			res.fillServers(fedRes.Servers)
		}

		if res.RoomID == "" {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(
					fmt.Sprintf("Room alias %s not found", roomAlias),
				),
			}
		}
	} else {
		joinedHostsReq := federationAPI.QueryJoinedHostServerNamesInRoomRequest{RoomID: res.RoomID}
		var joinedHostsRes federationAPI.QueryJoinedHostServerNamesInRoomResponse
		if err = fedSenderAPI.QueryJoinedHostServerNamesInRoom(req.Context(), &joinedHostsReq, &joinedHostsRes); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("fedSenderAPI.QueryJoinedHostServerNamesInRoom failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		res.fillServers(joinedHostsRes.ServerNames)
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}

// SetLocalAlias implements PUT /directory/room/{roomAlias}
func SetLocalAlias(
	req *http.Request,
	device *userapi.Device,
	alias string,
	cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
) xutil.JSONResponse {
	_, domain, err := xtools.SplitID('#', alias)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Room alias must be in the form '#localpart:domain'"),
		}
	}

	if !cfg.Matrix.IsLocalServerName(domain) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Alias must be on local homeserver"),
		}
	}

	// Check that the alias does not fall within an exclusive namespace of an
	// application service
	// TODO: This code should eventually be refactored with:
	// 1. The new method for checking for things matching an AS's namespace
	// 2. Using an overall Regex object for all AS's just like we did for usernames
	reqUserID, _, err := xtools.SplitID('@', device.UserID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("User ID must be in the form '@localpart:domain'"),
		}
	}
	for _, appservice := range cfg.Derived.ApplicationServices {
		// Don't prevent AS from creating aliases in its own namespace
		// Note that Dendrite uses SenderLocalpart as UserID for AS users
		if reqUserID != appservice.SenderLocalpart {
			if aliasNamespaces, ok := appservice.NamespaceMap["aliases"]; ok {
				for _, namespace := range aliasNamespaces {
					if namespace.Exclusive && namespace.RegexpObject.MatchString(alias) {
						return xutil.JSONResponse{
							Code: http.StatusBadRequest,
							JSON: spec.ASExclusive("Alias is reserved by an application service"),
						}
					}
				}
			}
		}
	}

	var r struct {
		RoomID string `json:"room_id"`
	}
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}

	roomID, err := spec.NewRoomID(r.RoomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *roomID, *userID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QuerySenderIDForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	} else if senderID == nil {
		xutil.GetLogger(req.Context()).WithField("roomID", *roomID).WithField("userID", *userID).Error("Sender ID not found")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	aliasAlreadyExists, err := rsAPI.SetRoomAlias(req.Context(), *senderID, *roomID, alias)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("aliasAPI.SetRoomAlias failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if aliasAlreadyExists {
		return xutil.JSONResponse{
			Code: http.StatusConflict,
			JSON: spec.Unknown("The alias " + alias + " already exists."),
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// RemoveLocalAlias implements DELETE /directory/room/{roomAlias}
func RemoveLocalAlias(
	req *http.Request,
	device *userapi.Device,
	alias string,
	rsAPI roomserverAPI.ClientRoomserverAPI,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "UserID for device is invalid"},
		}
	}

	roomIDReq := roomserverAPI.GetRoomIDForAliasRequest{Alias: alias}
	roomIDRes := roomserverAPI.GetRoomIDForAliasResponse{}
	err = rsAPI.GetRoomIDForAlias(req.Context(), &roomIDReq, &roomIDRes)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}

	validRoomID, err := spec.NewRoomID(roomIDRes.RoomID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}

	// This seems like the kind of auth check that should be done in the roomserver, but
	// if this check fails (user is not in the room), then there will be no SenderID for the user
	// for pseudo-ID rooms - it will just return "". However, we can't use lack of a sender ID
	// as meaning they are not in the room, since lacking a sender ID could be caused by other bugs.
	// TODO: maybe have QuerySenderIDForUser return richer errors?
	var queryResp roomserverAPI.QueryMembershipForUserResponse
	err = rsAPI.QueryMembershipForUser(req.Context(), &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: validRoomID.String(),
		UserID: *userID,
	}, &queryResp)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("roomserverAPI.QueryMembershipForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}
	if !queryResp.IsInRoom {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You do not have permission to remove this alias."),
		}
	}

	deviceSenderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *userID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}
	// TODO: how to handle this case? missing user/room keys seem to be a whole new class of errors
	if deviceSenderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	aliasFound, aliasRemoved, err := rsAPI.RemoveRoomAlias(req.Context(), *deviceSenderID, alias)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("aliasAPI.RemoveRoomAlias failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	if !aliasFound {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}

	if !aliasRemoved {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You do not have permission to remove this alias."),
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

type roomVisibility struct {
	Visibility string `json:"visibility"`
}

// GetVisibility implements GET /directory/list/room/{roomID}
func GetVisibility(
	req *http.Request, rsAPI roomserverAPI.ClientRoomserverAPI,
	roomID string,
) xutil.JSONResponse {
	var res roomserverAPI.QueryPublishedRoomsResponse
	err := rsAPI.QueryPublishedRooms(req.Context(), &roomserverAPI.QueryPublishedRoomsRequest{
		RoomID: roomID,
	}, &res)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QueryPublishedRooms failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var v roomVisibility
	if len(res.RoomIDs) == 1 {
		v.Visibility = spec.Public
	} else {
		v.Visibility = "private"
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: v,
	}
}

// SetVisibility implements PUT /directory/list/room/{roomID}
// TODO: Allow admin users to edit the room visibility
func SetVisibility(
	req *http.Request, rsAPI roomserverAPI.ClientRoomserverAPI, dev *userapi.Device,
	roomID string,
) xutil.JSONResponse {
	deviceUserID, err := spec.NewUserID(dev.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("userID for this device is invalid"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("roomID is invalid")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *deviceUserID)
	if err != nil || senderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("failed to find senderID for this user"),
		}
	}

	resErr := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if resErr != nil {
		return *resErr
	}

	queryEventsReq := roomserverAPI.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
		StateToFetch: []xtools.StateKeyTuple{{
			EventType: spec.MRoomPowerLevels,
			StateKey:  "",
		}},
	}
	var queryEventsRes roomserverAPI.QueryLatestEventsAndStateResponse
	err = rsAPI.QueryLatestEventsAndState(req.Context(), &queryEventsReq, &queryEventsRes)
	if err != nil || len(queryEventsRes.StateEvents) == 0 {
		xutil.GetLogger(req.Context()).WithError(err).Error("could not query events from room")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// NOTSPEC: Check if the user's power is greater than power required to change m.room.canonical_alias event
	power, _ := xtools.NewPowerLevelContentFromEvent(queryEventsRes.StateEvents[0].PDU)
	if power.UserLevel(*senderID) < power.EventLevel(spec.MRoomCanonicalAlias, true) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to change visibility"),
		}
	}

	var v roomVisibility
	if reqErr := httputil.UnmarshalJSONRequest(req, &v); reqErr != nil {
		return *reqErr
	}

	if err = rsAPI.PerformPublish(req.Context(), &roomserverAPI.PerformPublishRequest{
		RoomID:     roomID,
		Visibility: v.Visibility,
	}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("failed to publish room")
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

func SetVisibilityAS(
	req *http.Request, rsAPI roomserverAPI.ClientRoomserverAPI, dev *userapi.Device,
	networkID, roomID string,
) xutil.JSONResponse {
	if dev.AccountType != userapi.AccountTypeAppService {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Only appservice may use this endpoint"),
		}
	}
	var v roomVisibility

	// If the method is delete, we simply mark the visibility as private
	if req.Method == http.MethodDelete {
		v.Visibility = "private"
	} else {
		if reqErr := httputil.UnmarshalJSONRequest(req, &v); reqErr != nil {
			return *reqErr
		}
	}
	if err := rsAPI.PerformPublish(req.Context(), &roomserverAPI.PerformPublishRequest{
		RoomID:       roomID,
		Visibility:   v.Visibility,
		NetworkID:    networkID,
		AppserviceID: dev.AppserviceID,
	}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("failed to publish room")
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
