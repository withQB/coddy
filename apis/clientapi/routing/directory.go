package routing

import (
	"fmt"
	"net/http"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	federationAPI "github.com/withqb/coddy/apis/federationapi/api"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/setup/config"
)

type frameDirectoryResponse struct {
	FrameID  string   `json:"frame_id"`
	Servers []string `json:"servers"`
}

func (r *frameDirectoryResponse) fillServers(servers []spec.ServerName) {
	r.Servers = make([]string, len(servers))
	for i, s := range servers {
		r.Servers[i] = string(s)
	}
}

// DirectoryFrame looks up a frame alias
func DirectoryFrame(
	req *http.Request,
	frameAlias string,
	federation fclient.FederationClient,
	cfg *config.ClientAPI,
	rsAPI dataframeAPI.ClientDataframeAPI,
	fedSenderAPI federationAPI.ClientFederationAPI,
) xutil.JSONResponse {
	_, domain, err := xtools.SplitID('#', frameAlias)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Frame alias must be in the form '#localpart:domain'"),
		}
	}

	var res frameDirectoryResponse

	// Query the dataframe API to check if the alias exists locally.
	queryReq := &dataframeAPI.GetFrameIDForAliasRequest{
		Alias:              frameAlias,
		IncludeAppservices: true,
	}
	queryRes := &dataframeAPI.GetFrameIDForAliasResponse{}
	if err = rsAPI.GetFrameIDForAlias(req.Context(), queryReq, queryRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.GetFrameIDForAlias failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	res.FrameID = queryRes.FrameID

	if res.FrameID == "" {
		// If we don't know it locally, do a federation query.
		// But don't send the query to ourselves.
		if !cfg.Matrix.IsLocalServerName(domain) {
			fedRes, fedErr := federation.LookupFrameAlias(req.Context(), cfg.Matrix.ServerName, domain, frameAlias)
			if fedErr != nil {
				// TODO: Return 502 if the remote server errored.
				// TODO: Return 504 if the remote server timed out.
				xutil.GetLogger(req.Context()).WithError(fedErr).Error("federation.LookupFrameAlias failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			res.FrameID = fedRes.FrameID
			res.fillServers(fedRes.Servers)
		}

		if res.FrameID == "" {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(
					fmt.Sprintf("Frame alias %s not found", frameAlias),
				),
			}
		}
	} else {
		joinedHostsReq := federationAPI.QueryJoinedHostServerNamesInFrameRequest{FrameID: res.FrameID}
		var joinedHostsRes federationAPI.QueryJoinedHostServerNamesInFrameResponse
		if err = fedSenderAPI.QueryJoinedHostServerNamesInFrame(req.Context(), &joinedHostsReq, &joinedHostsRes); err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("fedSenderAPI.QueryJoinedHostServerNamesInFrame failed")
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

// SetLocalAlias implements PUT /directory/frame/{frameAlias}
func SetLocalAlias(
	req *http.Request,
	device *userapi.Device,
	alias string,
	cfg *config.ClientAPI,
	rsAPI dataframeAPI.ClientDataframeAPI,
) xutil.JSONResponse {
	_, domain, err := xtools.SplitID('#', alias)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Frame alias must be in the form '#localpart:domain'"),
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
		FrameID string `json:"frame_id"`
	}
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}

	frameID, err := spec.NewFrameID(r.FrameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid frame ID"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *frameID, *userID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QuerySenderIDForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	} else if senderID == nil {
		xutil.GetLogger(req.Context()).WithField("frameID", *frameID).WithField("userID", *userID).Error("Sender ID not found")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	aliasAlreadyExists, err := rsAPI.SetFrameAlias(req.Context(), *senderID, *frameID, alias)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("aliasAPI.SetFrameAlias failed")
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

// RemoveLocalAlias implements DELETE /directory/frame/{frameAlias}
func RemoveLocalAlias(
	req *http.Request,
	device *userapi.Device,
	alias string,
	rsAPI dataframeAPI.ClientDataframeAPI,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "UserID for device is invalid"},
		}
	}

	frameIDReq := dataframeAPI.GetFrameIDForAliasRequest{Alias: alias}
	frameIDRes := dataframeAPI.GetFrameIDForAliasResponse{}
	err = rsAPI.GetFrameIDForAlias(req.Context(), &frameIDReq, &frameIDRes)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}

	validFrameID, err := spec.NewFrameID(frameIDRes.FrameID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}

	// This seems like the kind of auth check that should be done in the dataframe, but
	// if this check fails (user is not in the frame), then there will be no SenderID for the user
	// for pseudo-ID frames - it will just return "". However, we can't use lack of a sender ID
	// as meaning they are not in the frame, since lacking a sender ID could be caused by other bugs.
	// TODO: maybe have QuerySenderIDForUser return richer errors?
	var queryResp dataframeAPI.QueryMembershipForUserResponse
	err = rsAPI.QueryMembershipForUser(req.Context(), &dataframeAPI.QueryMembershipForUserRequest{
		FrameID: validFrameID.String(),
		UserID: *userID,
	}, &queryResp)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("dataframeAPI.QueryMembershipForUser failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}
	if !queryResp.IsInFrame {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You do not have permission to remove this alias."),
		}
	}

	deviceSenderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validFrameID, *userID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The alias does not exist."),
		}
	}
	// TODO: how to handle this case? missing user/frame keys seem to be a whole new class of errors
	if deviceSenderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}

	aliasFound, aliasRemoved, err := rsAPI.RemoveFrameAlias(req.Context(), *deviceSenderID, alias)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("aliasAPI.RemoveFrameAlias failed")
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

type frameVisibility struct {
	Visibility string `json:"visibility"`
}

// GetVisibility implements GET /directory/list/frame/{frameID}
func GetVisibility(
	req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI,
	frameID string,
) xutil.JSONResponse {
	var res dataframeAPI.QueryPublishedFramesResponse
	err := rsAPI.QueryPublishedFrames(req.Context(), &dataframeAPI.QueryPublishedFramesRequest{
		FrameID: frameID,
	}, &res)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QueryPublishedFrames failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var v frameVisibility
	if len(res.FrameIDs) == 1 {
		v.Visibility = spec.Public
	} else {
		v.Visibility = "private"
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: v,
	}
}

// SetVisibility implements PUT /directory/list/frame/{frameID}
// TODO: Allow admin users to edit the frame visibility
func SetVisibility(
	req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI, dev *userapi.Device,
	frameID string,
) xutil.JSONResponse {
	deviceUserID, err := spec.NewUserID(dev.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("userID for this device is invalid"),
		}
	}
	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("frameID is invalid")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("FrameID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(req.Context(), *validFrameID, *deviceUserID)
	if err != nil || senderID == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("failed to find senderID for this user"),
		}
	}

	resErr := checkMemberInFrame(req.Context(), rsAPI, *deviceUserID, frameID)
	if resErr != nil {
		return *resErr
	}

	queryEventsReq := dataframeAPI.QueryLatestEventsAndStateRequest{
		FrameID: frameID,
		StateToFetch: []xtools.StateKeyTuple{{
			EventType: spec.MFramePowerLevels,
			StateKey:  "",
		}},
	}
	var queryEventsRes dataframeAPI.QueryLatestEventsAndStateResponse
	err = rsAPI.QueryLatestEventsAndState(req.Context(), &queryEventsReq, &queryEventsRes)
	if err != nil || len(queryEventsRes.StateEvents) == 0 {
		xutil.GetLogger(req.Context()).WithError(err).Error("could not query events from frame")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// NOTSPEC: Check if the user's power is greater than power required to change m.frame.canonical_alias event
	power, _ := xtools.NewPowerLevelContentFromEvent(queryEventsRes.StateEvents[0].PDU)
	if power.UserLevel(*senderID) < power.EventLevel(spec.MFrameCanonicalAlias, true) {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to change visibility"),
		}
	}

	var v frameVisibility
	if reqErr := httputil.UnmarshalJSONRequest(req, &v); reqErr != nil {
		return *reqErr
	}

	if err = rsAPI.PerformPublish(req.Context(), &dataframeAPI.PerformPublishRequest{
		FrameID:     frameID,
		Visibility: v.Visibility,
	}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("failed to publish frame")
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
	req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI, dev *userapi.Device,
	networkID, frameID string,
) xutil.JSONResponse {
	if dev.AccountType != userapi.AccountTypeAppService {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Only appservice may use this endpoint"),
		}
	}
	var v frameVisibility

	// If the method is delete, we simply mark the visibility as private
	if req.Method == http.MethodDelete {
		v.Visibility = "private"
	} else {
		if reqErr := httputil.UnmarshalJSONRequest(req, &v); reqErr != nil {
			return *reqErr
		}
	}
	if err := rsAPI.PerformPublish(req.Context(), &dataframeAPI.PerformPublishRequest{
		FrameID:       frameID,
		Visibility:   v.Visibility,
		NetworkID:    networkID,
		AppserviceID: dev.AppserviceID,
	}); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("failed to publish frame")
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
