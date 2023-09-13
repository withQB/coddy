package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
	"golang.org/x/exp/constraints"

	clientapi "github.com/withqb/coddy/services/clientapi/api"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/userapi/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
)

var validRegistrationTokenRegex = regexp.MustCompile("^[[:ascii:][:digit:]_]*$")

func AdminCreateNewRegistrationToken(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) xutil.JSONResponse {
	if !cfg.RegistrationRequiresToken {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Registration via tokens is not enabled on this homeserver"),
		}
	}
	request := struct {
		Token       string `json:"token"`
		UsesAllowed *int32 `json:"uses_allowed,omitempty"`
		ExpiryTime  *int64 `json:"expiry_time,omitempty"`
		Length      int32  `json:"length"`
	}{}

	if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(fmt.Sprintf("failed to decode request body: %s", err)),
		}
	}

	token := request.Token
	usesAllowed := request.UsesAllowed
	expiryTime := request.ExpiryTime
	length := request.Length

	if len(token) == 0 {
		if length == 0 {
			// length not provided in request. Assign default value of 16.
			length = 16
		}
		// token not present in request body. Hence, generate a random token.
		if length <= 0 || length > 64 {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("length must be greater than zero and not greater than 64"),
			}
		}
		token = xutil.RandomString(int(length))
	}

	if len(token) > 64 {
		//Token present in request body, but is too long.
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("token must not be longer than 64"),
		}
	}

	isTokenValid := validRegistrationTokenRegex.Match([]byte(token))
	if !isTokenValid {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("token must consist only of characters matched by the regex [A-Za-z0-9-_]"),
		}
	}
	// At this point, we have a valid token, either through request body or through random generation.
	if usesAllowed != nil && *usesAllowed < 0 {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("uses_allowed must be a non-negative integer or null"),
		}
	}
	if expiryTime != nil && spec.Timestamp(*expiryTime).Time().Before(time.Now()) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("expiry_time must not be in the past"),
		}
	}
	pending := int32(0)
	completed := int32(0)
	// If usesAllowed or expiryTime is 0, it means they are not present in the request. NULL (indicating unlimited uses / no expiration will be persisted in DB)
	registrationToken := &clientapi.RegistrationToken{
		Token:       &token,
		UsesAllowed: usesAllowed,
		Pending:     &pending,
		Completed:   &completed,
		ExpiryTime:  expiryTime,
	}
	created, err := userAPI.PerformAdminCreateRegistrationToken(req.Context(), registrationToken)
	if !created {
		return xutil.JSONResponse{
			Code: http.StatusConflict,
			JSON: map[string]string{
				"error": fmt.Sprintf("token: %s already exists", token),
			},
		}
	}
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: map[string]interface{}{
			"token":        token,
			"uses_allowed": getReturnValue(usesAllowed),
			"pending":      pending,
			"completed":    completed,
			"expiry_time":  getReturnValue(expiryTime),
		},
	}
}

func getReturnValue[t constraints.Integer](in *t) any {
	if in == nil {
		return nil
	}
	return *in
}

func AdminListRegistrationTokens(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) xutil.JSONResponse {
	queryParams := req.URL.Query()
	returnAll := true
	valid := true
	validQuery, ok := queryParams["valid"]
	if ok {
		returnAll = false
		validValue, err := strconv.ParseBool(validQuery[0])
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("invalid 'valid' query parameter"),
			}
		}
		valid = validValue
	}
	tokens, err := userAPI.PerformAdminListRegistrationTokens(req.Context(), returnAll, valid)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.ErrorUnknown,
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: map[string]interface{}{
			"registration_tokens": tokens,
		},
	}
}

func AdminGetRegistrationToken(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	tokenText := vars["token"]
	token, err := userAPI.PerformAdminGetRegistrationToken(req.Context(), tokenText)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("token: %s not found", tokenText)),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: token,
	}
}

func AdminDeleteRegistrationToken(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	tokenText := vars["token"]
	err = userAPI.PerformAdminDeleteRegistrationToken(req.Context(), tokenText)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: err,
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: map[string]interface{}{},
	}
}

func AdminUpdateRegistrationToken(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	tokenText := vars["token"]
	request := make(map[string]*int64)
	if err = json.NewDecoder(req.Body).Decode(&request); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(fmt.Sprintf("failed to decode request body: %s", err)),
		}
	}
	newAttributes := make(map[string]interface{})
	usesAllowed, ok := request["uses_allowed"]
	if ok {
		// Only add usesAllowed to newAtrributes if it is present and valid
		if usesAllowed != nil && *usesAllowed < 0 {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("uses_allowed must be a non-negative integer or null"),
			}
		}
		newAttributes["usesAllowed"] = usesAllowed
	}
	expiryTime, ok := request["expiry_time"]
	if ok {
		// Only add expiryTime to newAtrributes if it is present and valid
		if expiryTime != nil && spec.Timestamp(*expiryTime).Time().Before(time.Now()) {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("expiry_time must not be in the past"),
			}
		}
		newAttributes["expiryTime"] = expiryTime
	}
	if len(newAttributes) == 0 {
		// No attributes to update. Return existing token
		return AdminGetRegistrationToken(req, cfg, userAPI)
	}
	updatedToken, err := userAPI.PerformAdminUpdateRegistrationToken(req.Context(), tokenText, newAttributes)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("token: %s not found", tokenText)),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: *updatedToken,
	}
}

func AdminEvacuateFrame(req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	affected, err := rsAPI.PerformAdminEvacuateFrame(req.Context(), vars["frameID"])
	switch err.(type) {
	case nil:
	case eventutil.ErrFrameNoExists:
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(err.Error()),
		}
	default:
		logrus.WithError(err).WithField("frameID", vars["frameID"]).Error("failed to evacuate frame")
		return xutil.ErrorResponse(err)
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: map[string]interface{}{
			"affected": affected,
		},
	}
}

func AdminEvacuateUser(req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	affected, err := rsAPI.PerformAdminEvacuateUser(req.Context(), vars["userID"])
	if err != nil {
		logrus.WithError(err).WithField("userID", vars["userID"]).Error("failed to evacuate user")
		return xutil.MessageResponse(http.StatusBadRequest, err.Error())
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: map[string]interface{}{
			"affected": affected,
		},
	}
}

func AdminPurgeFrame(req *http.Request, rsAPI dataframeAPI.ClientDataframeAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	if err = rsAPI.PerformAdminPurgeFrame(context.Background(), vars["frameID"]); err != nil {
		return xutil.ErrorResponse(err)
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: struct{}{},
	}
}

func AdminResetPassword(req *http.Request, cfg *config.ClientAPI, device *api.Device, userAPI api.ClientUserAPI) xutil.JSONResponse {
	if req.Body == nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("Missing request body"),
		}
	}
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	var localpart string
	userID := vars["userID"]
	localpart, serverName, err := cfg.Coddy.SplitLocalID('@', userID)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}
	accAvailableResp := &api.QueryAccountAvailabilityResponse{}
	if err = userAPI.QueryAccountAvailability(req.Context(), &api.QueryAccountAvailabilityRequest{
		Localpart:  localpart,
		ServerName: serverName,
	}, accAvailableResp); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if accAvailableResp.Available {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.Unknown("User does not exist"),
		}
	}
	request := struct {
		Password      string `json:"password"`
		LogoutDevices bool   `json:"logout_devices"`
	}{}
	if err = json.NewDecoder(req.Body).Decode(&request); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("failed to decode request body: " + err.Error()),
		}
	}
	if request.Password == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Expecting non-empty password."),
		}
	}

	if err = internal.ValidatePassword(request.Password); err != nil {
		return *internal.PasswordResponse(err)
	}

	updateReq := &api.PerformPasswordUpdateRequest{
		Localpart:     localpart,
		ServerName:    serverName,
		Password:      request.Password,
		LogoutDevices: request.LogoutDevices,
	}
	updateRes := &api.PerformPasswordUpdateResponse{}
	if err := userAPI.PerformPasswordUpdate(req.Context(), updateReq, updateRes); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("failed to perform password update: " + err.Error()),
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			Updated bool `json:"password_updated"`
		}{
			Updated: updateRes.PasswordUpdated,
		},
	}
}

func AdminReindex(req *http.Request, cfg *config.ClientAPI, device *api.Device, natsClient *nats.Conn) xutil.JSONResponse {
	_, err := natsClient.RequestMsg(nats.NewMsg(cfg.Coddy.JetStream.Prefixed(jetstream.InputFulltextReindex)), time.Second*10)
	if err != nil {
		logrus.WithError(err).Error("failed to publish nats message")
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

func AdminMarkAsStale(req *http.Request, cfg *config.ClientAPI, keyAPI api.ClientKeyAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	userID := vars["userID"]

	_, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		return xutil.MessageResponse(http.StatusBadRequest, err.Error())
	}
	if cfg.Coddy.IsLocalServerName(domain) {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Can not mark local device list as stale"),
		}
	}

	err = keyAPI.PerformMarkAsStaleIfNeeded(req.Context(), &api.PerformMarkAsStaleRequest{
		UserID: userID,
		Domain: domain,
	}, &struct{}{})
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown(fmt.Sprintf("failed to mark device list as stale: %s", err)),
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

func AdminDownloadState(req *http.Request, device *api.Device, rsAPI dataframeAPI.ClientDataframeAPI) xutil.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
	if err != nil {
		return xutil.ErrorResponse(err)
	}
	frameID, ok := vars["frameID"]
	if !ok {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Expecting frame ID."),
		}
	}
	serverName, ok := vars["serverName"]
	if !ok {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Expecting remote server name."),
		}
	}
	if err = rsAPI.PerformAdminDownloadState(req.Context(), frameID, device.UserID, spec.ServerName(serverName)); err != nil {
		if errors.Is(err, eventutil.ErrFrameNoExists{}) {
			return xutil.JSONResponse{
				Code: 200,
				JSON: spec.NotFound(err.Error()),
			}
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"userID":     device.UserID,
			"serverName": serverName,
			"frameID":     frameID,
		}).Error("failed to download state")
		return xutil.ErrorResponse(err)
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: struct{}{},
	}
}
