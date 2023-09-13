package routing

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/services/dataframe/api"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	fedInternal "github.com/withqb/coddy/services/federationapi/internal"
	"github.com/withqb/coddy/services/federationapi/producers"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

const (
	SendRouteName           = "Send"
	QueryDirectoryRouteName = "QueryDirectory"
	QueryProfileRouteName   = "QueryProfile"
)

// Setup registers HTTP handlers with the given ServeMux.
// The provided publicAPIMux MUST have `UseEncodedPath()` enabled or else routes will incorrectly
// path unescape twice (once from the router, once from MakeFedAPI). We need to have this enabled
// so we can decode paths like foo/bar%2Fbaz as [foo, bar/baz] - by default it will decode to [foo, bar, baz]
//
// Due to Setup being used to call many other functions, a gocyclo nolint is
// applied:
// nolint: gocyclo
func Setup(
	routers httputil.Routers,
	dendriteCfg *config.Dendrite,
	rsAPI dataframeAPI.FederationDataframeAPI,
	fsAPI *fedInternal.FederationInternalAPI,
	keys xtools.JSONVerifier,
	federation fclient.FederationClient,
	userAPI userapi.FederationUserAPI,
	mscCfg *config.MSCs,
	producer *producers.SyncAPIProducer, enableMetrics bool,
) {
	fedMux := routers.Federation
	keyMux := routers.Keys
	wkMux := routers.WellKnown
	cfg := &dendriteCfg.FederationAPI

	if enableMetrics {
		prometheus.MustRegister(
			internal.PDUCountTotal, internal.EDUCountTotal,
		)
	}

	v2keysmux := keyMux.PathPrefix("/v2").Subrouter()
	v1fedmux := fedMux.PathPrefix("/v1").Subrouter()
	v2fedmux := fedMux.PathPrefix("/v2").Subrouter()
	v3fedmux := fedMux.PathPrefix("/v3").Subrouter()

	wakeup := &FederationWakeups{
		FsAPI: fsAPI,
	}

	localKeys := httputil.MakeExternalAPI("localkeys", func(req *http.Request) xutil.JSONResponse {
		return LocalKeys(cfg, spec.ServerName(req.Host))
	})

	notaryKeys := httputil.MakeExternalAPI("notarykeys", func(req *http.Request) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		var pkReq *xtools.PublicKeyNotaryLookupRequest
		serverName := spec.ServerName(vars["serverName"])
		keyID := xtools.KeyID(vars["keyID"])
		if serverName != "" && keyID != "" {
			pkReq = &xtools.PublicKeyNotaryLookupRequest{
				ServerKeys: map[spec.ServerName]map[xtools.KeyID]xtools.PublicKeyNotaryQueryCriteria{
					serverName: {
						keyID: xtools.PublicKeyNotaryQueryCriteria{},
					},
				},
			}
		}
		return NotaryKeys(req, cfg, fsAPI, pkReq)
	})

	if cfg.Coddy.WellKnownServerName != "" {
		logrus.Infof("Setting m.server as %s at /.well-known/coddy/server", cfg.Coddy.WellKnownServerName)
		wkMux.Handle("/server", httputil.MakeExternalAPI("wellknown", func(req *http.Request) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusOK,
				JSON: struct {
					ServerName string `json:"m.server"`
				}{
					ServerName: cfg.Coddy.WellKnownServerName,
				},
			}
		}),
		).Methods(http.MethodGet, http.MethodOptions)
	}

	// Ignore the {keyID} argument as we only have a single server key so we always
	// return that key.
	// Even if we had more than one server key, we would probably still ignore the
	// {keyID} argument and always return a response containing all of the keys.
	v2keysmux.Handle("/server/{keyID}", localKeys).Methods(http.MethodGet)
	v2keysmux.Handle("/server/", localKeys).Methods(http.MethodGet)
	v2keysmux.Handle("/server", localKeys).Methods(http.MethodGet)
	v2keysmux.Handle("/query", notaryKeys).Methods(http.MethodPost)
	v2keysmux.Handle("/query/{serverName}/{keyID}", notaryKeys).Methods(http.MethodGet)

	mu := internal.NewMutexByFrame()
	v1fedmux.Handle("/send/{txnID}", MakeFedAPI(
		"federation_send", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return Send(
				httpReq, request, xtools.TransactionID(vars["txnID"]),
				cfg, rsAPI, userAPI, keys, federation, mu, producer,
			)
		},
	)).Methods(http.MethodPut, http.MethodOptions).Name(SendRouteName)

	v1fedmux.Handle("/invite/{frameID}/{eventID}", MakeFedAPI(
		"federation_invite", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}

			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}
			return InviteV1(
				httpReq, request, *frameID, vars["eventID"],
				cfg, rsAPI, keys,
			)
		},
	)).Methods(http.MethodPut, http.MethodOptions)

	v2fedmux.Handle("/invite/{frameID}/{eventID}", MakeFedAPI(
		"federation_invite", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}

			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}
			return InviteV2(
				httpReq, request, *frameID, vars["eventID"],
				cfg, rsAPI, keys,
			)
		},
	)).Methods(http.MethodPut, http.MethodOptions)

	v3fedmux.Handle("/invite/{frameID}/{userID}", MakeFedAPI(
		"federation_invite", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}

			userID, err := spec.NewUserID(vars["userID"], true)
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid UserID"),
				}
			}
			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}
			return InviteV3(
				httpReq, request, *frameID, *userID,
				cfg, rsAPI, keys,
			)
		},
	)).Methods(http.MethodPut, http.MethodOptions)

	v1fedmux.Handle("/3pid/onbind", httputil.MakeExternalAPI("3pid_onbind",
		func(req *http.Request) xutil.JSONResponse {
			return CreateInvitesFrom3PIDInvites(req, rsAPI, cfg, federation, userAPI)
		},
	)).Methods(http.MethodPost, http.MethodOptions)

	v1fedmux.Handle("/exchange_third_party_invite/{frameID}", MakeFedAPI(
		"exchange_third_party_invite", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return ExchangeThirdPartyInvite(
				httpReq, request, vars["frameID"], rsAPI, cfg, federation,
			)
		},
	)).Methods(http.MethodPut, http.MethodOptions)

	v1fedmux.Handle("/event/{eventID}", MakeFedAPI(
		"federation_get_event", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return GetEvent(
				httpReq.Context(), request, rsAPI, vars["eventID"], cfg.Coddy.ServerName,
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/state/{frameID}", MakeFedAPI(
		"federation_get_state", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			return GetState(
				httpReq.Context(), request, rsAPI, vars["frameID"],
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/state_ids/{frameID}", MakeFedAPI(
		"federation_get_state_ids", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			return GetStateIDs(
				httpReq.Context(), request, rsAPI, vars["frameID"],
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/event_auth/{frameID}/{eventID}", MakeFedAPI(
		"federation_get_event_auth", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			return GetEventAuth(
				httpReq.Context(), request, rsAPI, vars["frameID"], vars["eventID"],
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/query/directory", MakeFedAPI(
		"federation_query_frame_alias", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return FrameAliasToID(
				httpReq, federation, cfg, rsAPI, fsAPI,
			)
		},
	)).Methods(http.MethodGet).Name(QueryDirectoryRouteName)

	v1fedmux.Handle("/query/profile", MakeFedAPI(
		"federation_query_profile", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return GetProfile(
				httpReq, userAPI, cfg,
			)
		},
	)).Methods(http.MethodGet).Name(QueryProfileRouteName)

	v1fedmux.Handle("/user/devices/{userID}", MakeFedAPI(
		"federation_user_devices", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return GetUserDevices(
				httpReq, userAPI, vars["userID"],
			)
		},
	)).Methods(http.MethodGet)

	if mscCfg.Enabled("msc2444") {
		v1fedmux.Handle("/peek/{frameID}/{peekID}", MakeFedAPI(
			"federation_peek", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
			func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
				if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
					return xutil.JSONResponse{
						Code: http.StatusForbidden,
						JSON: spec.Forbidden("Forbidden by server ACLs"),
					}
				}
				frameID := vars["frameID"]
				peekID := vars["peekID"]
				queryVars := httpReq.URL.Query()
				remoteVersions := []xtools.FrameVersion{}
				if vers, ok := queryVars["ver"]; ok {
					// The remote side supplied a ?ver= so use that to build up the list
					// of supported frame versions
					for _, v := range vers {
						remoteVersions = append(remoteVersions, xtools.FrameVersion(v))
					}
				} else {
					// The remote side didn't supply a ?ver= so just assume that they only
					// support frame version 1
					remoteVersions = append(remoteVersions, xtools.FrameVersionV1)
				}
				return Peek(
					httpReq, request, cfg, rsAPI, frameID, peekID, remoteVersions,
				)
			},
		)).Methods(http.MethodPut, http.MethodDelete)
	}

	v1fedmux.Handle("/make_join/{frameID}/{userID}", MakeFedAPI(
		"federation_make_join", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			queryVars := httpReq.URL.Query()
			remoteVersions := []xtools.FrameVersion{}
			if vers, ok := queryVars["ver"]; ok {
				// The remote side supplied a ?ver= so use that to build up the list
				// of supported frame versions
				for _, v := range vers {
					remoteVersions = append(remoteVersions, xtools.FrameVersion(v))
				}
			} else {
				// The remote side didn't supply a ?ver= so just assume that they only
				// support frame version 1, as per the spec
				remoteVersions = append(remoteVersions, xtools.FrameVersionV1)
			}

			userID, err := spec.NewUserID(vars["userID"], true)
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid UserID"),
				}
			}
			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}

			logrus.Debugf("Processing make_join for user %s, frame %s", userID.String(), frameID.String())
			return MakeJoin(
				httpReq, request, cfg, rsAPI, *frameID, *userID, remoteVersions,
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/send_join/{frameID}/{eventID}", MakeFedAPI(
		"federation_send_join", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			eventID := vars["eventID"]
			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}

			res := SendJoin(
				httpReq, request, cfg, rsAPI, keys, *frameID, eventID,
			)
			// not all responses get wrapped in [code, body]
			var body interface{}
			body = []interface{}{
				res.Code, res.JSON,
			}
			jerr, ok := res.JSON.(spec.CoddyError)
			if ok {
				body = jerr
			}

			return xutil.JSONResponse{
				Headers: res.Headers,
				Code:    res.Code,
				JSON:    body,
			}
		},
	)).Methods(http.MethodPut)

	v2fedmux.Handle("/send_join/{frameID}/{eventID}", MakeFedAPI(
		"federation_send_join", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			eventID := vars["eventID"]
			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}

			return SendJoin(
				httpReq, request, cfg, rsAPI, keys, *frameID, eventID,
			)
		},
	)).Methods(http.MethodPut)

	v1fedmux.Handle("/make_leave/{frameID}/{userID}", MakeFedAPI(
		"federation_make_leave", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			frameID, err := spec.NewFrameID(vars["frameID"])
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid FrameID"),
				}
			}
			userID, err := spec.NewUserID(vars["userID"], true)
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid UserID"),
				}
			}
			return MakeLeave(
				httpReq, request, cfg, rsAPI, *frameID, *userID,
			)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/send_leave/{frameID}/{eventID}", MakeFedAPI(
		"federation_send_leave", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			frameID := vars["frameID"]
			eventID := vars["eventID"]
			res := SendLeave(
				httpReq, request, cfg, rsAPI, keys, frameID, eventID,
			)
			// not all responses get wrapped in [code, body]
			var body interface{}
			body = []interface{}{
				res.Code, res.JSON,
			}
			jerr, ok := res.JSON.(spec.CoddyError)
			if ok {
				body = jerr
			}

			return xutil.JSONResponse{
				Headers: res.Headers,
				Code:    res.Code,
				JSON:    body,
			}
		},
	)).Methods(http.MethodPut)

	v2fedmux.Handle("/send_leave/{frameID}/{eventID}", MakeFedAPI(
		"federation_send_leave", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			frameID := vars["frameID"]
			eventID := vars["eventID"]
			return SendLeave(
				httpReq, request, cfg, rsAPI, keys, frameID, eventID,
			)
		},
	)).Methods(http.MethodPut)

	v1fedmux.Handle("/version", httputil.MakeExternalAPI(
		"federation_version",
		func(httpReq *http.Request) xutil.JSONResponse {
			return Version()
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/get_missing_events/{frameID}", MakeFedAPI(
		"federation_get_missing_events", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			return GetMissingEvents(httpReq, request, rsAPI, vars["frameID"])
		},
	)).Methods(http.MethodPost)

	v1fedmux.Handle("/backfill/{frameID}", MakeFedAPI(
		"federation_backfill", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			if dataframeAPI.IsServerBannedFromFrame(httpReq.Context(), rsAPI, vars["frameID"], request.Origin()) {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("Forbidden by server ACLs"),
				}
			}
			return Backfill(httpReq, request, rsAPI, vars["frameID"], cfg)
		},
	)).Methods(http.MethodGet)

	v1fedmux.Handle("/publicFrames",
		httputil.MakeExternalAPI("federation_public_frames", func(req *http.Request) xutil.JSONResponse {
			return GetPostPublicFrames(req, rsAPI)
		}),
	).Methods(http.MethodGet, http.MethodPost)

	v1fedmux.Handle("/user/keys/claim", MakeFedAPI(
		"federation_keys_claim", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return ClaimOneTimeKeys(httpReq, request, userAPI, cfg.Coddy.ServerName)
		},
	)).Methods(http.MethodPost)

	v1fedmux.Handle("/user/keys/query", MakeFedAPI(
		"federation_keys_query", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return QueryDeviceKeys(httpReq, request, userAPI, cfg.Coddy.ServerName)
		},
	)).Methods(http.MethodPost)

	v1fedmux.Handle("/openid/userinfo",
		httputil.MakeExternalAPI("federation_openid_userinfo", func(req *http.Request) xutil.JSONResponse {
			return GetOpenIDUserInfo(req, userAPI)
		}),
	).Methods(http.MethodGet)

	v1fedmux.Handle("/hierarchy/{frameID}", MakeFedAPI(
		"federation_frame_hierarchy", cfg.Coddy.ServerName, cfg.Coddy.IsLocalServerName, keys, wakeup,
		func(httpReq *http.Request, request *fclient.FederationRequest, vars map[string]string) xutil.JSONResponse {
			return QueryFrameHierarchy(httpReq, request, vars["frameID"], rsAPI)
		},
	)).Methods(http.MethodGet)
}

func ErrorIfLocalServerNotInFrame(
	ctx context.Context,
	rsAPI api.FederationDataframeAPI,
	frameID string,
) *xutil.JSONResponse {
	// Check if we think we're in this frame. If we aren't then
	// we won't waste CPU cycles serving this request.
	joinedReq := &api.QueryServerJoinedToFrameRequest{
		FrameID: frameID,
	}
	joinedRes := &api.QueryServerJoinedToFrameResponse{}
	if err := rsAPI.QueryServerJoinedToFrame(ctx, joinedReq, joinedRes); err != nil {
		res := xutil.ErrorResponse(err)
		return &res
	}
	if !joinedRes.IsInFrame {
		return &xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("This server is not joined to frame %s", frameID)),
		}
	}
	return nil
}

// MakeFedAPI makes an http.Handler that checks coddy federation authentication.
func MakeFedAPI(
	metricsName string, serverName spec.ServerName,
	isLocalServerName func(spec.ServerName) bool,
	keyRing xtools.JSONVerifier,
	wakeup *FederationWakeups,
	f func(*http.Request, *fclient.FederationRequest, map[string]string) xutil.JSONResponse,
) http.Handler {
	h := func(req *http.Request) xutil.JSONResponse {
		fedReq, errResp := fclient.VerifyHTTPRequest(
			req, time.Now(), serverName, isLocalServerName, keyRing,
		)
		if fedReq == nil {
			return errResp
		}
		// add the user to Sentry, if enabled
		hub := sentry.GetHubFromContext(req.Context())
		if hub != nil {
			hub.Scope().SetTag("origin", string(fedReq.Origin()))
			hub.Scope().SetTag("uri", fedReq.RequestURI())
		}
		defer func() {
			if r := recover(); r != nil {
				if hub != nil {
					hub.CaptureException(fmt.Errorf("%s panicked", req.URL.Path))
				}
				// re-panic to return the 500
				panic(r)
			}
		}()
		go wakeup.Wakeup(req.Context(), fedReq.Origin())
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.CoddyErrorResponse(400, string(spec.ErrorUnrecognized), "badly encoded query params")
		}

		jsonRes := f(req, fedReq, vars)
		// do not log 4xx as errors as they are client fails, not server fails
		if hub != nil && jsonRes.Code >= 500 {
			hub.Scope().SetExtra("response", jsonRes)
			hub.CaptureException(fmt.Errorf("%s returned HTTP %d", req.URL.Path, jsonRes.Code))
		}
		return jsonRes
	}
	return httputil.MakeExternalAPI(metricsName, h)
}

type FederationWakeups struct {
	FsAPI   *fedInternal.FederationInternalAPI
	origins sync.Map
}

func (f *FederationWakeups) Wakeup(ctx context.Context, origin spec.ServerName) {
	key, keyok := f.origins.Load(origin)
	if keyok {
		lastTime, ok := key.(time.Time)
		if ok && time.Since(lastTime) < time.Minute {
			return
		}
	}
	f.FsAPI.MarkServersAlive([]spec.ServerName{origin})
	f.origins.Store(origin, time.Now())
}
