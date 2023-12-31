package routing

import (
	"context"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
	"golang.org/x/sync/singleflight"

	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/base"

	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/internal/transactions"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/clientapi/api"
	"github.com/withqb/coddy/services/clientapi/auth"
	clientxutil "github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/clientapi/producers"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	federationAPI "github.com/withqb/coddy/services/federationapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
)

type WellKnownClientHomeserver struct {
	BaseUrl string `json:"base_url"`
}

type WellKnownSlidingSyncProxy struct {
	Url string `json:"url"`
}

type WellKnownClientResponse struct {
	Homeserver       WellKnownClientHomeserver  `json:"m.homeserver"`
	SlidingSyncProxy *WellKnownSlidingSyncProxy `json:"org.coddy.msc3575.proxy,omitempty"`
}

// Setup registers HTTP handlers with the given ServeMux. It also supplies the given http.Client
// to clients which need to make outbound HTTP requests.
//
// Due to Setup being used to call many other functions, a gocyclo nolint is
// applied:
// nolint: gocyclo
func Setup(
	routers httputil.Routers,
	dendriteCfg *config.Dendrite,
	rsAPI dataframeAPI.ClientDataframeAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	userAPI userapi.ClientUserAPI,
	userDirectoryProvider userapi.QuerySearchProfilesAPI,
	federation fclient.FederationClient,
	syncProducer *producers.SyncAPIProducer,
	transactionsCache *transactions.Cache,
	federationSender federationAPI.ClientFederationAPI,
	extFramesProvider api.ExtraPublicFramesProvider,
	natsClient *nats.Conn, enableMetrics bool,
) {
	cfg := &dendriteCfg.ClientAPI
	mscCfg := &dendriteCfg.MSCs
	publicAPIMux := routers.Client
	wkMux := routers.WellKnown
	synapseAdminRouter := routers.SynapseAdmin
	dendriteAdminRouter := routers.DendriteAdmin

	if enableMetrics {
		prometheus.MustRegister(amtRegUsers, sendEventDuration)
	}

	rateLimits := httputil.NewRateLimits(&cfg.RateLimiting)
	userInteractiveAuth := auth.NewUserInteractive(userAPI, cfg)

	unstableFeatures := map[string]bool{
		"org.coddy.e2e_cross_signing": true,
		"org.coddy.msc2285.stable":    true,
	}
	for _, msc := range cfg.MSCs.MSCs {
		unstableFeatures["org.coddy."+msc] = true
	}

	// singleflight protects /join endpoints from being invoked
	// multiple times from the same user and frame, otherwise
	// a state reset can occur. This also avoids unneeded
	// state calculations.
	// TDO: actually fix this in the dataframe, as there are
	// 		 possibly other ways that can result in a stat reset.
	sf := singleflight.Group{}

	if cfg.Coddy.WellKnownClientName != "" {
		logrus.Infof("Setting m.homeserver base_url as %s at /.well-known/coddy/client", cfg.Coddy.WellKnownClientName)
		if cfg.Coddy.WellKnownSlidingSyncProxy != "" {
			logrus.Infof("Setting org.coddy.msc3575.proxy url as %s at /.well-known/coddy/client", cfg.Coddy.WellKnownSlidingSyncProxy)
		}
		wkMux.Handle("/client", httputil.MakeExternalAPI("wellknown", func(r *http.Request) xutil.JSONResponse {
			response := WellKnownClientResponse{
				Homeserver: WellKnownClientHomeserver{cfg.Coddy.WellKnownClientName},
			}
			if cfg.Coddy.WellKnownSlidingSyncProxy != "" {
				response.SlidingSyncProxy = &WellKnownSlidingSyncProxy{
					Url: cfg.Coddy.WellKnownSlidingSyncProxy,
				}
			}

			return xutil.JSONResponse{
				Code: http.StatusOK,
				JSON: response,
			}
		})).Methods(http.MethodGet, http.MethodOptions)
	}

	publicAPIMux.Handle("/versions",
		httputil.MakeExternalAPI("versions", func(req *http.Request) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusOK,
				JSON: struct {
					Versions         []string        `json:"versions"`
					UnstableFeatures map[string]bool `json:"unstable_features"`
				}{Versions: []string{
					"r0.0.1",
					"r0.1.0",
					"r0.2.0",
					"r0.3.0",
					"r0.4.0",
					"r0.5.0",
					"r0.6.1",
					"v1.0",
					"v1.1",
					"v1.2",
				}, UnstableFeatures: unstableFeatures},
			}
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	if cfg.RegistrationSharedSecret != "" {
		logrus.Info("Enabling shared secret registration at /_synapse/admin/v1/register")
		sr := NewSharedSecretRegistration(cfg.RegistrationSharedSecret)
		synapseAdminRouter.Handle("/admin/v1/register",
			httputil.MakeExternalAPI("shared_secret_registration", func(req *http.Request) xutil.JSONResponse {
				if req.Method == http.MethodGet {
					return xutil.JSONResponse{
						Code: 200,
						JSON: struct {
							Nonce string `json:"nonce"`
						}{
							Nonce: sr.GenerateNonce(),
						},
					}
				}
				if req.Method == http.MethodPost {
					return handleSharedSecretRegistration(cfg, userAPI, sr, req)
				}
				return xutil.JSONResponse{
					Code: http.StatusMethodNotAllowed,
					JSON: spec.NotFound("unknown method"),
				}
			}),
		).Methods(http.MethodGet, http.MethodPost, http.MethodOptions)
	}
	dendriteAdminRouter.Handle("/admin/registrationTokens/new",
		httputil.MakeAdminAPI("admin_registration_tokens_new", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminCreateNewRegistrationToken(req, cfg, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/registrationTokens",
		httputil.MakeAdminAPI("admin_list_registration_tokens", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminListRegistrationTokens(req, cfg, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/registrationTokens/{token}",
		httputil.MakeAdminAPI("admin_get_registration_token", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			switch req.Method {
			case http.MethodGet:
				return AdminGetRegistrationToken(req, cfg, userAPI)
			case http.MethodPut:
				return AdminUpdateRegistrationToken(req, cfg, userAPI)
			case http.MethodDelete:
				return AdminDeleteRegistrationToken(req, cfg, userAPI)
			default:
				return xutil.CoddyErrorResponse(
					404,
					string(spec.ErrorNotFound),
					"unknown method",
				)
			}
		}),
	).Methods(http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/evacuateFrame/{frameID}",
		httputil.MakeAdminAPI("admin_evacuate_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminEvacuateFrame(req, rsAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/evacuateUser/{userID}",
		httputil.MakeAdminAPI("admin_evacuate_user", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminEvacuateUser(req, rsAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/purgeFrame/{frameID}",
		httputil.MakeAdminAPI("admin_purge_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminPurgeFrame(req, rsAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/resetPassword/{userID}",
		httputil.MakeAdminAPI("admin_reset_password", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminResetPassword(req, cfg, device, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/downloadState/{serverName}/{frameID}",
		httputil.MakeAdminAPI("admin_download_state", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminDownloadState(req, device, rsAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/fulltext/reindex",
		httputil.MakeAdminAPI("admin_fultext_reindex", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminReindex(req, cfg, device, natsClient)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	dendriteAdminRouter.Handle("/admin/refreshDevices/{userID}",
		httputil.MakeAdminAPI("admin_refresh_devices", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return AdminMarkAsStale(req, cfg, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	// server notifications
	if cfg.Coddy.ServerNotices.Enabled {
		logrus.Info("Enabling server notices at /_synapse/admin/v1/send_server_notice")
		serverNotificationSender, err := getSenderDevice(context.Background(), rsAPI, userAPI, cfg)
		if err != nil {
			logrus.WithError(err).Fatal("unable to get account for sending sending server notices")
		}

		synapseAdminRouter.Handle("/admin/v1/send_server_notice/{txnID}",
			httputil.MakeAuthAPI("send_server_notice", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
				// not specced, but ensure we're rate limiting requests to this endpoint
				if r := rateLimits.Limit(req, device); r != nil {
					return *r
				}
				var vars map[string]string
				vars, err = httputil.URLDecodeMapValues(mux.Vars(req))
				if err != nil {
					return xutil.ErrorResponse(err)
				}
				txnID := vars["txnID"]
				return SendServerNotice(
					req, &cfg.Coddy.ServerNotices,
					cfg, userAPI, rsAPI, asAPI,
					device, serverNotificationSender,
					&txnID, transactionsCache,
				)
			}),
		).Methods(http.MethodPut, http.MethodOptions)

		synapseAdminRouter.Handle("/admin/v1/send_server_notice",
			httputil.MakeAuthAPI("send_server_notice", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
				// not specced, but ensure we're rate limiting requests to this endpoint
				if r := rateLimits.Limit(req, device); r != nil {
					return *r
				}
				return SendServerNotice(
					req, &cfg.Coddy.ServerNotices,
					cfg, userAPI, rsAPI, asAPI,
					device, serverNotificationSender,
					nil, transactionsCache,
				)
			}),
		).Methods(http.MethodPost, http.MethodOptions)
	}

	// You can't just do PathPrefix("/(r0|v3)") because regexps only apply when inside named path variables.
	// So make a named path variable called 'apiversion' (which we will never read in handlers) and then do
	// (r0|v3) - BUT this is a captured group, which makes no sense because you cannot extract this group
	// from a match (gorilla/mux exposes no way to do this) so it demands you make it a non-capturing group
	// using ?: so the final regexp becomes what is below. We also need a trailing slash to stop 'v33333' matching.
	// Note that 'apiversion' is chosen because it must not collide with a variable used in any of the routing!
	v3mux := publicAPIMux.PathPrefix("/{apiversion:(?:r0|v3)}/").Subrouter()

	v1mux := publicAPIMux.PathPrefix("/v1/").Subrouter()

	unstableMux := publicAPIMux.PathPrefix("/unstable").Subrouter()

	v3mux.Handle("/createFrame",
		httputil.MakeAuthAPI("createFrame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return CreateFrame(req, device, cfg, userAPI, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/join/{frameIDOrAlias}",
		httputil.MakeAuthAPI(spec.Join, userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			// Only execute a join for frameIDOrAlias and UserID once. If there is a join in progress
			// it waits for it to complete and returns that result for subsequent requests.
			resp, _, _ := sf.Do(vars["frameIDOrAlias"]+device.UserID, func() (any, error) {
				return JoinFrameByIDOrAlias(
					req, device, rsAPI, userAPI, vars["frameIDOrAlias"],
				), nil
			})
			// once all joins are processed, drop them from the cache. Further requests
			// will be processed as usual.
			sf.Forget(vars["frameIDOrAlias"] + device.UserID)
			return resp.(xutil.JSONResponse)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)

	if mscCfg.Enabled("msc2753") {
		v3mux.Handle("/peek/{frameIDOrAlias}",
			httputil.MakeAuthAPI(spec.Peek, userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
				if r := rateLimits.Limit(req, device); r != nil {
					return *r
				}
				vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
				if err != nil {
					return xutil.ErrorResponse(err)
				}
				return PeekFrameByIDOrAlias(
					req, device, rsAPI, vars["frameIDOrAlias"],
				)
			}),
		).Methods(http.MethodPost, http.MethodOptions)
	}
	v3mux.Handle("/joined_frames",
		httputil.MakeAuthAPI("joined_frames", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetJoinedFrames(req, device, rsAPI)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/join",
		httputil.MakeAuthAPI(spec.Join, userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			// Only execute a join for frameID and UserID once. If there is a join in progress
			// it waits for it to complete and returns that result for subsequent requests.
			resp, _, _ := sf.Do(vars["frameID"]+device.UserID, func() (any, error) {
				return JoinFrameByIDOrAlias(
					req, device, rsAPI, userAPI, vars["frameID"],
				), nil
			})
			// once all joins are processed, drop them from the cache. Further requests
			// will be processed as usual.
			sf.Forget(vars["frameID"] + device.UserID)
			return resp.(xutil.JSONResponse)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/leave",
		httputil.MakeAuthAPI("membership", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return LeaveFrameByID(
				req, device, rsAPI, vars["frameID"],
			)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/unpeek",
		httputil.MakeAuthAPI("unpeek", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return UnpeekFrameByID(
				req, device, rsAPI, vars["frameID"],
			)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/ban",
		httputil.MakeAuthAPI("membership", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendBan(req, userAPI, device, vars["frameID"], cfg, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/invite",
		httputil.MakeAuthAPI("membership", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendInvite(req, userAPI, device, vars["frameID"], cfg, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/kick",
		httputil.MakeAuthAPI("membership", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendKick(req, userAPI, device, vars["frameID"], cfg, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/unban",
		httputil.MakeAuthAPI("membership", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendUnban(req, userAPI, device, vars["frameID"], cfg, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/send/{eventType}",
		httputil.MakeAuthAPI("send_message", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendEvent(req, device, vars["frameID"], vars["eventType"], nil, nil, cfg, rsAPI, nil)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/send/{eventType}/{txnID}",
		httputil.MakeAuthAPI("send_message", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			txnID := vars["txnID"]
			return SendEvent(req, device, vars["frameID"], vars["eventType"], &txnID,
				nil, cfg, rsAPI, transactionsCache)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/state", httputil.MakeAuthAPI("frame_state", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return OnIncomingStateRequest(req.Context(), device, rsAPI, vars["frameID"])
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/aliases", httputil.MakeAuthAPI("aliases", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return GetAliases(req, rsAPI, device, vars["frameID"])
	})).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/state/{type:[^/]+/?}", httputil.MakeAuthAPI("frame_state", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		// If there's a trailing slash, remove it
		eventType := strings.TrimSuffix(vars["type"], "/")
		eventFormat := req.URL.Query().Get("format") == "event"
		return OnIncomingStateTypeRequest(req.Context(), device, rsAPI, vars["frameID"], eventType, "", eventFormat)
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/state/{type}/{stateKey}", httputil.MakeAuthAPI("frame_state", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		eventFormat := req.URL.Query().Get("format") == "event"
		return OnIncomingStateTypeRequest(req.Context(), device, rsAPI, vars["frameID"], vars["type"], vars["stateKey"], eventFormat)
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/state/{eventType:[^/]+/?}",
		httputil.MakeAuthAPI("send_message", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			emptyString := ""
			eventType := strings.TrimSuffix(vars["eventType"], "/")
			return SendEvent(req, device, vars["frameID"], eventType, nil, &emptyString, cfg, rsAPI, nil)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/state/{eventType}/{stateKey}",
		httputil.MakeAuthAPI("send_message", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			stateKey := vars["stateKey"]
			return SendEvent(req, device, vars["frameID"], vars["eventType"], nil, &stateKey, cfg, rsAPI, nil)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	// Defined outside of handler to persist between calls
	// TDO: clear based on some criteria
	frameHierarchyPaginationCache := NewFrameHierarchyPaginationCache()
	v1mux.Handle("/frames/{frameID}/hierarchy",
		httputil.MakeAuthAPI("spaces", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return QueryFrameHierarchy(req, device, vars["frameID"], rsAPI, &frameHierarchyPaginationCache)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/register", httputil.MakeExternalAPI("register", func(req *http.Request) xutil.JSONResponse {
		if r := rateLimits.Limit(req, nil); r != nil {
			return *r
		}
		return Register(req, userAPI, cfg)
	})).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/register/available", httputil.MakeExternalAPI("registerAvailable", func(req *http.Request) xutil.JSONResponse {
		if r := rateLimits.Limit(req, nil); r != nil {
			return *r
		}
		return RegisterAvailable(req, cfg, userAPI)
	})).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/directory/frame/{frameAlias}",
		httputil.MakeExternalAPI("directory_frame", func(req *http.Request) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return DirectoryFrame(req, vars["frameAlias"], federation, cfg, rsAPI, federationSender)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/directory/frame/{frameAlias}",
		httputil.MakeAuthAPI("directory_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetLocalAlias(req, device, vars["frameAlias"], cfg, rsAPI)
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/directory/frame/{frameAlias}",
		httputil.MakeAuthAPI("directory_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return RemoveLocalAlias(req, device, vars["frameAlias"], rsAPI)
		}),
	).Methods(http.MethodDelete, http.MethodOptions)
	v3mux.Handle("/directory/list/frame/{frameID}",
		httputil.MakeExternalAPI("directory_list", func(req *http.Request) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetVisibility(req, rsAPI, vars["frameID"])
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/directory/list/frame/{frameID}",
		httputil.MakeAuthAPI("directory_list", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetVisibility(req, rsAPI, device, vars["frameID"])
		}),
	).Methods(http.MethodPut, http.MethodOptions)
	v3mux.Handle("/directory/list/appservice/{networkID}/{frameID}",
		httputil.MakeAuthAPI("directory_list", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetVisibilityAS(req, rsAPI, device, vars["networkID"], vars["frameID"])
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	// Undocumented endpoint
	v3mux.Handle("/directory/list/appservice/{networkID}/{frameID}",
		httputil.MakeAuthAPI("directory_list", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetVisibilityAS(req, rsAPI, device, vars["networkID"], vars["frameID"])
		}),
	).Methods(http.MethodDelete, http.MethodOptions)

	v3mux.Handle("/publicFrames",
		httputil.MakeExternalAPI("public_frames", func(req *http.Request) xutil.JSONResponse {
			return GetPostPublicFrames(req, rsAPI, extFramesProvider, federation, cfg)
		}),
	).Methods(http.MethodGet, http.MethodPost, http.MethodOptions)

	v3mux.Handle("/logout",
		httputil.MakeAuthAPI("logout", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return Logout(req, userAPI, device)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/logout/all",
		httputil.MakeAuthAPI("logout", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return LogoutAll(req, userAPI, device)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/typing/{userID}",
		httputil.MakeAuthAPI("frames_typing", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendTyping(req, device, vars["frameID"], vars["userID"], rsAPI, syncProducer)
		}),
	).Methods(http.MethodPut, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/redact/{eventID}",
		httputil.MakeAuthAPI("frames_redact", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendRedaction(req, device, vars["frameID"], vars["eventID"], cfg, rsAPI, nil, nil)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameID}/redact/{eventID}/{txnId}",
		httputil.MakeAuthAPI("frames_redact", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			txnID := vars["txnId"]
			return SendRedaction(req, device, vars["frameID"], vars["eventID"], cfg, rsAPI, &txnID, transactionsCache)
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/sendToDevice/{eventType}/{txnID}",
		httputil.MakeAuthAPI("send_to_device", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			txnID := vars["txnID"]
			return SendToDevice(req, device, syncProducer, transactionsCache, vars["eventType"], &txnID)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	// This is only here because sytest refers to /unstable for this endpoint
	// rather than r0. It's an exact duplicate of the above handler.
	// TDO: Remove this if/when sytest is fixed!
	unstableMux.Handle("/sendToDevice/{eventType}/{txnID}",
		httputil.MakeAuthAPI("send_to_device", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			txnID := vars["txnID"]
			return SendToDevice(req, device, syncProducer, transactionsCache, vars["eventType"], &txnID)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/account/whoami",
		httputil.MakeAuthAPI("whoami", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return Whoami(req, device)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/account/password",
		httputil.MakeAuthAPI("password", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return Password(req, userAPI, device, cfg)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/account/deactivate",
		httputil.MakeAuthAPI("deactivate", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return Deactivate(req, userInteractiveAuth, userAPI, device)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	// Stub endpoints required by Element

	v3mux.Handle("/login",
		httputil.MakeExternalAPI("login", func(req *http.Request) xutil.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			return Login(req, userAPI, cfg)
		}),
	).Methods(http.MethodGet, http.MethodPost, http.MethodOptions)

	v3mux.Handle("/auth/{authType}/fallback/web",
		httputil.MakeHTMLAPI("auth_fallback", enableMetrics, func(w http.ResponseWriter, req *http.Request) {
			vars := mux.Vars(req)
			AuthFallback(w, req, vars["authType"], cfg)
		}),
	).Methods(http.MethodGet, http.MethodPost, http.MethodOptions)

	// Push rules

	v3mux.Handle("/pushrules",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("missing trailing slash"),
			}
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetAllPushRules(req.Context(), device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("scope, kind and rule ID must be specified"),
			}
		}),
	).Methods(http.MethodPut)

	v3mux.Handle("/pushrules/{scope}/",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetPushRulesByScope(req.Context(), vars["scope"], device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("missing trailing slash after scope"),
			}
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope:[^/]+/?}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("kind and rule ID must be specified"),
			}
		}),
	).Methods(http.MethodPut)

	v3mux.Handle("/pushrules/{scope}/{kind}/",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetPushRulesByKind(req.Context(), vars["scope"], vars["kind"], device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope}/{kind}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("missing trailing slash after kind"),
			}
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope}/{kind:[^/]+/?}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("rule ID must be specified"),
			}
		}),
	).Methods(http.MethodPut)

	v3mux.Handle("/pushrules/{scope}/{kind}/{ruleID}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetPushRuleByRuleID(req.Context(), vars["scope"], vars["kind"], vars["ruleID"], device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope}/{kind}/{ruleID}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			query := req.URL.Query()
			return PutPushRuleByRuleID(req.Context(), vars["scope"], vars["kind"], vars["ruleID"], query.Get("after"), query.Get("before"), req.Body, device, userAPI)
		}),
	).Methods(http.MethodPut)

	v3mux.Handle("/pushrules/{scope}/{kind}/{ruleID}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return DeletePushRuleByRuleID(req.Context(), vars["scope"], vars["kind"], vars["ruleID"], device, userAPI)
		}),
	).Methods(http.MethodDelete)

	v3mux.Handle("/pushrules/{scope}/{kind}/{ruleID}/{attr}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetPushRuleAttrByRuleID(req.Context(), vars["scope"], vars["kind"], vars["ruleID"], vars["attr"], device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushrules/{scope}/{kind}/{ruleID}/{attr}",
		httputil.MakeAuthAPI("push_rules", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return PutPushRuleAttrByRuleID(req.Context(), vars["scope"], vars["kind"], vars["ruleID"], vars["attr"], req.Body, device, userAPI)
		}),
	).Methods(http.MethodPut)

	// Element user settings

	v3mux.Handle("/profile/{userID}",
		httputil.MakeExternalAPI("profile", func(req *http.Request) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetProfile(req, userAPI, cfg, vars["userID"], asAPI, federation)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/profile/{userID}/avatar_url",
		httputil.MakeExternalAPI("profile_avatar_url", func(req *http.Request) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetAvatarURL(req, userAPI, cfg, vars["userID"], asAPI, federation)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/profile/{userID}/avatar_url",
		httputil.MakeAuthAPI("profile_avatar_url", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetAvatarURL(req, userAPI, device, vars["userID"], cfg, rsAPI)
		}),
	).Methods(http.MethodPut, http.MethodOptions)
	// Browsers use the OPTIONS HTTP method to check if the CORS policy allows
	// PUT requests, so we need to allow this method

	v3mux.Handle("/profile/{userID}/displayname",
		httputil.MakeExternalAPI("profile_displayname", func(req *http.Request) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetDisplayName(req, userAPI, cfg, vars["userID"], asAPI, federation)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/profile/{userID}/displayname",
		httputil.MakeAuthAPI("profile_displayname", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetDisplayName(req, userAPI, device, vars["userID"], cfg, rsAPI)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)
	// Browsers use the OPTIONS HTTP method to check if the CORS policy allows
	// PUT requests, so we need to allow this method

	threePIDClient := base.CreateClient(dendriteCfg, nil) // TDO: Move this somewhere else, e.g. pass in as parameter

	v3mux.Handle("/account/3pid",
		httputil.MakeAuthAPI("account_3pid", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetAssociated3PIDs(req, userAPI, device)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/account/3pid",
		httputil.MakeAuthAPI("account_3pid", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return CheckAndSave3PIDAssociation(req, userAPI, device, cfg, threePIDClient)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/account/3pid/delete",
		httputil.MakeAuthAPI("account_3pid", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return Forget3PID(req, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/{path:(?:account/3pid|register)}/email/requestToken",
		httputil.MakeExternalAPI("account_3pid_request_token", func(req *http.Request) xutil.JSONResponse {
			return RequestEmailToken(req, userAPI, cfg, threePIDClient)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/voip/turnServer",
		httputil.MakeAuthAPI("turn_server", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return RequestTurnServer(req, device, cfg)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/protocols",
		httputil.MakeAuthAPI("thirdparty_protocols", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return Protocols(req, asAPI, device, "")
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/protocol/{protocolID}",
		httputil.MakeAuthAPI("thirdparty_protocols", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return Protocols(req, asAPI, device, vars["protocolID"])
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/user/{protocolID}",
		httputil.MakeAuthAPI("thirdparty_user", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return User(req, asAPI, device, vars["protocolID"], req.URL.Query())
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/user",
		httputil.MakeAuthAPI("thirdparty_user", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return User(req, asAPI, device, "", req.URL.Query())
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/location/{protocolID}",
		httputil.MakeAuthAPI("thirdparty_location", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return Location(req, asAPI, device, vars["protocolID"], req.URL.Query())
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thirdparty/location",
		httputil.MakeAuthAPI("thirdparty_location", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return Location(req, asAPI, device, "", req.URL.Query())
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/initialSync",
		httputil.MakeExternalAPI("frames_initial_sync", func(req *http.Request) xutil.JSONResponse {
			// TDO: Allow people to peek into frames.
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.GuestAccessForbidden("Guest access not implemented"),
			}
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/user/{userID}/account_data/{type}",
		httputil.MakeAuthAPI("user_account_data", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SaveAccountData(req, userAPI, device, vars["userID"], "", vars["type"], syncProducer)
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/user/{userID}/frames/{frameID}/account_data/{type}",
		httputil.MakeAuthAPI("user_account_data", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SaveAccountData(req, userAPI, device, vars["userID"], vars["frameID"], vars["type"], syncProducer)
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/user/{userID}/account_data/{type}",
		httputil.MakeAuthAPI("user_account_data", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetAccountData(req, userAPI, device, vars["userID"], "", vars["type"])
		}),
	).Methods(http.MethodGet)

	v3mux.Handle("/user/{userID}/frames/{frameID}/account_data/{type}",
		httputil.MakeAuthAPI("user_account_data", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetAccountData(req, userAPI, device, vars["userID"], vars["frameID"], vars["type"])
		}),
	).Methods(http.MethodGet)

	v3mux.Handle("/admin/whois/{userID}",
		httputil.MakeAuthAPI("admin_whois", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetAdminWhois(req, userAPI, device, vars["userID"])
		}),
	).Methods(http.MethodGet)

	v3mux.Handle("/user/{userID}/openid/request_token",
		httputil.MakeAuthAPI("openid_request_token", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return CreateOpenIDToken(req, userAPI, device, vars["userID"], cfg)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/user_directory/search",
		httputil.MakeAuthAPI("userdirectory_search", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			postContent := struct {
				SearchString string `json:"search_term"`
				Limit        int    `json:"limit"`
			}{}

			if resErr := clientxutil.UnmarshalJSONRequest(req, &postContent); resErr != nil {
				return *resErr
			}
			return SearchUserDirectory(
				req.Context(),
				device,
				rsAPI,
				userDirectoryProvider,
				postContent.SearchString,
				postContent.Limit,
				federation,
				cfg.Coddy.ServerName,
			)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/read_markers",
		httputil.MakeAuthAPI("frames_read_markers", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SaveReadMarker(req, userAPI, rsAPI, syncProducer, device, vars["frameID"])
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/forget",
		httputil.MakeAuthAPI("frames_forget", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SendForget(req, device, vars["frameID"], rsAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/upgrade",
		httputil.MakeAuthAPI("frames_upgrade", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return UpgradeFrame(req, device, cfg, vars["frameID"], userAPI, rsAPI, asAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/devices",
		httputil.MakeAuthAPI("get_devices", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetDevicesByLocalpart(req, userAPI, device)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/devices/{deviceID}",
		httputil.MakeAuthAPI("get_device", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetDeviceByID(req, userAPI, device, vars["deviceID"])
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/devices/{deviceID}",
		httputil.MakeAuthAPI("device_data", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return UpdateDeviceByID(req, userAPI, device, vars["deviceID"])
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/devices/{deviceID}",
		httputil.MakeAuthAPI("delete_device", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return DeleteDeviceById(req, userInteractiveAuth, userAPI, device, vars["deviceID"])
		}),
	).Methods(http.MethodDelete, http.MethodOptions)

	v3mux.Handle("/delete_devices",
		httputil.MakeAuthAPI("delete_devices", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return DeleteDevices(req, userInteractiveAuth, userAPI, device)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/notifications",
		httputil.MakeAuthAPI("get_notifications", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetNotifications(req, device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushers",
		httputil.MakeAuthAPI("get_pushers", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return GetPushers(req, device, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/pushers/set",
		httputil.MakeAuthAPI("set_pushers", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return SetPusher(req, device, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	// Stub implementations for sytest
	v3mux.Handle("/events",
		httputil.MakeAuthAPI("events", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{Code: http.StatusOK, JSON: map[string]interface{}{
				"chunk": []interface{}{},
				"start": "",
				"end":   "",
			}}
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/initialSync",
		httputil.MakeAuthAPI("initial_sync", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return xutil.JSONResponse{Code: http.StatusOK, JSON: map[string]interface{}{
				"end": "",
			}}
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/user/{userId}/frames/{frameId}/tags",
		httputil.MakeAuthAPI("get_tags", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetTags(req, userAPI, device, vars["userId"], vars["frameId"], syncProducer)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/user/{userId}/frames/{frameId}/tags/{tag}",
		httputil.MakeAuthAPI("put_tag", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return PutTag(req, userAPI, device, vars["userId"], vars["frameId"], vars["tag"], syncProducer)
		}),
	).Methods(http.MethodPut, http.MethodOptions)

	v3mux.Handle("/user/{userId}/frames/{frameId}/tags/{tag}",
		httputil.MakeAuthAPI("delete_tag", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return DeleteTag(req, userAPI, device, vars["userId"], vars["frameId"], vars["tag"], syncProducer)
		}),
	).Methods(http.MethodDelete, http.MethodOptions)

	v3mux.Handle("/capabilities",
		httputil.MakeAuthAPI("capabilities", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			return GetCapabilities(rsAPI)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	// Key Backup Versions (Metadata)

	getBackupKeysVersion := httputil.MakeAuthAPI("get_backup_keys_version", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return KeyBackupVersion(req, userAPI, device, vars["version"])
	})

	getLatestBackupKeysVersion := httputil.MakeAuthAPI("get_latest_backup_keys_version", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return KeyBackupVersion(req, userAPI, device, "")
	})

	putBackupKeysVersion := httputil.MakeAuthAPI("put_backup_keys_version", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return ModifyKeyBackupVersionAuthData(req, userAPI, device, vars["version"])
	})

	deleteBackupKeysVersion := httputil.MakeAuthAPI("delete_backup_keys_version", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return DeleteKeyBackupVersion(req, userAPI, device, vars["version"])
	})

	postNewBackupKeysVersion := httputil.MakeAuthAPI("post_new_backup_keys_version", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return CreateKeyBackupVersion(req, userAPI, device)
	})

	v3mux.Handle("/frame_keys/version/{version}", getBackupKeysVersion).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/frame_keys/version", getLatestBackupKeysVersion).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/frame_keys/version/{version}", putBackupKeysVersion).Methods(http.MethodPut)
	v3mux.Handle("/frame_keys/version/{version}", deleteBackupKeysVersion).Methods(http.MethodDelete)
	v3mux.Handle("/frame_keys/version", postNewBackupKeysVersion).Methods(http.MethodPost, http.MethodOptions)

	unstableMux.Handle("/frame_keys/version/{version}", getBackupKeysVersion).Methods(http.MethodGet, http.MethodOptions)
	unstableMux.Handle("/frame_keys/version", getLatestBackupKeysVersion).Methods(http.MethodGet, http.MethodOptions)
	unstableMux.Handle("/frame_keys/version/{version}", putBackupKeysVersion).Methods(http.MethodPut)
	unstableMux.Handle("/frame_keys/version/{version}", deleteBackupKeysVersion).Methods(http.MethodDelete)
	unstableMux.Handle("/frame_keys/version", postNewBackupKeysVersion).Methods(http.MethodPost, http.MethodOptions)

	// Inserting E2E Backup Keys

	// Bulk frame and session
	putBackupKeys := httputil.MakeAuthAPI("put_backup_keys", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		version := req.URL.Query().Get("version")
		if version == "" {
			return xutil.JSONResponse{
				Code: 400,
				JSON: spec.InvalidParam("version must be specified"),
			}
		}
		var reqBody keyBackupSessionRequest
		resErr := clientxutil.UnmarshalJSONRequest(req, &reqBody)
		if resErr != nil {
			return *resErr
		}
		return UploadBackupKeys(req, userAPI, device, version, &reqBody)
	})

	// Single frame bulk session
	putBackupKeysFrame := httputil.MakeAuthAPI("put_backup_keys_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		version := req.URL.Query().Get("version")
		if version == "" {
			return xutil.JSONResponse{
				Code: 400,
				JSON: spec.InvalidParam("version must be specified"),
			}
		}
		frameID := vars["frameID"]
		var reqBody keyBackupSessionRequest
		reqBody.Frames = make(map[string]struct {
			Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
		})
		reqBody.Frames[frameID] = struct {
			Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
		}{
			Sessions: map[string]userapi.KeyBackupSession{},
		}
		body := reqBody.Frames[frameID]
		resErr := clientxutil.UnmarshalJSONRequest(req, &body)
		if resErr != nil {
			return *resErr
		}
		reqBody.Frames[frameID] = body
		return UploadBackupKeys(req, userAPI, device, version, &reqBody)
	})

	// Single frame, single session
	putBackupKeysFrameSession := httputil.MakeAuthAPI("put_backup_keys_frame_session", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		version := req.URL.Query().Get("version")
		if version == "" {
			return xutil.JSONResponse{
				Code: 400,
				JSON: spec.InvalidParam("version must be specified"),
			}
		}
		var reqBody userapi.KeyBackupSession
		resErr := clientxutil.UnmarshalJSONRequest(req, &reqBody)
		if resErr != nil {
			return *resErr
		}
		frameID := vars["frameID"]
		sessionID := vars["sessionID"]
		var keyReq keyBackupSessionRequest
		keyReq.Frames = make(map[string]struct {
			Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
		})
		keyReq.Frames[frameID] = struct {
			Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
		}{
			Sessions: make(map[string]userapi.KeyBackupSession),
		}
		keyReq.Frames[frameID].Sessions[sessionID] = reqBody
		return UploadBackupKeys(req, userAPI, device, version, &keyReq)
	})

	v3mux.Handle("/frame_keys/keys", putBackupKeys).Methods(http.MethodPut)
	v3mux.Handle("/frame_keys/keys/{frameID}", putBackupKeysFrame).Methods(http.MethodPut)
	v3mux.Handle("/frame_keys/keys/{frameID}/{sessionID}", putBackupKeysFrameSession).Methods(http.MethodPut)

	unstableMux.Handle("/frame_keys/keys", putBackupKeys).Methods(http.MethodPut)
	unstableMux.Handle("/frame_keys/keys/{frameID}", putBackupKeysFrame).Methods(http.MethodPut)
	unstableMux.Handle("/frame_keys/keys/{frameID}/{sessionID}", putBackupKeysFrameSession).Methods(http.MethodPut)

	// Querying E2E Backup Keys

	getBackupKeys := httputil.MakeAuthAPI("get_backup_keys", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return GetBackupKeys(req, userAPI, device, req.URL.Query().Get("version"), "", "")
	})

	getBackupKeysFrame := httputil.MakeAuthAPI("get_backup_keys_frame", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return GetBackupKeys(req, userAPI, device, req.URL.Query().Get("version"), vars["frameID"], "")
	})

	getBackupKeysFrameSession := httputil.MakeAuthAPI("get_backup_keys_frame_session", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return GetBackupKeys(req, userAPI, device, req.URL.Query().Get("version"), vars["frameID"], vars["sessionID"])
	})

	v3mux.Handle("/frame_keys/keys", getBackupKeys).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/frame_keys/keys/{frameID}", getBackupKeysFrame).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/frame_keys/keys/{frameID}/{sessionID}", getBackupKeysFrameSession).Methods(http.MethodGet, http.MethodOptions)

	unstableMux.Handle("/frame_keys/keys", getBackupKeys).Methods(http.MethodGet, http.MethodOptions)
	unstableMux.Handle("/frame_keys/keys/{frameID}", getBackupKeysFrame).Methods(http.MethodGet, http.MethodOptions)
	unstableMux.Handle("/frame_keys/keys/{frameID}/{sessionID}", getBackupKeysFrameSession).Methods(http.MethodGet, http.MethodOptions)

	// Deleting E2E Backup Keys

	// Cross-signing device keys

	postDeviceSigningKeys := httputil.MakeAuthAPI("post_device_signing_keys", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return UploadCrossSigningDeviceKeys(req, userInteractiveAuth, userAPI, device, userAPI, cfg)
	})

	postDeviceSigningSignatures := httputil.MakeAuthAPI("post_device_signing_signatures", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return UploadCrossSigningDeviceSignatures(req, userAPI, device)
	}, httputil.WithAllowGuests())

	v3mux.Handle("/keys/device_signing/upload", postDeviceSigningKeys).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/keys/signatures/upload", postDeviceSigningSignatures).Methods(http.MethodPost, http.MethodOptions)

	unstableMux.Handle("/keys/device_signing/upload", postDeviceSigningKeys).Methods(http.MethodPost, http.MethodOptions)
	unstableMux.Handle("/keys/signatures/upload", postDeviceSigningSignatures).Methods(http.MethodPost, http.MethodOptions)

	// Supplying a device ID is deprecated.
	v3mux.Handle("/keys/upload/{deviceID}",
		httputil.MakeAuthAPI("keys_upload", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return UploadKeys(req, userAPI, device)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/keys/upload",
		httputil.MakeAuthAPI("keys_upload", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return UploadKeys(req, userAPI, device)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/keys/query",
		httputil.MakeAuthAPI("keys_query", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return QueryKeys(req, userAPI, device)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/keys/claim",
		httputil.MakeAuthAPI("keys_claim", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			return ClaimKeys(req, userAPI)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/frames/{frameId}/receipt/{receiptType}/{eventId}",
		httputil.MakeAuthAPI(spec.Join, userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, device); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}

			return SetReceipt(req, userAPI, syncProducer, device, vars["frameId"], vars["receiptType"], vars["eventId"])
		}),
	).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/presence/{userId}/status",
		httputil.MakeAuthAPI("set_presence", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return SetPresence(req, cfg, device, syncProducer, vars["userId"])
		}),
	).Methods(http.MethodPut, http.MethodOptions)
	v3mux.Handle("/presence/{userId}/status",
		httputil.MakeAuthAPI("get_presence", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetPresence(req, device, natsClient, cfg.Coddy.JetStream.Prefixed(jetstream.RequestPresence), vars["userId"])
		}),
	).Methods(http.MethodGet, http.MethodOptions)
}
