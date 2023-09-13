package routing

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/fulltext"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/sync"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
)

// Setup configures the given mux with sync-server listeners
//
// Due to Setup being used to call many other functions, a gocyclo nolint is
// applied:
// nolint: gocyclo
func Setup(
	csMux *mux.Router, srp *sync.RequestPool, syncDB storage.Database,
	userAPI userapi.SyncUserAPI,
	rsAPI api.SyncDataframeAPI,
	cfg *config.SyncAPI,
	lazyLoadCache caching.LazyLoadCache,
	fts fulltext.Indexer,
	rateLimits *httputil.RateLimits,
) {
	v1unstablemux := csMux.PathPrefix("/{apiversion:(?:v1|unstable)}/").Subrouter()
	v3mux := csMux.PathPrefix("/{apiversion:(?:r0|v3)}/").Subrouter()

	// TDO: Add AS support for all handlers below.
	v3mux.Handle("/sync", httputil.MakeAuthAPI("sync", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return srp.OnIncomingSyncRequest(req, device)
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/messages", httputil.MakeAuthAPI("frame_messages", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		// not specced, but ensure we're rate limiting requests to this endpoint
		if r := rateLimits.Limit(req, device); r != nil {
			return *r
		}
		vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		return OnIncomingMessagesRequest(req, syncDB, vars["frameID"], device, rsAPI, cfg, srp, lazyLoadCache)
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/event/{eventID}",
		httputil.MakeAuthAPI("frames_get_event", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetEvent(req, device, vars["frameID"], vars["eventID"], cfg, syncDB, rsAPI)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/user/{userId}/filter",
		httputil.MakeAuthAPI("put_filter", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return PutFilter(req, device, syncDB, vars["userId"])
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/user/{userId}/filter/{filterId}",
		httputil.MakeAuthAPI("get_filter", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			return GetFilter(req, device, syncDB, vars["userId"], vars["filterId"])
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/keys/changes", httputil.MakeAuthAPI("keys_changes", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		return srp.OnIncomingKeyChangeRequest(req, device)
	}, httputil.WithAllowGuests())).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameId}/context/{eventId}",
		httputil.MakeAuthAPI("context", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}

			return Context(
				req, device,
				rsAPI, syncDB,
				vars["frameId"], vars["eventId"],
				lazyLoadCache,
			)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v1unstablemux.Handle("/frames/{frameId}/relations/{eventId}",
		httputil.MakeAuthAPI("relations", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}

			return Relations(
				req, device, syncDB, rsAPI,
				vars["frameId"], vars["eventId"], "", "",
			)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v1unstablemux.Handle("/frames/{frameId}/relations/{eventId}/{relType}",
		httputil.MakeAuthAPI("relation_type", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}

			return Relations(
				req, device, syncDB, rsAPI,
				vars["frameId"], vars["eventId"], vars["relType"], "",
			)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v1unstablemux.Handle("/frames/{frameId}/relations/{eventId}/{relType}/{eventType}",
		httputil.MakeAuthAPI("relation_type_event", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}

			return Relations(
				req, device, syncDB, rsAPI,
				vars["frameId"], vars["eventId"], vars["relType"], vars["eventType"],
			)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/search",
		httputil.MakeAuthAPI("search", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			if !cfg.Fulltext.Enabled {
				return xutil.JSONResponse{
					Code: http.StatusNotImplemented,
					JSON: spec.Unknown("Search has been disabled by the server administrator."),
				}
			}
			var nextBatch *string
			if err := req.ParseForm(); err != nil {
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			if req.Form.Has("next_batch") {
				nb := req.FormValue("next_batch")
				nextBatch = &nb
			}
			return Search(req, device, syncDB, fts, nextBatch, rsAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/members",
		httputil.MakeAuthAPI("frames_members", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			var membership, notMembership *string
			if req.URL.Query().Has("membership") {
				m := req.URL.Query().Get("membership")
				membership = &m
			}
			if req.URL.Query().Has("not_membership") {
				m := req.URL.Query().Get("not_membership")
				notMembership = &m
			}

			at := req.URL.Query().Get("at")
			return GetMemberships(req, device, vars["frameID"], syncDB, rsAPI, false, membership, notMembership, at)
		}, httputil.WithAllowGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/frames/{frameID}/joined_members",
		httputil.MakeAuthAPI("frames_members", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
			vars, err := httputil.URLDecodeMapValues(mux.Vars(req))
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			at := req.URL.Query().Get("at")
			membership := spec.Join
			return GetMemberships(req, device, vars["frameID"], syncDB, rsAPI, true, &membership, nil, at)
		}),
	).Methods(http.MethodGet, http.MethodOptions)
}
