package routing

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/withqb/coddy/apis/mediaapi/storage"
	"github.com/withqb/coddy/apis/mediaapi/types"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/httputil"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// configResponse is the response to GET /_coddy/media/r0/config
type configResponse struct {
	UploadSize *config.FileSizeBytes `json:"m.upload.size,omitempty"`
}

// Setup registers the media API HTTP handlers
//
// Due to Setup being used to call many other functions, a gocyclo nolint is
// applied:
// nolint: gocyclo
func Setup(
	publicAPIMux *mux.Router,
	cfg *config.Dendrite,
	db storage.Database,
	userAPI userapi.MediaUserAPI,
	client *fclient.Client,
) {
	rateLimits := httputil.NewRateLimits(&cfg.ClientAPI.RateLimiting)

	v3mux := publicAPIMux.PathPrefix("/{apiversion:(?:r0|v1|v3)}/").Subrouter()

	activeThumbnailGeneration := &types.ActiveThumbnailGeneration{
		PathToResult: map[string]*types.ThumbnailGenerationResult{},
	}

	uploadHandler := httputil.MakeAuthAPI(
		"upload", userAPI,
		func(req *http.Request, dev *userapi.Device) xutil.JSONResponse {
			if r := rateLimits.Limit(req, dev); r != nil {
				return *r
			}
			return Upload(req, &cfg.MediaAPI, dev, db, activeThumbnailGeneration)
		},
	)

	configHandler := httputil.MakeAuthAPI("config", userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		if r := rateLimits.Limit(req, device); r != nil {
			return *r
		}
		respondSize := &cfg.MediaAPI.MaxFileSizeBytes
		if cfg.MediaAPI.MaxFileSizeBytes == 0 {
			respondSize = nil
		}
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: configResponse{UploadSize: respondSize},
		}
	})

	v3mux.Handle("/upload", uploadHandler).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/config", configHandler).Methods(http.MethodGet, http.MethodOptions)

	activeRemoteRequests := &types.ActiveRemoteRequests{
		MXCToResult: map[string]*types.RemoteRequestResult{},
	}

	downloadHandler := makeDownloadAPI("download", &cfg.MediaAPI, rateLimits, db, client, activeRemoteRequests, activeThumbnailGeneration)
	v3mux.Handle("/download/{serverName}/{mediaId}", downloadHandler).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/download/{serverName}/{mediaId}/{downloadName}", downloadHandler).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle("/thumbnail/{serverName}/{mediaId}",
		makeDownloadAPI("thumbnail", &cfg.MediaAPI, rateLimits, db, client, activeRemoteRequests, activeThumbnailGeneration),
	).Methods(http.MethodGet, http.MethodOptions)
}

func makeDownloadAPI(
	name string,
	cfg *config.MediaAPI,
	rateLimits *httputil.RateLimits,
	db storage.Database,
	client *fclient.Client,
	activeRemoteRequests *types.ActiveRemoteRequests,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
) http.HandlerFunc {
	var counterVec *prometheus.CounterVec
	if cfg.Matrix.Metrics.Enabled {
		counterVec = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: name,
				Help: "Total number of media_api requests for either thumbnails or full downloads",
			},
			[]string{"code"},
		)
	}
	httpHandler := func(w http.ResponseWriter, req *http.Request) {
		req = xutil.RequestWithLogging(req)

		// Set internal headers returned regardless of the outcome of the request
		xutil.SetCORSHeaders(w)
		// Content-Type will be overridden in case of returning file data, else we respond with JSON-formatted errors
		w.Header().Set("Content-Type", "application/json")

		// Ratelimit requests
		// NOTSPEC: The spec says everything at /media/ should be rate limited, but this causes issues with thumbnails (#2243)
		if name != "thumbnail" {
			if r := rateLimits.Limit(req, nil); r != nil {
				if err := json.NewEncoder(w).Encode(r); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		}

		vars, _ := httputil.URLDecodeMapValues(mux.Vars(req))
		serverName := spec.ServerName(vars["serverName"])

		// For the purposes of loop avoidance, we will return a 404 if allow_remote is set to
		// false in the query string and the target server name isn't our own.
		// https://github.com/withqb/coddy-doc/pull/1265
		if allowRemote := req.URL.Query().Get("allow_remote"); strings.ToLower(allowRemote) == "false" {
			if serverName != cfg.Matrix.ServerName {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		// Cache media for at least one day.
		w.Header().Set("Cache-Control", "public,max-age=86400,s-maxage=86400")

		Download(
			w,
			req,
			serverName,
			types.MediaID(vars["mediaId"]),
			cfg,
			db,
			client,
			activeRemoteRequests,
			activeThumbnailGeneration,
			name == "thumbnail",
			vars["downloadName"],
		)
	}

	var handlerFunc http.HandlerFunc
	if counterVec != nil {
		handlerFunc = promhttp.InstrumentHandlerCounter(counterVec, http.HandlerFunc(httpHandler))
	} else {
		handlerFunc = http.HandlerFunc(httpHandler)
	}
	return handlerFunc
}
