package httputil

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"strings"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/services/clientapi/auth"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"
)

// BasicAuth is used for authorization on /metrics handlers
type BasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type AuthAPIOpts struct {
	GuestAccessAllowed bool
}

// AuthAPIOption is an option to MakeAuthAPI to add additional checks (e.g. guest access) to verify
// the user is allowed to do specific things.
type AuthAPIOption func(opts *AuthAPIOpts)

// WithAllowGuests checks that guest users have access to this endpoint
func WithAllowGuests() AuthAPIOption {
	return func(opts *AuthAPIOpts) {
		opts.GuestAccessAllowed = true
	}
}

// MakeAuthAPI turns a xutil.JSONRequestHandler function into an http.Handler which authenticates the request.
func MakeAuthAPI(
	metricsName string, userAPI userapi.QueryAcccessTokenAPI,
	f func(*http.Request, *userapi.Device) xutil.JSONResponse,
	checks ...AuthAPIOption,
) http.Handler {
	h := func(req *http.Request) xutil.JSONResponse {
		logger := xutil.GetLogger(req.Context())
		device, err := auth.VerifyUserFromRequest(req, userAPI)
		if err != nil {
			logger.Debugf("VerifyUserFromRequest %s -> HTTP %d", req.RemoteAddr, err.Code)
			return *err
		}
		// add the user ID to the logger
		logger = logger.WithField("user_id", device.UserID)
		req = req.WithContext(xutil.ContextWithLogger(req.Context(), logger))
		// add the user to Sentry, if enabled
		hub := sentry.GetHubFromContext(req.Context())
		if hub != nil {
			hub.Scope().SetUser(sentry.User{
				Username: device.UserID,
			})
			hub.Scope().SetTag("user_id", device.UserID)
			hub.Scope().SetTag("device_id", device.ID)
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

		// apply additional checks, if any
		opts := AuthAPIOpts{}
		for _, opt := range checks {
			opt(&opts)
		}

		if !opts.GuestAccessAllowed && device.AccountType == userapi.AccountTypeGuest {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.GuestAccessForbidden("Guest access not allowed"),
			}
		}

		jsonRes := f(req, device)
		// do not log 4xx as errors as they are client fails, not server fails
		if hub != nil && jsonRes.Code >= 500 {
			hub.Scope().SetExtra("response", jsonRes)
			hub.CaptureException(fmt.Errorf("%s returned HTTP %d", req.URL.Path, jsonRes.Code))
		}
		return jsonRes
	}
	return MakeExternalAPI(metricsName, h)
}

// MakeAdminAPI is a wrapper around MakeAuthAPI which enforces that the request can only be
// completed by a user that is a server administrator.
func MakeAdminAPI(
	metricsName string, userAPI userapi.QueryAcccessTokenAPI,
	f func(*http.Request, *userapi.Device) xutil.JSONResponse,
) http.Handler {
	return MakeAuthAPI(metricsName, userAPI, func(req *http.Request, device *userapi.Device) xutil.JSONResponse {
		if device.AccountType != userapi.AccountTypeAdmin {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("This API can only be used by admin users."),
			}
		}
		return f(req, device)
	})
}

// MakeExternalAPI turns a xutil.JSONRequestHandler function into an http.Handler.
// This is used for APIs that are called from the internet.
func MakeExternalAPI(metricsName string, f func(*http.Request) xutil.JSONResponse) http.Handler {
	// TDO: We shouldn't be directly reading env vars here, inject it in instead.
	// Refactor this when we split out config structs.
	verbose := false
	if os.Getenv("DENDRITE_TRACE_HTTP") == "1" {
		verbose = true
	}
	h := xutil.MakeJSONAPI(xutil.NewJSONRequestHandler(f))
	withSpan := func(w http.ResponseWriter, req *http.Request) {
		nextWriter := w
		if verbose {
			logger := logrus.NewEntry(logrus.StandardLogger())
			// Log outgoing response
			rec := httptest.NewRecorder()
			nextWriter = rec
			defer func() {
				resp := rec.Result()
				dump, err := httputil.DumpResponse(resp, true)
				if err != nil {
					logger.Debugf("failed to dump outgoing response: %s", err)
				} else {
					strSlice := strings.Split(string(dump), "\n")
					for _, s := range strSlice {
						logger.Debug(s)
					}
				}
				// copy the response to the client
				for hdr, vals := range resp.Header {
					for _, val := range vals {
						w.Header().Add(hdr, val)
					}
				}
				w.WriteHeader(resp.StatusCode)
				// discard errors as this is for debugging
				_, _ = io.Copy(w, resp.Body)
				_ = resp.Body.Close()
			}()

			// Log incoming request
			dump, err := httputil.DumpRequest(req, true)
			if err != nil {
				logger.Debugf("failed to dump incoming request: %s", err)
			} else {
				strSlice := strings.Split(string(dump), "\n")
				for _, s := range strSlice {
					logger.Debug(s)
				}
			}
		}

		trace, ctx := internal.StartTask(req.Context(), metricsName)
		defer trace.EndTask()
		req = req.WithContext(ctx)
		h.ServeHTTP(nextWriter, req)

	}

	return http.HandlerFunc(withSpan)
}

// MakeHTMLAPI adds Span metrics to the HTML Handler function
// This is used to serve HTML alongside JSON error messages
func MakeHTMLAPI(metricsName string, enableMetrics bool, f func(http.ResponseWriter, *http.Request)) http.Handler {
	withSpan := func(w http.ResponseWriter, req *http.Request) {
		trace, ctx := internal.StartTask(req.Context(), metricsName)
		defer trace.EndTask()
		req = req.WithContext(ctx)
		f(w, req)
	}

	if !enableMetrics {
		return http.HandlerFunc(withSpan)
	}

	return promhttp.InstrumentHandlerCounter(
		promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name:      metricsName,
				Help:      "Total number of http requests for HTML resources",
				Namespace: "dendrite",
			},
			[]string{"code"},
		),
		http.HandlerFunc(withSpan),
	)
}

// WrapHandlerInBasicAuth adds basic auth to a handler. Only used for /metrics
func WrapHandlerInBasicAuth(h http.Handler, b BasicAuth) http.HandlerFunc {
	if b.Username == "" || b.Password == "" {
		logrus.Warn("Metrics are exposed without protection. Make sure you set up protection at proxy level.")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Serve without authorization if either Username or Password is unset
		if b.Username == "" || b.Password == "" {
			h.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()

		if !ok || user != b.Username || pass != b.Password {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	}
}

// WrapHandlerInCORS adds CORS headers to all responses, including all error
// responses.
// Handles OPTIONS requests directly.
func WrapHandlerInCORS(h http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")

		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			// Its easiest just to always return a 200 OK for everything. Whether
			// this is technically correct or not is a question, but in the end this
			// is what a lot of other people do (including synapse) and the clients
			// are perfectly happy with it.
			w.WriteHeader(http.StatusOK)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}
