package routing

import (
	"net/http"
	"net/url"

	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/userapi/api"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/xtools/spec"
)

// Protocols implements
//
//	GET /_coddy/client/v3/thirdparty/protocols/{protocol}
//	GET /_coddy/client/v3/thirdparty/protocols
func Protocols(req *http.Request, asAPI appserviceAPI.AppServiceInternalAPI, device *api.Device, protocol string) xutil.JSONResponse {
	resp := &appserviceAPI.ProtocolResponse{}

	if err := asAPI.Protocols(req.Context(), &appserviceAPI.ProtocolRequest{Protocol: protocol}, resp); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !resp.Exists {
		if protocol != "" {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("The protocol is unknown."),
			}
		}
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	if protocol != "" {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: resp.Protocols[protocol],
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp.Protocols,
	}
}

// User implements
//
//	GET /_coddy/client/v3/thirdparty/user
//	GET /_coddy/client/v3/thirdparty/user/{protocol}
func User(req *http.Request, asAPI appserviceAPI.AppServiceInternalAPI, device *api.Device, protocol string, params url.Values) xutil.JSONResponse {
	resp := &appserviceAPI.UserResponse{}

	params.Del("access_token")
	if err := asAPI.User(req.Context(), &appserviceAPI.UserRequest{
		Protocol: protocol,
		Params:   params.Encode(),
	}, resp); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !resp.Exists {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The Matrix User ID was not found"),
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp.Users,
	}
}

// Location implements
//
//	GET /_coddy/client/v3/thirdparty/location
//	GET /_coddy/client/v3/thirdparty/location/{protocol}
func Location(req *http.Request, asAPI appserviceAPI.AppServiceInternalAPI, device *api.Device, protocol string, params url.Values) xutil.JSONResponse {
	resp := &appserviceAPI.LocationResponse{}

	params.Del("access_token")
	if err := asAPI.Locations(req.Context(), &appserviceAPI.LocationRequest{
		Protocol: protocol,
		Params:   params.Encode(),
	}, resp); err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !resp.Exists {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("No portal frames were found."),
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp.Locations,
	}
}
