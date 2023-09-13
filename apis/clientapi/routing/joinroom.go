package routing

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/withqb/coddy/apis/clientapi/httputil"
	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/internal/eventutil"
	dataframeAPI "github.com/withqb/coddy/servers/dataframe/api"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

func JoinFrameByIDOrAlias(
	req *http.Request,
	device *api.Device,
	rsAPI dataframeAPI.ClientDataframeAPI,
	profileAPI api.ClientUserAPI,
	frameIDOrAlias string,
) xutil.JSONResponse {
	// Prepare to ask the dataframe to perform the frame join.
	joinReq := dataframeAPI.PerformJoinRequest{
		FrameIDOrAlias: frameIDOrAlias,
		UserID:        device.UserID,
		IsGuest:       device.AccountType == api.AccountTypeGuest,
		Content:       map[string]interface{}{},
	}

	// Check to see if any ?server_name= query parameters were
	// given in the request.
	if serverNames, ok := req.URL.Query()["server_name"]; ok {
		for _, serverName := range serverNames {
			joinReq.ServerNames = append(
				joinReq.ServerNames,
				spec.ServerName(serverName),
			)
		}
	}

	// If content was provided in the request then include that
	// in the request. It'll get used as a part of the membership
	// event content.
	_ = httputil.UnmarshalJSONRequest(req, &joinReq.Content)

	// Work out our localpart for the client profile request.

	// Request our profile content to populate the request content with.
	profile, err := profileAPI.QueryProfile(req.Context(), device.UserID)

	switch err {
	case nil:
		joinReq.Content["displayname"] = profile.DisplayName
		joinReq.Content["avatar_url"] = profile.AvatarURL
	case appserviceAPI.ErrProfileNotExists:
		xutil.GetLogger(req.Context()).Error("Unable to query user profile, no profile found.")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Unable to query user profile, no profile found."),
		}
	default:
	}

	// Ask the dataframe to perform the join.
	done := make(chan xutil.JSONResponse, 1)
	go func() {
		defer close(done)
		frameID, _, err := rsAPI.PerformJoin(req.Context(), &joinReq)
		var response xutil.JSONResponse

		switch e := err.(type) {
		case nil: // success case
			response = xutil.JSONResponse{
				Code: http.StatusOK,
				// TDO: Put the response struct somewhere internal.
				JSON: struct {
					FrameID string `json:"frame_id"`
				}{frameID},
			}
		case dataframeAPI.ErrInvalidID:
			response = xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown(e.Error()),
			}
		case dataframeAPI.ErrNotAllowed:
			jsonErr := spec.Forbidden(e.Error())
			if device.AccountType == api.AccountTypeGuest {
				jsonErr = spec.GuestAccessForbidden(e.Error())
			}
			response = xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: jsonErr,
			}
		case *xcore.HTTPError: // this ensures we proxy responses over federation to the client
			response = xutil.JSONResponse{
				Code: e.Code,
				JSON: json.RawMessage(e.Message),
			}
		case eventutil.ErrFrameNoExists:
			response = xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(e.Error()),
			}
		default:
			response = xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		done <- response
	}()

	// Wait either for the join to finish, or for us to hit a reasonable
	// timeout, at which point we'll just return a 200 to placate clients.
	timer := time.NewTimer(time.Second * 20)
	select {
	case <-timer.C:
		return xutil.JSONResponse{
			Code: http.StatusAccepted,
			JSON: spec.Unknown("The frame join will continue in the background."),
		}
	case result := <-done:
		// Stop and drain the timer
		if !timer.Stop() {
			<-timer.C
		}
		return result
	}
}
