// Copyright 2017 New Vector Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/withqb/coddy/internal/eventutil"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetProfile implements GET /_coddy/federation/v1/query/profile
func GetProfile(
	httpReq *http.Request,
	userAPI userapi.FederationUserAPI,
	cfg *config.FederationAPI,
) xutil.JSONResponse {
	userID, field := httpReq.FormValue("user_id"), httpReq.FormValue("field")

	// httpReq.FormValue will return an empty string if value is not found
	if userID == "" {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("The request body did not contain required argument 'user_id'."),
		}
	}

	_, domain, err := cfg.Coddy.SplitLocalID('@', userID)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("xtools.SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("Domain %q does not match this server", domain)),
		}
	}

	profile, err := userAPI.QueryProfile(httpReq.Context(), userID)
	if err != nil {
		if errors.Is(err, appserviceAPI.ErrProfileNotExists) {
			return xutil.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("The user does not exist or does not have a profile."),
			}
		}
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("userAPI.QueryProfile failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var res interface{}
	code := http.StatusOK

	if field != "" {
		switch field {
		case "displayname":
			res = eventutil.UserProfile{
				DisplayName: profile.DisplayName,
			}
		case "avatar_url":
			res = eventutil.UserProfile{
				AvatarURL: profile.AvatarURL,
			}
		default:
			code = http.StatusBadRequest
			res = spec.InvalidParam("The request body did not contain an allowed value of argument 'field'. Allowed values are either: 'avatar_url', 'displayname'.")
		}
	} else {
		res = eventutil.UserProfile{
			AvatarURL:   profile.AvatarURL,
			DisplayName: profile.DisplayName,
		}
	}

	return xutil.JSONResponse{
		Code: code,
		JSON: res,
	}
}
