// Copyright 2021 The Matrix.org Foundation C.I.C.
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
	"net/http"
	"time"

	userapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type openIDUserInfoResponse struct {
	Sub string `json:"sub"`
}

// GetOpenIDUserInfo implements GET /_matrix/federation/v1/openid/userinfo
func GetOpenIDUserInfo(
	httpReq *http.Request,
	userAPI userapi.FederationUserAPI,
) xutil.JSONResponse {
	token := httpReq.URL.Query().Get("access_token")
	if len(token) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.MissingParam("access_token is missing"),
		}
	}

	req := userapi.QueryOpenIDTokenRequest{
		Token: token,
	}

	var openIDTokenAttrResponse userapi.QueryOpenIDTokenResponse
	err := userAPI.QueryOpenIDToken(httpReq.Context(), &req, &openIDTokenAttrResponse)
	if err != nil {
		xutil.GetLogger(httpReq.Context()).WithError(err).Error("userAPI.QueryOpenIDToken failed")
	}

	var res interface{} = openIDUserInfoResponse{Sub: openIDTokenAttrResponse.Sub}
	code := http.StatusOK
	nowMS := time.Now().UnixNano() / int64(time.Millisecond)
	if openIDTokenAttrResponse.Sub == "" || nowMS > openIDTokenAttrResponse.ExpiresAtMS {
		code = http.StatusUnauthorized
		res = spec.UnknownToken("Access Token unknown or expired")
	}

	return xutil.JSONResponse{
		Code: code,
		JSON: res,
	}
}
