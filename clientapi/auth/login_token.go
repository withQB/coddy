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

package auth

import (
	"context"
	"net/http"

	"github.com/withqb/coddy/clientapi/auth/authtypes"
	"github.com/withqb/coddy/clientapi/httputil"
	"github.com/withqb/coddy/setup/config"
	uapi "github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// LoginTypeToken describes how to authenticate with a login token.
type LoginTypeToken struct {
	UserAPI uapi.LoginTokenInternalAPI
	Config  *config.ClientAPI
}

// Name implements Type.
func (t *LoginTypeToken) Name() string {
	return authtypes.LoginTypeToken
}

// LoginFromJSON implements Type. The cleanup function deletes the token from
// the database on success.
func (t *LoginTypeToken) LoginFromJSON(ctx context.Context, reqBytes []byte) (*Login, LoginCleanupFunc, *xutil.JSONResponse) {
	var r loginTokenRequest
	if err := httputil.UnmarshalJSON(reqBytes, &r); err != nil {
		return nil, nil, err
	}

	var res uapi.QueryLoginTokenResponse
	if err := t.UserAPI.QueryLoginToken(ctx, &uapi.QueryLoginTokenRequest{Token: r.Token}, &res); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("UserAPI.QueryLoginToken failed")
		return nil, nil, &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if res.Data == nil {
		return nil, nil, &xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("invalid login token"),
		}
	}

	r.Login.Identifier.Type = "m.id.user"
	r.Login.Identifier.User = res.Data.UserID

	cleanup := func(ctx context.Context, authRes *xutil.JSONResponse) {
		if authRes == nil {
			xutil.GetLogger(ctx).Error("No JSONResponse provided to LoginTokenType cleanup function")
			return
		}
		if authRes.Code == http.StatusOK {
			var res uapi.PerformLoginTokenDeletionResponse
			if err := t.UserAPI.PerformLoginTokenDeletion(ctx, &uapi.PerformLoginTokenDeletionRequest{Token: r.Token}, &res); err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("UserAPI.PerformLoginTokenDeletion failed")
			}
		}
	}
	return &r.Login, cleanup, nil
}

// loginTokenRequest struct to hold the possible parameters from an HTTP request.
type loginTokenRequest struct {
	Login
	Token string `json:"token"`
}
