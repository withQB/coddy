// Copyright 2021 The Coddy.org Foundation C.I.C.
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

	"github.com/withqb/coddy/services/clientapi/auth"
	"github.com/withqb/coddy/services/clientapi/auth/authtypes"
	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type crossSigningRequest struct {
	api.PerformUploadDeviceKeysRequest
	Auth newPasswordAuth `json:"auth"`
}

func UploadCrossSigningDeviceKeys(
	req *http.Request, userInteractiveAuth *auth.UserInteractive,
	keyserverAPI api.ClientKeyAPI, device *api.Device,
	accountAPI api.ClientUserAPI, cfg *config.ClientAPI,
) xutil.JSONResponse {
	uploadReq := &crossSigningRequest{}
	uploadRes := &api.PerformUploadDeviceKeysResponse{}

	resErr := httputil.UnmarshalJSONRequest(req, &uploadReq)
	if resErr != nil {
		return *resErr
	}
	sessionID := uploadReq.Auth.Session
	if sessionID == "" {
		sessionID = xutil.RandomString(sessionIDLength)
	}
	if uploadReq.Auth.Type != authtypes.LoginTypePassword {
		return xutil.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: newUserInteractiveResponse(
				sessionID,
				[]authtypes.Flow{
					{
						Stages: []authtypes.LoginType{authtypes.LoginTypePassword},
					},
				},
				nil,
			),
		}
	}
	typePassword := auth.LoginTypePassword{
		GetAccountByPassword: accountAPI.QueryAccountByPassword,
		Config:               cfg,
	}
	if _, authErr := typePassword.Login(req.Context(), &uploadReq.Auth.PasswordRequest); authErr != nil {
		return *authErr
	}
	sessions.addCompletedSessionStage(sessionID, authtypes.LoginTypePassword)

	uploadReq.UserID = device.UserID
	keyserverAPI.PerformUploadDeviceKeys(req.Context(), &uploadReq.PerformUploadDeviceKeysRequest, uploadRes)

	if err := uploadRes.Error; err != nil {
		switch {
		case err.IsInvalidSignature:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidSignature(err.Error()),
			}
		case err.IsMissingParam:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MissingParam(err.Error()),
			}
		case err.IsInvalidParam:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		default:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown(err.Error()),
			}
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

func UploadCrossSigningDeviceSignatures(req *http.Request, keyserverAPI api.ClientKeyAPI, device *api.Device) xutil.JSONResponse {
	uploadReq := &api.PerformUploadDeviceSignaturesRequest{}
	uploadRes := &api.PerformUploadDeviceSignaturesResponse{}

	if err := httputil.UnmarshalJSONRequest(req, &uploadReq.Signatures); err != nil {
		return *err
	}

	uploadReq.UserID = device.UserID
	keyserverAPI.PerformUploadDeviceSignatures(req.Context(), uploadReq, uploadRes)

	if err := uploadRes.Error; err != nil {
		switch {
		case err.IsInvalidSignature:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidSignature(err.Error()),
			}
		case err.IsMissingParam:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MissingParam(err.Error()),
			}
		case err.IsInvalidParam:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		default:
			return xutil.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown(err.Error()),
			}
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
