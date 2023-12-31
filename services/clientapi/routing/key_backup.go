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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/withqb/coddy/services/clientapi/httputil"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type keyBackupVersion struct {
	Algorithm string          `json:"algorithm"`
	AuthData  json.RawMessage `json:"auth_data"`
}

type keyBackupVersionCreateResponse struct {
	Version string `json:"version"`
}

type keyBackupVersionResponse struct {
	Algorithm string          `json:"algorithm"`
	AuthData  json.RawMessage `json:"auth_data"`
	Count     int64           `json:"count"`
	ETag      string          `json:"etag"`
	Version   string          `json:"version"`
}

type keyBackupSessionRequest struct {
	Frames map[string]struct {
		Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
	} `json:"frames"`
}

type keyBackupSessionResponse struct {
	Count int64  `json:"count"`
	ETag  string `json:"etag"`
}

// Create a new key backup. Request must contain a `keyBackupVersion`. Returns a `keyBackupVersionCreateResponse`.
// Implements  POST /_coddy/client/r0/frame_keys/version
func CreateKeyBackupVersion(req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device) xutil.JSONResponse {
	var kb keyBackupVersion
	resErr := httputil.UnmarshalJSONRequest(req, &kb)
	if resErr != nil {
		return *resErr
	}
	if len(kb.AuthData) == 0 {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("missing auth_data"),
		}
	}
	version, err := userAPI.PerformKeyBackup(req.Context(), &userapi.PerformKeyBackupRequest{
		UserID:    device.UserID,
		Version:   "",
		AuthData:  kb.AuthData,
		Algorithm: kb.Algorithm,
	})
	if err != nil {
		return xutil.ErrorResponse(fmt.Errorf("PerformKeyBackup: %w", err))
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: keyBackupVersionCreateResponse{
			Version: version,
		},
	}
}

// KeyBackupVersion returns the key backup version specified. If `version` is empty, the latest `keyBackupVersionResponse` is returned.
// Implements GET  /_coddy/client/r0/frame_keys/version and GET /_coddy/client/r0/frame_keys/version/{version}
func KeyBackupVersion(req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device, version string) xutil.JSONResponse {
	queryResp, err := userAPI.QueryKeyBackup(req.Context(), &userapi.QueryKeyBackupRequest{
		UserID:  device.UserID,
		Version: version,
	})
	if err != nil {
		return xutil.ErrorResponse(fmt.Errorf("QueryKeyBackup: %s", err))
	}
	if !queryResp.Exists {
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("version not found"),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: keyBackupVersionResponse{
			Algorithm: queryResp.Algorithm,
			AuthData:  queryResp.AuthData,
			Count:     queryResp.Count,
			ETag:      queryResp.ETag,
			Version:   queryResp.Version,
		},
	}
}

// Modify the auth data of a key backup. Version must not be empty. Request must contain a `keyBackupVersion`
// Implements PUT  /_coddy/client/r0/frame_keys/version/{version}
func ModifyKeyBackupVersionAuthData(req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device, version string) xutil.JSONResponse {
	var kb keyBackupVersion
	resErr := httputil.UnmarshalJSONRequest(req, &kb)
	if resErr != nil {
		return *resErr
	}
	performKeyBackupResp, err := userAPI.UpdateBackupKeyAuthData(req.Context(), &userapi.PerformKeyBackupRequest{
		UserID:    device.UserID,
		Version:   version,
		AuthData:  kb.AuthData,
		Algorithm: kb.Algorithm,
	})
	switch e := err.(type) {
	case spec.ErrFrameKeysVersion:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: e,
		}
	case nil:
	default:
		return xutil.ErrorResponse(fmt.Errorf("PerformKeyBackup: %w", e))
	}

	if !performKeyBackupResp.Exists {
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("backup version not found"),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: keyBackupVersionCreateResponse{
			Version: performKeyBackupResp.Version,
		},
	}
}

// Delete a version of key backup. Version must not be empty. If the key backup was previously deleted, will return 200 OK.
// Implements DELETE  /_coddy/client/r0/frame_keys/version/{version}
func DeleteKeyBackupVersion(req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device, version string) xutil.JSONResponse {
	exists, err := userAPI.DeleteKeyBackup(req.Context(), device.UserID, version)
	if err != nil {
		return xutil.ErrorResponse(fmt.Errorf("DeleteKeyBackup: %s", err))
	}
	if !exists {
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("backup version not found"),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: struct{}{},
	}
}

// Upload a bunch of session keys for a given `version`.
func UploadBackupKeys(
	req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device, version string, keys *keyBackupSessionRequest,
) xutil.JSONResponse {
	performKeyBackupResp, err := userAPI.UpdateBackupKeyAuthData(req.Context(), &userapi.PerformKeyBackupRequest{
		UserID:  device.UserID,
		Version: version,
		Keys:    *keys,
	})

	switch e := err.(type) {
	case spec.ErrFrameKeysVersion:
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: e,
		}
	case nil:
	default:
		return xutil.ErrorResponse(fmt.Errorf("PerformKeyBackup: %w", e))
	}
	if !performKeyBackupResp.Exists {
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("backup version not found"),
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: keyBackupSessionResponse{
			Count: performKeyBackupResp.KeyCount,
			ETag:  performKeyBackupResp.KeyETag,
		},
	}
}

// Get keys from a given backup version. Response returned varies depending on if frameID and sessionID are set.
func GetBackupKeys(
	req *http.Request, userAPI userapi.ClientUserAPI, device *userapi.Device, version, frameID, sessionID string,
) xutil.JSONResponse {
	queryResp, err := userAPI.QueryKeyBackup(req.Context(), &userapi.QueryKeyBackupRequest{
		UserID:           device.UserID,
		Version:          version,
		ReturnKeys:       true,
		KeysForFrameID:    frameID,
		KeysForSessionID: sessionID,
	})
	if err != nil {
		return xutil.ErrorResponse(fmt.Errorf("QueryKeyBackup: %w", err))
	}
	if !queryResp.Exists {
		return xutil.JSONResponse{
			Code: 404,
			JSON: spec.NotFound("version not found"),
		}
	}
	if sessionID != "" {
		// return the key itself if it was found
		frameData, ok := queryResp.Keys[frameID]
		if ok {
			key, ok := frameData[sessionID]
			if ok {
				return xutil.JSONResponse{
					Code: 200,
					JSON: key,
				}
			}
		}
	} else if frameID != "" {
		frameData, ok := queryResp.Keys[frameID]
		if !ok {
			// If no keys are found, then an object with an empty sessions property will be returned
			frameData = make(map[string]userapi.KeyBackupSession)
		}
		// wrap response in "sessions"
		return xutil.JSONResponse{
			Code: 200,
			JSON: struct {
				Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
			}{
				Sessions: frameData,
			},
		}

	} else {
		// response is the same as the upload request
		var resp keyBackupSessionRequest
		resp.Frames = make(map[string]struct {
			Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
		})
		for frameID, frameData := range queryResp.Keys {
			resp.Frames[frameID] = struct {
				Sessions map[string]userapi.KeyBackupSession `json:"sessions"`
			}{
				Sessions: frameData,
			}
		}
		return xutil.JSONResponse{
			Code: 200,
			JSON: resp,
		}
	}
	return xutil.JSONResponse{
		Code: 404,
		JSON: spec.NotFound("keys not found"),
	}
}
