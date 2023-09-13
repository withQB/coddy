// Copyright 2017 Michael Telatysnki <7t3chguy@gmail.com>
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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	"github.com/withqb/xcore"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

// RequestTurnServer implements:
//
//	GET /voip/turnServer
func RequestTurnServer(req *http.Request, device *api.Device, cfg *config.ClientAPI) xutil.JSONResponse {
	turnConfig := cfg.TURN

	// TODO Guest Support
	if len(turnConfig.URIs) == 0 || turnConfig.UserLifetime == "" {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	// Duration checked at startup, err not possible
	duration, _ := time.ParseDuration(turnConfig.UserLifetime)

	resp := xcore.RespTurnServer{
		URIs: turnConfig.URIs,
		TTL:  int(duration.Seconds()),
	}

	if turnConfig.SharedSecret != "" {
		expiry := time.Now().Add(duration).Unix()
		resp.Username = fmt.Sprintf("%d:%s", expiry, device.UserID)
		mac := hmac.New(sha1.New, []byte(turnConfig.SharedSecret))
		_, err := mac.Write([]byte(resp.Username))

		if err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("mac.Write failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		resp.Password = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	} else if turnConfig.Username != "" && turnConfig.Password != "" {
		resp.Username = turnConfig.Username
		resp.Password = turnConfig.Password
	} else {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}
