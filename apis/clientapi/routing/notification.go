// Copyright 2021 Dan Peleg <dan@globekeeper.com>
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
	"strconv"

	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// GetNotifications handles /_coddy/client/r0/notifications
func GetNotifications(
	req *http.Request, device *userapi.Device,
	userAPI userapi.ClientUserAPI,
) xutil.JSONResponse {
	var limit int64
	if limitStr := req.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			xutil.GetLogger(req.Context()).WithError(err).Error("ParseInt(limit) failed")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	var queryRes userapi.QueryNotificationsResponse
	localpart, domain, err := xtools.SplitID('@', device.UserID)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("SplitID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	err = userAPI.QueryNotifications(req.Context(), &userapi.QueryNotificationsRequest{
		Localpart:  localpart,
		ServerName: domain,
		From:       req.URL.Query().Get("from"),
		Limit:      int(limit),
		Only:       req.URL.Query().Get("only"),
	}, &queryRes)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("QueryNotifications failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	xutil.GetLogger(req.Context()).WithField("from", req.URL.Query().Get("from")).WithField("limit", limit).WithField("only", req.URL.Query().Get("only")).WithField("next", queryRes.NextToken).Infof("QueryNotifications: len %d", len(queryRes.Notifications))
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: queryRes,
	}
}
