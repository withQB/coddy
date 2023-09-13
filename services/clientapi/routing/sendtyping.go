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

	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/clientapi/producers"
	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"
)

type typingContentJSON struct {
	Typing  bool  `json:"typing"`
	Timeout int64 `json:"timeout"`
}

// SendTyping handles PUT /frames/{frameID}/typing/{userID}
// sends the typing events to client API typingProducer
func SendTyping(
	req *http.Request, device *userapi.Device, frameID string,
	userID string, rsAPI dataframeAPI.ClientDataframeAPI,
	syncProducer *producers.SyncAPIProducer,
) xutil.JSONResponse {
	if device.UserID != userID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Cannot set another user's typing state"),
		}
	}

	deviceUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to change visibility"),
		}
	}

	// Verify that the user is a member of this frame
	resErr := checkMemberInFrame(req.Context(), rsAPI, *deviceUserID, frameID)
	if resErr != nil {
		return *resErr
	}

	// parse the incoming http request
	var r typingContentJSON
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	if err := syncProducer.SendTyping(req.Context(), userID, frameID, r.Typing, r.Timeout); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("eduProducer.Send failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
