// Copyright 2020 New Vector Ltd
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

package perform

import (
	"context"
	"fmt"
	"strings"

	fsAPI "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/input"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

type Unpeeker struct {
	ServerName spec.ServerName
	Cfg        *config.DataFrame
	FSAPI      fsAPI.DataframeFederationAPI
	Inputer    *input.Inputer
}

// PerformUnpeek handles un-peeking coddy frames, including over federation by talking to the federationapi.
func (r *Unpeeker) PerformUnpeek(
	ctx context.Context,
	frameID, userID, deviceID string,
) error {
	// FXME: there's way too much duplication with performJoin
	_, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		return api.ErrInvalidID{Err: fmt.Errorf("supplied user ID %q in incorrect format", userID)}
	}
	if !r.Cfg.Matrix.IsLocalServerName(domain) {
		return api.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", userID)}
	}
	if strings.HasPrefix(frameID, "!") {
		return r.performUnpeekFrameByID(ctx, frameID, userID, deviceID)
	}
	return api.ErrInvalidID{Err: fmt.Errorf("frame ID %q is invalid", frameID)}
}

func (r *Unpeeker) performUnpeekFrameByID(
	_ context.Context,
	frameID, userID, deviceID string,
) (err error) {
	// Get the domain part of the frame ID.
	_, _, err = xtools.SplitID('!', frameID)
	if err != nil {
		return api.ErrInvalidID{Err: fmt.Errorf("frame ID %q is invalid: %w", frameID, err)}
	}

	// TDO: handle federated peeks
	// By this point, if req.FrameIDOrAlias contained an alias, then
	// it will have been overwritten with a frame ID by performPeekFrameByAlias.
	// We should now include this in the response so that the CS API can
	// return the right frame ID.
	return r.Inputer.OutputProducer.ProduceFrameEvents(frameID, []api.OutputEvent{
		{
			Type: api.OutputTypeRetirePeek,
			RetirePeek: &api.OutputRetirePeek{
				FrameID:   frameID,
				UserID:   userID,
				DeviceID: deviceID,
			},
		},
	})
}
