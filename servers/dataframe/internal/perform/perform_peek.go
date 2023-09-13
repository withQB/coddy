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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	fsAPI "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/input"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type Peeker struct {
	ServerName spec.ServerName
	Cfg        *config.DataFrame
	FSAPI      fsAPI.DataframeFederationAPI
	DB         storage.Database

	Inputer *input.Inputer
}

// PerformPeek handles peeking into coddy frames, including over federation by talking to the federationapi.
func (r *Peeker) PerformPeek(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (frameID string, err error) {
	return r.performPeek(ctx, req)
}

func (r *Peeker) performPeek(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (string, error) {
	// FXME: there's way too much duplication with performJoin
	_, domain, err := xtools.SplitID('@', req.UserID)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("supplied user ID %q in incorrect format", req.UserID)}
	}
	if !r.Cfg.Matrix.IsLocalServerName(domain) {
		return "", api.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", req.UserID)}
	}
	if strings.HasPrefix(req.FrameIDOrAlias, "!") {
		return r.performPeekFrameByID(ctx, req)
	}
	if strings.HasPrefix(req.FrameIDOrAlias, "#") {
		return r.performPeekFrameByAlias(ctx, req)
	}
	return "", api.ErrInvalidID{Err: fmt.Errorf("frame ID or alias %q is invalid", req.FrameIDOrAlias)}
}

func (r *Peeker) performPeekFrameByAlias(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (string, error) {
	// Get the domain part of the frame alias.
	_, domain, err := xtools.SplitID('#', req.FrameIDOrAlias)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("alias %q is not in the correct format", req.FrameIDOrAlias)}
	}
	req.ServerNames = append(req.ServerNames, domain)

	// Check if this alias matches our own server configuration. If it
	// doesn't then we'll need to try a federated peek.
	var frameID string
	if !r.Cfg.Matrix.IsLocalServerName(domain) {
		// The alias isn't owned by us, so we will need to try peeking using
		// a remote server.
		dirReq := fsAPI.PerformDirectoryLookupRequest{
			FrameAlias:  req.FrameIDOrAlias, // the frame alias to lookup
			ServerName: domain,            // the server to ask
		}
		dirRes := fsAPI.PerformDirectoryLookupResponse{}
		err = r.FSAPI.PerformDirectoryLookup(ctx, &dirReq, &dirRes)
		if err != nil {
			logrus.WithError(err).Errorf("error looking up alias %q", req.FrameIDOrAlias)
			return "", fmt.Errorf("looking up alias %q over federation failed: %w", req.FrameIDOrAlias, err)
		}
		frameID = dirRes.FrameID
		req.ServerNames = append(req.ServerNames, dirRes.ServerNames...)
	} else {
		// Otherwise, look up if we know this frame alias locally.
		frameID, err = r.DB.GetFrameIDForAlias(ctx, req.FrameIDOrAlias)
		if err != nil {
			return "", fmt.Errorf("lookup frame alias %q failed: %w", req.FrameIDOrAlias, err)
		}
	}

	// If the frame ID is empty then we failed to look up the alias.
	if frameID == "" {
		return "", fmt.Errorf("alias %q not found", req.FrameIDOrAlias)
	}

	// If we do, then pluck out the frame ID and continue the peek.
	req.FrameIDOrAlias = frameID
	return r.performPeekFrameByID(ctx, req)
}

func (r *Peeker) performPeekFrameByID(
	ctx context.Context,
	req *api.PerformPeekRequest,
) (frameID string, err error) {
	frameID = req.FrameIDOrAlias

	// Get the domain part of the frame ID.
	_, domain, err := xtools.SplitID('!', frameID)
	if err != nil {
		return "", api.ErrInvalidID{Err: fmt.Errorf("frame ID %q is invalid: %w", frameID, err)}
	}

	// handle federated peeks
	// FXME: don't create an outbound peek if we already have one going.
	if !r.Cfg.Matrix.IsLocalServerName(domain) {
		// If the server name in the frame ID isn't ours then it's a
		// possible candidate for finding the frame via federation. Add
		// it to the list of servers to try.
		req.ServerNames = append(req.ServerNames, domain)

		// Try peeking by all of the supplied server names.
		fedReq := fsAPI.PerformOutboundPeekRequest{
			FrameID:      req.FrameIDOrAlias, // the frame ID to try and peek
			ServerNames: req.ServerNames,   // the servers to try peeking via
		}
		fedRes := fsAPI.PerformOutboundPeekResponse{}
		_ = r.FSAPI.PerformOutboundPeek(ctx, &fedReq, &fedRes)
		if fedRes.LastError != nil {
			return "", fedRes.LastError
		}
	}

	// If this frame isn't world_readable, we reject.
	// XXX: would be nicer to call this with NIDs
	// XXX: we should probably factor out history_visibility checks into a common utility method somewhere
	// which handles the default value etc.
	var worldReadable = false
	if ev, _ := r.DB.GetStateEvent(ctx, frameID, "m.frame.history_visibility", ""); ev != nil {
		content := map[string]string{}
		if err = json.Unmarshal(ev.Content(), &content); err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("json.Unmarshal for history visibility failed")
			return "", err
		}
		if visibility, ok := content["history_visibility"]; ok {
			worldReadable = visibility == "world_readable"
		}
	}

	if !worldReadable {
		return "", api.ErrNotAllowed{Err: fmt.Errorf("frame is not world-readable")}
	}

	if ev, _ := r.DB.GetStateEvent(ctx, frameID, "m.frame.encryption", ""); ev != nil {
		return "", api.ErrNotAllowed{Err: fmt.Errorf("cannot peek into an encrypted frame")}
	}

	// TDO: handle federated peeks

	err = r.Inputer.OutputProducer.ProduceFrameEvents(frameID, []api.OutputEvent{
		{
			Type: api.OutputTypeNewPeek,
			NewPeek: &api.OutputNewPeek{
				FrameID:   frameID,
				UserID:   req.UserID,
				DeviceID: req.DeviceID,
			},
		},
	})
	if err != nil {
		return "", err
	}

	// By this point, if req.FrameIDOrAlias contained an alias, then
	// it will have been overwritten with a frame ID by performPeekFrameByAlias.
	// We should now include this in the response so that the CS API can
	// return the right frame ID.
	return frameID, nil
}
