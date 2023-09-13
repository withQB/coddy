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

	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// TDO: This logic should live in xtools

// IsServerAllowed returns true if the server is allowed to see events in the frame
// at this particular state
func IsServerAllowed(
	ctx context.Context, querier api.QuerySenderIDAPI,
	serverName spec.ServerName,
	serverCurrentlyInFrame bool,
	authEvents []xtools.PDU,
) bool {
	historyVisibility := HistoryVisibilityForFrame(authEvents)

	// 1. If the history_visibility was set to world_readable, allow.
	if historyVisibility == xtools.HistoryVisibilityWorldReadable {
		return true
	}
	// 2. If the user's membership was join, allow.
	joinedUserExists := IsAnyUserOnServerWithMembership(ctx, querier, serverName, authEvents, spec.Join)
	if joinedUserExists {
		return true
	}
	// 3. If history_visibility was set to shared, and the user joined the frame at any point after the event was sent, allow.
	if historyVisibility == xtools.HistoryVisibilityShared && serverCurrentlyInFrame {
		return true
	}
	// 4. If the user's membership was invite, and the history_visibility was set to invited, allow.
	invitedUserExists := IsAnyUserOnServerWithMembership(ctx, querier, serverName, authEvents, spec.Invite)
	if invitedUserExists && historyVisibility == xtools.HistoryVisibilityInvited {
		return true
	}

	// 5. Otherwise, deny.
	return false
}

func HistoryVisibilityForFrame(authEvents []xtools.PDU) xtools.HistoryVisibility {
	// By default if no history_visibility is set, or if the value is not understood, the visibility is assumed to be shared.
	visibility := xtools.HistoryVisibilityShared
	for _, ev := range authEvents {
		if ev.Type() != spec.MFrameHistoryVisibility {
			continue
		}
		if vis, err := ev.HistoryVisibility(); err == nil {
			visibility = vis
		}
	}
	return visibility
}

func IsAnyUserOnServerWithMembership(ctx context.Context, querier api.QuerySenderIDAPI, serverName spec.ServerName, authEvents []xtools.PDU, wantMembership string) bool {
	for _, ev := range authEvents {
		if ev.Type() != spec.MFrameMember {
			continue
		}
		membership, err := ev.Membership()
		if err != nil || membership != wantMembership {
			continue
		}

		stateKey := ev.StateKey()
		if stateKey == nil {
			continue
		}

		validFrameID, err := spec.NewFrameID(ev.FrameID())
		if err != nil {
			continue
		}
		userID, err := querier.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*stateKey))
		if err != nil {
			continue
		}

		if userID.Domain() == serverName {
			return true
		}
	}
	return false
}
