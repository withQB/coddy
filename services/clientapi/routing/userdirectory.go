package routing

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/withqb/coddy/services/clientapi/auth/authtypes"
	"github.com/withqb/coddy/services/dataframe/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type UserDirectoryResponse struct {
	Results []authtypes.FullyQualifiedProfile `json:"results"`
	Limited bool                              `json:"limited"`
}

func SearchUserDirectory(
	ctx context.Context,
	device *userapi.Device,
	rsAPI api.ClientDataframeAPI,
	provider userapi.QuerySearchProfilesAPI,
	searchString string,
	limit int,
	federation fclient.FederationClient,
	localServerName spec.ServerName,
) xutil.JSONResponse {
	if limit < 10 {
		limit = 10
	}

	results := map[string]authtypes.FullyQualifiedProfile{}
	response := &UserDirectoryResponse{
		Results: []authtypes.FullyQualifiedProfile{},
		Limited: false,
	}

	// Get users we share a frame with
	knownUsersReq := &api.QueryKnownUsersRequest{
		UserID: device.UserID,
		Limit:  limit,
	}
	knownUsersRes := &api.QueryKnownUsersResponse{}
	if err := rsAPI.QueryKnownUsers(ctx, knownUsersReq, knownUsersRes); err != nil && err != sql.ErrNoRows {
		return xutil.ErrorResponse(fmt.Errorf("rsAPI.QueryKnownUsers: %w", err))
	}

knownUsersLoop:
	for _, profile := range knownUsersRes.Users {
		if len(results) == limit {
			response.Limited = true
			break
		}
		userID := profile.UserID
		// get the full profile of the local user
		localpart, serverName, _ := xtools.SplitID('@', userID)
		if serverName == localServerName {
			userReq := &userapi.QuerySearchProfilesRequest{
				SearchString: localpart,
				Limit:        limit,
			}
			userRes := &userapi.QuerySearchProfilesResponse{}
			if err := provider.QuerySearchProfiles(ctx, userReq, userRes); err != nil {
				return xutil.ErrorResponse(fmt.Errorf("userAPI.QuerySearchProfiles: %w", err))
			}
			for _, p := range userRes.Profiles {
				if strings.Contains(p.DisplayName, searchString) ||
					strings.Contains(p.Localpart, searchString) {
					profile.DisplayName = p.DisplayName
					profile.AvatarURL = p.AvatarURL
					results[userID] = profile
					if len(results) == limit {
						response.Limited = true
						break knownUsersLoop
					}
				}
			}
		} else {
			// If the username already contains the search string, don't bother hitting federation.
			// This will result in missing avatars and displaynames, but saves the federation roundtrip.
			if strings.Contains(localpart, searchString) {
				results[userID] = profile
				if len(results) == limit {
					response.Limited = true
					break knownUsersLoop
				}
				continue
			}
			// TDO: We should probably cache/store this
			fedProfile, fedErr := federation.LookupProfile(ctx, localServerName, serverName, userID, "")
			if fedErr != nil {
				if x, ok := fedErr.(xcore.HTTPError); ok {
					if x.Code == http.StatusNotFound {
						continue
					}
				}
			}
			if strings.Contains(fedProfile.DisplayName, searchString) {
				profile.DisplayName = fedProfile.DisplayName
				profile.AvatarURL = fedProfile.AvatarURL
				results[userID] = profile
				if len(results) == limit {
					response.Limited = true
					break knownUsersLoop
				}
			}
		}
	}

	for _, result := range results {
		response.Results = append(response.Results, result)
	}

	return xutil.JSONResponse{
		Code: 200,
		JSON: response,
	}
}
