package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/withqb/coddy/services/clientapi/auth/authtypes"
	userapi "github.com/withqb/coddy/services/userapi/api"
)

// AppServiceInternalAPI is used to query user and frame alias data from application
// services
type AppServiceInternalAPI interface {
	// Check whether a frame alias exists within any application service namespaces
	FrameAliasExists(
		ctx context.Context,
		req *FrameAliasExistsRequest,
		resp *FrameAliasExistsResponse,
	) error
	// Check whether a user ID exists within any application service namespaces
	UserIDExists(
		ctx context.Context,
		req *UserIDExistsRequest,
		resp *UserIDExistsResponse,
	) error

	Locations(ctx context.Context, req *LocationRequest, resp *LocationResponse) error
	User(ctx context.Context, request *UserRequest, response *UserResponse) error
	Protocols(ctx context.Context, req *ProtocolRequest, resp *ProtocolResponse) error
}

// FrameAliasExistsRequest is a request to an application service
// about whether a frame alias exists
type FrameAliasExistsRequest struct {
	// Alias we want to lookup
	Alias string `json:"alias"`
}

// FrameAliasExistsResponse is a response from an application service
// about whether a frame alias exists
type FrameAliasExistsResponse struct {
	AliasExists bool `json:"exists"`
}

// UserIDExistsRequest is a request to an application service about whether a
// user ID exists
type UserIDExistsRequest struct {
	// UserID we want to lookup
	UserID string `json:"user_id"`
}

// UserIDExistsRequestAccessToken is a request to an application service
// about whether a user ID exists. Includes an access token
type UserIDExistsRequestAccessToken struct {
	// UserID we want to lookup
	UserID      string `json:"user_id"`
	AccessToken string `json:"access_token"`
}

// UserIDExistsResponse is a response from an application service about
// whether a user ID exists
type UserIDExistsResponse struct {
	UserIDExists bool `json:"exists"`
}

const (
	ASProtocolPath = "/_coddy/app/unstable/thirdparty/protocol/"
	ASUserPath     = "/_coddy/app/unstable/thirdparty/user"
	ASLocationPath = "/_coddy/app/unstable/thirdparty/location"
)

type ProtocolRequest struct {
	Protocol string `json:"protocol,omitempty"`
}

type ProtocolResponse struct {
	Protocols map[string]ASProtocolResponse `json:"protocols"`
	Exists    bool                          `json:"exists"`
}

type ASProtocolResponse struct {
	FieldTypes     map[string]FieldType `json:"field_types,omitempty"` // NOTSPEC: field_types is required by the spec
	Icon           string               `json:"icon"`
	Instances      []ProtocolInstance   `json:"instances"`
	LocationFields []string             `json:"location_fields"`
	UserFields     []string             `json:"user_fields"`
}

type FieldType struct {
	Placeholder string `json:"placeholder"`
	Regexp      string `json:"regexp"`
}

type ProtocolInstance struct {
	Description string          `json:"desc"`
	Icon        string          `json:"icon,omitempty"`
	NetworkID   string          `json:"network_id,omitempty"` // NOTSPEC: network_id is required by the spec
	Fields      json.RawMessage `json:"fields,omitempty"`     // NOTSPEC: fields is required by the spec
}

type UserRequest struct {
	Protocol string `json:"protocol"`
	Params   string `json:"params"`
}

type UserResponse struct {
	Users  []ASUserResponse `json:"users,omitempty"`
	Exists bool             `json:"exists,omitempty"`
}

type ASUserResponse struct {
	Protocol string          `json:"protocol"`
	UserID   string          `json:"userid"`
	Fields   json.RawMessage `json:"fields"`
}

type LocationRequest struct {
	Protocol string `json:"protocol"`
	Params   string `json:"params"`
}

type LocationResponse struct {
	Locations []ASLocationResponse `json:"locations,omitempty"`
	Exists    bool                 `json:"exists,omitempty"`
}

type ASLocationResponse struct {
	Alias    string          `json:"alias"`
	Protocol string          `json:"protocol"`
	Fields   json.RawMessage `json:"fields"`
}

// ErrProfileNotExists is returned when trying to lookup a user's profile that
// doesn't exist locally.
var ErrProfileNotExists = errors.New("no known profile for given user ID")

// RetrieveUserProfile is a wrapper that queries both the local database and
// application services for a given user's profile
// TDO: Remove this, it's called from federationapi and clientapi but is a pure function
func RetrieveUserProfile(
	ctx context.Context,
	userID string,
	asAPI AppServiceInternalAPI,
	profileAPI userapi.ProfileAPI,
) (*authtypes.Profile, error) {
	// Try to query the user from the local database
	profile, err := profileAPI.QueryProfile(ctx, userID)
	if err == nil {
		return profile, nil
	}

	// Query the appservice component for the existence of an AS user
	userReq := UserIDExistsRequest{UserID: userID}
	var userResp UserIDExistsResponse
	if err = asAPI.UserIDExists(ctx, &userReq, &userResp); err != nil {
		return nil, err
	}

	// If no user exists, return
	if !userResp.UserIDExists {
		return nil, ErrProfileNotExists
	}

	// Try to query the user from the local database again
	profile, err = profileAPI.QueryProfile(ctx, userID)
	if err != nil {
		return nil, err
	}

	// profile should not be nil at this point
	return profile, nil
}
