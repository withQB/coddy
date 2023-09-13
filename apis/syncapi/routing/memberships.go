package routing

import (
	"encoding/json"
	"math"
	"net/http"

	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type getMembershipResponse struct {
	Chunk []synctypes.ClientEvent `json:"chunk"`
}

type getJoinedMembersResponse struct {
	Joined map[string]joinedMember `json:"joined"`
}

type joinedMember struct {
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
}

// The database stores 'displayname' without an underscore.
// Deserialize into this and then change to the actual API response
type databaseJoinedMember struct {
	DisplayName string `json:"displayname"`
	AvatarURL   string `json:"avatar_url"`
}

// GetMemberships implements
//
//	GET /frames/{frameId}/members
//	GET /frames/{frameId}/joined_members
func GetMemberships(
	req *http.Request, device *userapi.Device, frameID string,
	syncDB storage.Database, rsAPI api.SyncDataframeAPI,
	joinedOnly bool, membership, notMembership *string, at string,
) xutil.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Device UserID is invalid"),
		}
	}
	queryReq := api.QueryMembershipForUserRequest{
		FrameID: frameID,
		UserID: *userID,
	}

	var queryRes api.QueryMembershipForUserResponse
	if queryErr := rsAPI.QueryMembershipForUser(req.Context(), &queryReq, &queryRes); queryErr != nil {
		xutil.GetLogger(req.Context()).WithError(queryErr).Error("rsAPI.QueryMembershipsForFrame failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !queryRes.HasBeenInFrame {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You aren't a member of the frame and weren't previously a member of the frame."),
		}
	}

	if joinedOnly && !queryRes.IsInFrame {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You aren't a member of the frame and weren't previously a member of the frame."),
		}
	}

	db, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	defer db.Rollback() // nolint: errcheck

	atToken, err := types.NewTopologyTokenFromString(at)
	if err != nil {
		atToken = types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}
		if queryRes.HasBeenInFrame && !queryRes.IsInFrame {
			// If you have left the frame then this will be the members of the frame when you left.
			atToken, err = db.EventPositionInTopology(req.Context(), queryRes.EventID)
			if err != nil {
				xutil.GetLogger(req.Context()).WithError(err).Error("unable to get 'atToken'")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
	}

	eventIDs, err := db.SelectMemberships(req.Context(), frameID, atToken, membership, notMembership)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("db.SelectMemberships failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	qryRes := &api.QueryEventsByIDResponse{}
	if err := rsAPI.QueryEventsByID(req.Context(), &api.QueryEventsByIDRequest{EventIDs: eventIDs, FrameID: frameID}, qryRes); err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.QueryEventsByID failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	result := qryRes.Events

	if joinedOnly {
		var res getJoinedMembersResponse
		res.Joined = make(map[string]joinedMember)
		for _, ev := range result {
			var content databaseJoinedMember
			if err := json.Unmarshal(ev.Content(), &content); err != nil {
				xutil.GetLogger(req.Context()).WithError(err).Error("failed to unmarshal event content")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			validFrameID, err := spec.NewFrameID(ev.FrameID())
			if err != nil {
				xutil.GetLogger(req.Context()).WithError(err).Error("frameID is invalid")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			userID, err := rsAPI.QueryUserIDForSender(req.Context(), *validFrameID, ev.SenderID())
			if err != nil || userID == nil {
				xutil.GetLogger(req.Context()).WithError(err).Error("rsAPI.QueryUserIDForSender failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			if err != nil {
				return xutil.JSONResponse{
					Code: http.StatusForbidden,
					JSON: spec.Forbidden("You don't have permission to kick this user, unknown senderID"),
				}
			}
			res.Joined[userID.String()] = joinedMember(content)
		}
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: res,
		}
	}
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: getMembershipResponse{synctypes.ToClientEvents(xtools.ToPDUs(result), synctypes.FormatAll, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(req.Context(), frameID, senderID)
		})},
	}
}
