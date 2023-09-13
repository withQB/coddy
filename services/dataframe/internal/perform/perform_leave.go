package perform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/dataframe/api"
	rsAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/internal/helpers"
	"github.com/withqb/coddy/services/dataframe/internal/input"
	"github.com/withqb/coddy/services/dataframe/storage"
	fsAPI "github.com/withqb/coddy/services/federationapi/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
)

type Leaver struct {
	Cfg     *config.DataFrame
	DB      storage.Database
	FSAPI   fsAPI.DataframeFederationAPI
	RSAPI   rsAPI.DataframeInternalAPI
	UserAPI userapi.DataframeUserAPI
	Inputer *input.Inputer
}

// WriteOutputEvents implements OutputFrameEventWriter
func (r *Leaver) PerformLeave(
	ctx context.Context,
	req *api.PerformLeaveRequest,
	res *api.PerformLeaveResponse,
) ([]api.OutputEvent, error) {
	if !r.Cfg.Coddy.IsLocalServerName(req.Leaver.Domain()) {
		return nil, fmt.Errorf("user %q does not belong to this homeserver", req.Leaver.String())
	}
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"frame_id": req.FrameID,
		"user_id": req.Leaver.String(),
	})
	logger.Info("User requested to leave join")
	if strings.HasPrefix(req.FrameID, "!") {
		output, err := r.performLeaveFrameByID(context.Background(), req, res)
		if err != nil {
			logger.WithError(err).Error("failed to leave frame")
		} else {
			logger.Info("User left frame successfully")
		}
		return output, err
	}
	return nil, fmt.Errorf("frame ID %q is invalid", req.FrameID)
}

// nolint:gocyclo
func (r *Leaver) performLeaveFrameByID(
	ctx context.Context,
	req *api.PerformLeaveRequest,
	res *api.PerformLeaveResponse, // nolint:unparam
) ([]api.OutputEvent, error) {
	frameID, err := spec.NewFrameID(req.FrameID)
	if err != nil {
		return nil, err
	}
	leaver, err := r.RSAPI.QuerySenderIDForUser(ctx, *frameID, req.Leaver)
	if err != nil || leaver == nil {
		return nil, fmt.Errorf("leaver %s has no matching senderID in this frame", req.Leaver.String())
	}

	// If there's an invite outstanding for the frame then respond to
	// that.
	isInvitePending, senderUser, eventID, _, err := helpers.IsInvitePending(ctx, r.DB, req.FrameID, *leaver)
	if err == nil && isInvitePending {
		sender, serr := r.RSAPI.QueryUserIDForSender(ctx, *frameID, senderUser)
		if serr != nil || sender == nil {
			return nil, fmt.Errorf("sender %q has no matching userID", senderUser)
		}
		if !r.Cfg.Coddy.IsLocalServerName(sender.Domain()) {
			return r.performFederatedRejectInvite(ctx, req, res, *sender, eventID, *leaver)
		}
		// check that this is not a "server notice frame"
		accData := &userapi.QueryAccountDataResponse{}
		if err = r.UserAPI.QueryAccountData(ctx, &userapi.QueryAccountDataRequest{
			UserID:   req.Leaver.String(),
			FrameID:   req.FrameID,
			DataType: "m.tag",
		}, accData); err != nil {
			return nil, fmt.Errorf("unable to query account data: %w", err)
		}

		if frameData, ok := accData.FrameAccountData[req.FrameID]; ok {
			tagData, ok := frameData["m.tag"]
			if ok {
				tags := xcore.TagContent{}
				if err = json.Unmarshal(tagData, &tags); err != nil {
					return nil, fmt.Errorf("unable to unmarshal tag content")
				}
				if _, ok = tags.Tags["m.server_notice"]; ok {
					// mimic the returned values from Synapse
					res.Message = "You cannot reject this invite"
					res.Code = 403
					return nil, spec.LeaveServerNoticeError()
				}
			}
		}
	}

	// There's no invite pending, so first of all we want to find out
	// if the frame exists and if the user is actually in it.
	latestReq := api.QueryLatestEventsAndStateRequest{
		FrameID: req.FrameID,
		StateToFetch: []xtools.StateKeyTuple{
			{
				EventType: spec.MFrameMember,
				StateKey:  string(*leaver),
			},
		},
	}
	latestRes := api.QueryLatestEventsAndStateResponse{}
	if err = helpers.QueryLatestEventsAndState(ctx, r.DB, r.RSAPI, &latestReq, &latestRes); err != nil {
		return nil, err
	}
	if !latestRes.FrameExists {
		return nil, fmt.Errorf("frame %q does not exist", req.FrameID)
	}

	// Now let's see if the user is in the frame.
	if len(latestRes.StateEvents) == 0 {
		return nil, fmt.Errorf("user %q is not a member of frame %q", req.Leaver.String(), req.FrameID)
	}
	membership, err := latestRes.StateEvents[0].Membership()
	if err != nil {
		return nil, fmt.Errorf("error getting membership: %w", err)
	}
	if membership != spec.Join && membership != spec.Invite {
		return nil, fmt.Errorf("user %q is not joined to the frame (membership is %q)", req.Leaver.String(), membership)
	}

	// Prepare the template for the leave event.
	senderIDString := string(*leaver)
	proto := xtools.ProtoEvent{
		Type:     spec.MFrameMember,
		SenderID: senderIDString,
		StateKey: &senderIDString,
		FrameID:   req.FrameID,
		Redacts:  "",
	}
	if err = proto.SetContent(map[string]interface{}{"membership": "leave"}); err != nil {
		return nil, fmt.Errorf("eb.SetContent: %w", err)
	}
	if err = proto.SetUnsigned(struct{}{}); err != nil {
		return nil, fmt.Errorf("eb.SetUnsigned: %w", err)
	}

	// We know that the user is in the frame at this point so let's build
	// a leave event.
	// TDO: Check what happens if the frame exists on the server
	// but everyone has since left. I suspect it does the wrong thing.

	validFrameID, err := spec.NewFrameID(req.FrameID)
	if err != nil {
		return nil, err
	}

	var buildRes rsAPI.QueryLatestEventsAndStateResponse
	identity, err := r.RSAPI.SigningIdentityFor(ctx, *validFrameID, req.Leaver)
	if err != nil {
		return nil, fmt.Errorf("SigningIdentityFor: %w", err)
	}
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, time.Now(), r.RSAPI, &buildRes)
	if err != nil {
		return nil, fmt.Errorf("eventutil.QueryAndBuildEvent: %w", err)
	}

	// Give our leave event to the dataframe input stream. The
	// dataframe will process the membership change and notify
	// downstream automatically.
	inputReq := api.InputFrameEventsRequest{
		InputFrameEvents: []api.InputFrameEvent{
			{
				Kind:         api.KindNew,
				Event:        event,
				Origin:       req.Leaver.Domain(),
				SendAsServer: string(req.Leaver.Domain()),
			},
		},
	}
	inputRes := api.InputFrameEventsResponse{}
	r.Inputer.InputFrameEvents(ctx, &inputReq, &inputRes)
	if err = inputRes.Err(); err != nil {
		return nil, fmt.Errorf("r.InputFrameEvents: %w", err)
	}

	return nil, nil
}

func (r *Leaver) performFederatedRejectInvite(
	ctx context.Context,
	req *api.PerformLeaveRequest,
	res *api.PerformLeaveResponse, // nolint:unparam
	inviteSender spec.UserID, eventID string,
	leaver spec.SenderID,
) ([]api.OutputEvent, error) {
	// Ask the federation sender to perform a federated leave for us.
	leaveReq := fsAPI.PerformLeaveRequest{
		FrameID:      req.FrameID,
		UserID:      req.Leaver.String(),
		ServerNames: []spec.ServerName{inviteSender.Domain()},
	}
	leaveRes := fsAPI.PerformLeaveResponse{}
	if err := r.FSAPI.PerformLeave(ctx, &leaveReq, &leaveRes); err != nil {
		// failures in PerformLeave should NEVER stop us from telling other components like the
		// sync API that the invite was withdrawn. Otherwise we can end up with stuck invites.
		xutil.GetLogger(ctx).WithError(err).Errorf("failed to PerformLeave, still retiring invite event")
	}

	info, err := r.DB.FrameInfo(ctx, req.FrameID)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Errorf("failed to get FrameInfo, still retiring invite event")
	}

	updater, err := r.DB.MembershipUpdater(ctx, req.FrameID, string(leaver), true, info.FrameVersion)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Errorf("failed to get MembershipUpdater, still retiring invite event")
	}
	if updater != nil {
		if err = updater.Delete(); err != nil {
			xutil.GetLogger(ctx).WithError(err).Errorf("failed to delete membership, still retiring invite event")
			if err = updater.Rollback(); err != nil {
				xutil.GetLogger(ctx).WithError(err).Errorf("failed to rollback deleting membership, still retiring invite event")
			}
		} else {
			if err = updater.Commit(); err != nil {
				xutil.GetLogger(ctx).WithError(err).Errorf("failed to commit deleting membership, still retiring invite event")
			}
		}
	}

	// Withdraw the invite, so that the sync API etc are
	// notified that we rejected it.
	return []api.OutputEvent{
		{
			Type: api.OutputTypeRetireInviteEvent,
			RetireInviteEvent: &api.OutputRetireInviteEvent{
				EventID:        eventID,
				FrameID:         req.FrameID,
				Membership:     "leave",
				TargetSenderID: leaver,
			},
		},
	}, nil
}
