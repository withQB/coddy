package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xcore"
	"github.com/withqb/xtools/tokens"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/services/dataframe/types"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/transactions"
	appserviceAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/clientapi/httputil"
	"github.com/withqb/coddy/services/dataframe/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools/spec"
)

// Unspecced server notice request
// https://github.com/withqb/synapse/blob/develop/docs/admin_api/server_notices.md
type sendServerNoticeRequest struct {
	UserID  string `json:"user_id,omitempty"`
	Content struct {
		MsgType string `json:"msgtype,omitempty"`
		Body    string `json:"body,omitempty"`
	} `json:"content,omitempty"`
	Type     string `json:"type,omitempty"`
	StateKey string `json:"state_key,omitempty"`
}

// nolint:gocyclo
// SendServerNotice sends a message to a specific user. It can only be invoked by an admin.
func SendServerNotice(
	req *http.Request,
	cfgNotices *config.ServerNotices,
	cfgClient *config.ClientAPI,
	userAPI userapi.ClientUserAPI,
	rsAPI api.ClientDataframeAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	device *userapi.Device,
	senderDevice *userapi.Device,
	txnID *string,
	txnCache *transactions.Cache,
) xutil.JSONResponse {
	if device.AccountType != userapi.AccountTypeAdmin {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("This API can only be used by admin users."),
		}
	}

	if txnID != nil {
		// Try to fetch response from transactionsCache
		if res, ok := txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}

	ctx := req.Context()
	var r sendServerNoticeRequest
	resErr := httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	// check that all required fields are set
	if !r.valid() {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid request"),
		}
	}

	userID, err := spec.NewUserID(r.UserID, true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid user ID"),
		}
	}

	// get frames for specified user
	allUserFrames := []spec.FrameID{}
	// Get frames the user is either joined, invited or has left.
	for _, membership := range []string{"join", "invite", "leave"} {
		userFrames, queryErr := rsAPI.QueryFramesForUser(ctx, *userID, membership)
		if queryErr != nil {
			return xutil.ErrorResponse(err)
		}
		allUserFrames = append(allUserFrames, userFrames...)
	}

	// get frames of the sender
	senderUserID, err := spec.NewUserID(fmt.Sprintf("@%s:%s", cfgNotices.LocalPart, cfgClient.Coddy.ServerName), true)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("internal server error"),
		}
	}
	senderFrames, err := rsAPI.QueryFramesForUser(ctx, *senderUserID, "join")
	if err != nil {
		return xutil.ErrorResponse(err)
	}

	// check if we have frames in common
	commonFrames := []spec.FrameID{}
	for _, userFrameID := range allUserFrames {
		for _, senderFrameID := range senderFrames {
			if userFrameID == senderFrameID {
				commonFrames = append(commonFrames, senderFrameID)
			}
		}
	}

	if len(commonFrames) > 1 {
		return xutil.ErrorResponse(fmt.Errorf("expected to find one frame, but got %d", len(commonFrames)))
	}

	var (
		frameID      string
		frameVersion = rsAPI.DefaultFrameVersion()
	)

	// create a new frame for the user
	if len(commonFrames) == 0 {
		powerLevelContent := eventutil.InitialPowerLevelsContent(senderUserID.String())
		powerLevelContent.Users[r.UserID] = -10 // taken from Synapse
		pl, err := json.Marshal(powerLevelContent)
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		createContent := map[string]interface{}{}
		createContent["m.federate"] = false
		cc, err := json.Marshal(createContent)
		if err != nil {
			return xutil.ErrorResponse(err)
		}
		crReq := createFrameRequest{
			Invite:                    []string{r.UserID},
			Name:                      cfgNotices.FrameName,
			Visibility:                "private",
			Preset:                    spec.PresetPrivateChat,
			CreationContent:           cc,
			FrameVersion:               frameVersion,
			PowerLevelContentOverride: pl,
		}

		frameRes := createFrame(ctx, crReq, senderDevice, cfgClient, userAPI, rsAPI, asAPI, time.Now())

		switch data := frameRes.JSON.(type) {
		case createFrameResponse:
			frameID = data.FrameID

			// tag the frame, so we can later check if the user tries to reject an invite
			serverAlertTag := xcore.TagContent{Tags: map[string]xcore.TagProperties{
				"m.server_notice": {
					Order: 1.0,
				},
			}}
			if err = saveTagData(req, r.UserID, frameID, userAPI, serverAlertTag); err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("saveTagData failed")
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

		default:
			// if we didn't get a createFrameResponse, we probably received an error, so return that.
			return frameRes
		}
	} else {
		// we've found a frame in common, check the membership
		deviceUserID, err := spec.NewUserID(r.UserID, true)
		if err != nil {
			return xutil.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("userID doesn't have power level to change visibility"),
			}
		}

		frameID = commonFrames[0].String()
		membershipRes := api.QueryMembershipForUserResponse{}
		err = rsAPI.QueryMembershipForUser(ctx, &api.QueryMembershipForUserRequest{UserID: *deviceUserID, FrameID: frameID}, &membershipRes)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Error("unable to query membership for user")
			return xutil.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if !membershipRes.IsInFrame {
			// re-invite the user
			res, err := sendInvite(ctx, userAPI, senderDevice, frameID, r.UserID, "Server notice frame", cfgClient, rsAPI, asAPI, time.Now())
			if err != nil {
				return res
			}
		}
	}

	startedGeneratingEvent := time.Now()

	request := map[string]interface{}{
		"body":    r.Content.Body,
		"msgtype": r.Content.MsgType,
	}
	e, resErr := generateSendEvent(ctx, request, senderDevice, frameID, "m.frame.message", nil, rsAPI, time.Now())
	if resErr != nil {
		logrus.Errorf("failed to send message: %+v", resErr)
		return *resErr
	}
	timeToGenerateEvent := time.Since(startedGeneratingEvent)

	var txnAndSessionID *api.TransactionID
	if txnID != nil {
		txnAndSessionID = &api.TransactionID{
			TransactionID: *txnID,
			SessionID:     device.SessionID,
		}
	}

	// pass the new event to the dataframe and receive the correct event ID
	// event ID in case of duplicate transaction is discarded
	startedSubmittingEvent := time.Now()
	if err := api.SendEvents(
		ctx, rsAPI,
		api.KindNew,
		[]*types.HeaderedEvent{
			{PDU: e},
		},
		device.UserDomain(),
		cfgClient.Coddy.ServerName,
		cfgClient.Coddy.ServerName,
		txnAndSessionID,
		false,
	); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("SendEvents failed")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	xutil.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id":     e.EventID(),
		"frame_id":      frameID,
		"frame_version": frameVersion,
	}).Info("Sent event to dataframe")
	timeToSubmitEvent := time.Since(startedSubmittingEvent)

	res := xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: sendEventResponse{e.EventID()},
	}
	// Add response to transactionsCache
	if txnID != nil {
		txnCache.AddTransaction(device.AccessToken, *txnID, req.URL, &res)
	}

	// Take a note of how long it took to generate the event vs submit
	// it to the dataframe.
	sendEventDuration.With(prometheus.Labels{"action": "build"}).Observe(float64(timeToGenerateEvent.Milliseconds()))
	sendEventDuration.With(prometheus.Labels{"action": "submit"}).Observe(float64(timeToSubmitEvent.Milliseconds()))

	return res
}

func (r sendServerNoticeRequest) valid() (ok bool) {
	if r.UserID == "" {
		return false
	}
	if r.Content.MsgType == "" || r.Content.Body == "" {
		return false
	}
	return true
}

// getSenderDevice creates a user account to be used when sending server notices.
// It returns an userapi.Device, which is used for building the event
func getSenderDevice(
	ctx context.Context,
	rsAPI api.ClientDataframeAPI,
	userAPI userapi.ClientUserAPI,
	cfg *config.ClientAPI,
) (*userapi.Device, error) {
	var accRes userapi.PerformAccountCreationResponse
	// create account if it doesn't exist
	err := userAPI.PerformAccountCreation(ctx, &userapi.PerformAccountCreationRequest{
		AccountType: userapi.AccountTypeUser,
		Localpart:   cfg.Coddy.ServerNotices.LocalPart,
		ServerName:  cfg.Coddy.ServerName,
		OnConflict:  userapi.ConflictUpdate,
	}, &accRes)
	if err != nil {
		return nil, err
	}

	// Set the avatarurl for the user
	profile, avatarChanged, err := userAPI.SetAvatarURL(ctx,
		cfg.Coddy.ServerNotices.LocalPart,
		cfg.Coddy.ServerName,
		cfg.Coddy.ServerNotices.AvatarURL,
	)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("userAPI.SetAvatarURL failed")
		return nil, err
	}

	// Set the displayname for the user
	_, displayNameChanged, err := userAPI.SetDisplayName(ctx,
		cfg.Coddy.ServerNotices.LocalPart,
		cfg.Coddy.ServerName,
		cfg.Coddy.ServerNotices.DisplayName,
	)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("userAPI.SetDisplayName failed")
		return nil, err
	}

	if displayNameChanged {
		profile.DisplayName = cfg.Coddy.ServerNotices.DisplayName
	}

	// Check if we got existing devices
	deviceRes := &userapi.QueryDevicesResponse{}
	err = userAPI.QueryDevices(ctx, &userapi.QueryDevicesRequest{
		UserID: accRes.Account.UserID,
	}, deviceRes)
	if err != nil {
		return nil, err
	}

	// We've got an existing account, return the first device of it
	if len(deviceRes.Devices) > 0 {
		// If there were changes to the profile, create a new membership event
		if displayNameChanged || avatarChanged {
			_, err = updateProfile(ctx, rsAPI, &deviceRes.Devices[0], profile, accRes.Account.UserID, time.Now())
			if err != nil {
				return nil, err
			}
		}
		return &deviceRes.Devices[0], nil
	}

	// create an AccessToken
	token, err := tokens.GenerateLoginToken(tokens.TokenOptions{
		ServerPrivateKey: cfg.Coddy.PrivateKey.Seed(),
		ServerName:       string(cfg.Coddy.ServerName),
		UserID:           accRes.Account.UserID,
	})
	if err != nil {
		return nil, err
	}

	// create a new device, if we didn't find any
	var devRes userapi.PerformDeviceCreationResponse
	err = userAPI.PerformDeviceCreation(ctx, &userapi.PerformDeviceCreationRequest{
		Localpart:          cfg.Coddy.ServerNotices.LocalPart,
		ServerName:         cfg.Coddy.ServerName,
		DeviceDisplayName:  &cfg.Coddy.ServerNotices.LocalPart,
		AccessToken:        token,
		NoDeviceListUpdate: true,
	}, &devRes)

	if err != nil {
		return nil, err
	}
	return devRes.Device, nil
}
