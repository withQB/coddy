package routing

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/clientapi/httputil"
	"github.com/withqb/coddy/clientapi/producers"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/syncapi/types"
	"github.com/withqb/coddy/userapi/api"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type presenceReq struct {
	Presence  string  `json:"presence"`
	StatusMsg *string `json:"status_msg,omitempty"`
}

func SetPresence(
	req *http.Request,
	cfg *config.ClientAPI,
	device *api.Device,
	producer *producers.SyncAPIProducer,
	userID string,
) xutil.JSONResponse {
	if !cfg.Matrix.Presence.EnableOutbound {
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	if device.UserID != userID {
		return xutil.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Unable to set presence for other user."),
		}
	}
	var presence presenceReq
	parseErr := httputil.UnmarshalJSONRequest(req, &presence)
	if parseErr != nil {
		return *parseErr
	}

	presenceStatus, ok := types.PresenceFromString(presence.Presence)
	if !ok {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(fmt.Sprintf("Unknown presence '%s'.", presence.Presence)),
		}
	}
	err := producer.SendPresence(req.Context(), userID, presenceStatus, presence.StatusMsg)
	if err != nil {
		log.WithError(err).Errorf("failed to update presence")
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

func GetPresence(
	req *http.Request,
	device *api.Device,
	natsClient *nats.Conn,
	presenceTopic string,
	userID string,
) xutil.JSONResponse {
	msg := nats.NewMsg(presenceTopic)
	msg.Header.Set(jetstream.UserID, userID)

	presence, err := natsClient.RequestMsg(msg, time.Second*10)
	if err != nil {
		log.WithError(err).Errorf("unable to get presence")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	statusMsg := presence.Header.Get("status_msg")
	e := presence.Header.Get("error")
	if e != "" {
		log.Errorf("received error msg from nats: %s", e)
		return xutil.JSONResponse{
			Code: http.StatusOK,
			JSON: types.PresenceClientResponse{
				Presence: types.PresenceUnavailable.String(),
			},
		}
	}
	lastActive, err := strconv.Atoi(presence.Header.Get("last_active_ts"))
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	p := types.PresenceInternal{LastActiveTS: spec.Timestamp(lastActive)}
	currentlyActive := p.CurrentlyActive()
	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: types.PresenceClientResponse{
			CurrentlyActive: &currentlyActive,
			LastActiveAgo:   p.LastActiveAgo(),
			Presence:        presence.Header.Get("presence"),
			StatusMsg:       &statusMsg,
		},
	}
}
