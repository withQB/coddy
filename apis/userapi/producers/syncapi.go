package producers

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"

	"github.com/withqb/coddy/apis/userapi/storage"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/setup/jetstream"
)

type JetStreamPublisher interface {
	PublishMsg(*nats.Msg, ...nats.PubOpt) (*nats.PubAck, error)
}

// SyncAPI produces messages for the Sync API server to consume.
type SyncAPI struct {
	db                    storage.Notification
	producer              JetStreamPublisher
	clientDataTopic       string
	notificationDataTopic string
}

func NewSyncAPI(db storage.UserDatabase, js JetStreamPublisher, clientDataTopic string, notificationDataTopic string) *SyncAPI {
	return &SyncAPI{
		db:                    db,
		producer:              js,
		clientDataTopic:       clientDataTopic,
		notificationDataTopic: notificationDataTopic,
	}
}

// SendAccountData sends account data to the Sync API server.
func (p *SyncAPI) SendAccountData(userID string, data eventutil.AccountData) error {
	m := &nats.Msg{
		Subject: p.clientDataTopic,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)

	var err error
	m.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"user_id":   userID,
		"frame_id":   data.FrameID,
		"data_type": data.Type,
	}).Tracef("Producing to topic '%s'", p.clientDataTopic)

	_, err = p.producer.PublishMsg(m)
	return err
}

// GetAndSendNotificationData reads the database and sends data about unread
// notifications to the Sync API server.
func (p *SyncAPI) GetAndSendNotificationData(ctx context.Context, userID, frameID string) error {
	localpart, domain, err := xtools.SplitID('@', userID)
	if err != nil {
		return err
	}

	ntotal, nhighlight, err := p.db.GetFrameNotificationCounts(ctx, localpart, domain, frameID)
	if err != nil {
		return err
	}

	return p.sendNotificationData(userID, &eventutil.NotificationData{
		FrameID:                  frameID,
		UnreadHighlightCount:    int(nhighlight),
		UnreadNotificationCount: int(ntotal),
	})
}

// sendNotificationData sends data about unread notifications to the Sync API server.
func (p *SyncAPI) sendNotificationData(userID string, data *eventutil.NotificationData) error {
	m := &nats.Msg{
		Subject: p.notificationDataTopic,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)

	var err error
	m.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"user_id": userID,
		"frame_id": data.FrameID,
	}).Tracef("Producing to topic '%s'", p.clientDataTopic)

	_, err = p.producer.PublishMsg(m)
	return err
}
