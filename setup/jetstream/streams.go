package jetstream

import (
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	UserID        = "user_id"
	FrameID        = "frame_id"
	EventID       = "event_id"
	FrameEventType = "output_frame_event_type"
)

var (
	InputFrameEvent          = "InputFrameEvent"
	InputDeviceListUpdate   = "InputDeviceListUpdate"
	InputSigningKeyUpdate   = "InputSigningKeyUpdate"
	OutputFrameEvent         = "OutputFrameEvent"
	OutputSendToDeviceEvent = "OutputSendToDeviceEvent"
	OutputKeyChangeEvent    = "OutputKeyChangeEvent"
	OutputTypingEvent       = "OutputTypingEvent"
	OutputClientData        = "OutputClientData"
	OutputNotificationData  = "OutputNotificationData"
	OutputReceiptEvent      = "OutputReceiptEvent"
	OutputStreamEvent       = "OutputStreamEvent"
	OutputReadUpdate        = "OutputReadUpdate"
	RequestPresence         = "GetPresence"
	OutputPresenceEvent     = "OutputPresenceEvent"
	InputFulltextReindex    = "InputFulltextReindex"
)

var safeCharacters = regexp.MustCompile("[^A-Za-z0-9$]+")

func Tokenise(str string) string {
	return safeCharacters.ReplaceAllString(str, "_")
}

func InputFrameEventSubj(frameID string) string {
	return fmt.Sprintf("%s.%s", InputFrameEvent, Tokenise(frameID))
}

var streams = []*nats.StreamConfig{
	{
		Name:      InputFrameEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
		MaxAge:    time.Hour * 24,
	},
	{
		Name:      InputDeviceListUpdate,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      InputSigningKeyUpdate,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputFrameEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputSendToDeviceEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputKeyChangeEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputTypingEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.MemoryStorage,
		MaxAge:    time.Second * 60,
	},
	{
		Name:      OutputClientData,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputReceiptEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputNotificationData,
		Retention: nats.InterestPolicy,
		Storage:   nats.FileStorage,
	},
	{
		Name:      OutputPresenceEvent,
		Retention: nats.InterestPolicy,
		Storage:   nats.MemoryStorage,
		MaxAge:    time.Minute * 5,
	},
}
