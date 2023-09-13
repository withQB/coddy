package api

import (
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// An OutputType is a type of dataframe output.
type OutputType string

const (
	// OutputTypeNewFrameEvent indicates that the event is an OutputNewFrameEvent
	OutputTypeNewFrameEvent OutputType = "new_frame_event"
	// OutputTypeOldFrameEvent indicates that the event is an OutputOldFrameEvent
	OutputTypeOldFrameEvent OutputType = "old_frame_event"
	// OutputTypeNewInviteEvent indicates that the event is an OutputNewInviteEvent
	OutputTypeNewInviteEvent OutputType = "new_invite_event"
	// OutputTypeRetireInviteEvent indicates that the event is an OutputRetireInviteEvent
	OutputTypeRetireInviteEvent OutputType = "retire_invite_event"
	// OutputTypeRedactedEvent indicates that the event is an OutputRedactedEvent
	//
	// This event is emitted when a redaction has been 'validated' (meaning both the redaction and the event to redact are known).
	// Redaction validation happens when the dataframe receives either:
	// - A redaction for which we have the event to redact.
	// - Any event for which we have a redaction.
	// When the dataframe receives an event, it will check against the redactions table to see if there is a matching redaction
	// for the event. If there is, it will mark the redaction as validated and emit this event. In the common case of a redaction
	// happening after receiving the event to redact, the dataframe will emit a OutputTypeNewFrameEvent of m.frame.redaction
	// immediately followed by a OutputTypeRedactedEvent. In the uncommon case of receiving the redaction BEFORE the event to redact,
	// the dataframe will emit a OutputTypeNewFrameEvent of the event to redact immediately followed by a OutputTypeRedactedEvent.
	//
	// In order to honour redactions correctly, downstream components must ignore m.frame.redaction events emitted via OutputTypeNewFrameEvent.
	// When downstream components receive an OutputTypeRedactedEvent they must:
	// - Pull out the event to redact from the database. They should have this because the redaction is validated.
	// - Redact the event and set the corresponding `unsigned` fields to indicate it as redacted.
	// - Replace the event in the database.
	OutputTypeRedactedEvent OutputType = "redacted_event"

	// OutputTypeNewPeek indicates that the kafka event is an OutputNewPeek
	OutputTypeNewPeek OutputType = "new_peek"
	// OutputTypeNewInboundPeek indicates that the kafka event is an OutputNewInboundPeek
	OutputTypeNewInboundPeek OutputType = "new_inbound_peek"
	// OutputTypeRetirePeek indicates that the kafka event is an OutputRetirePeek
	OutputTypeRetirePeek OutputType = "retire_peek"
	// OutputTypePurgeFrame indicates the event is an OutputPurgeFrame
	OutputTypePurgeFrame OutputType = "purge_frame"
)

// An OutputEvent is an entry in the dataframe output kafka log.
// Consumers should check the type field when consuming this event.
type OutputEvent struct {
	// What sort of event this is.
	Type OutputType `json:"type"`
	// The content of event with type OutputTypeNewFrameEvent
	NewFrameEvent *OutputNewFrameEvent `json:"new_frame_event,omitempty"`
	// The content of event with type OutputTypeOldFrameEvent
	OldFrameEvent *OutputOldFrameEvent `json:"old_frame_event,omitempty"`
	// The content of event with type OutputTypeNewInviteEvent
	NewInviteEvent *OutputNewInviteEvent `json:"new_invite_event,omitempty"`
	// The content of event with type OutputTypeRetireInviteEvent
	RetireInviteEvent *OutputRetireInviteEvent `json:"retire_invite_event,omitempty"`
	// The content of event with type OutputTypeRedactedEvent
	RedactedEvent *OutputRedactedEvent `json:"redacted_event,omitempty"`
	// The content of event with type OutputTypeNewPeek
	NewPeek *OutputNewPeek `json:"new_peek,omitempty"`
	// The content of event with type OutputTypeNewInboundPeek
	NewInboundPeek *OutputNewInboundPeek `json:"new_inbound_peek,omitempty"`
	// The content of event with type OutputTypeRetirePeek
	RetirePeek *OutputRetirePeek `json:"retire_peek,omitempty"`
	// The content of the event with type OutputPurgeFrame
	PurgeFrame *OutputPurgeFrame `json:"purge_frame,omitempty"`
}

// Type of the OutputNewFrameEvent.
type OutputFrameEventType int

const (
	// The event is a timeline event and likely just happened.
	OutputFrameTimeline OutputFrameEventType = iota

	// The event is a state event and quite possibly happened in the past.
	OutputFrameState
)

// An OutputNewFrameEvent is written when the dataframe receives a new event.
// It contains the full coddy frame event and enough information for a
// consumer to construct the current state of the frame and the state before the
// event.
//
// When we talk about state in a coddy frame we are talking about the state
// after a list of events. The current state is the state after the latest
// event IDs in the frame. The state before an event is the state after its
// prev_events.
type OutputNewFrameEvent struct {
	// The Event.
	Event *types.HeaderedEvent `json:"event"`
	// Does the event completely rewrite the frame state? If so, then AddsStateEventIDs
	// will contain the entire frame state.
	RewritesState bool `json:"rewrites_state,omitempty"`
	// The latest events in the frame after this event.
	// This can be used to set the prev events for new events in the frame.
	// This also can be used to get the full current state after this event.
	LatestEventIDs []string `json:"latest_event_ids"`
	// The state event IDs that were added to the state of the frame by this event.
	// Together with RemovesStateEventIDs this allows the receiver to keep an up to date
	// view of the current state of the frame.
	AddsStateEventIDs []string `json:"adds_state_event_ids,omitempty"`
	// The state event IDs that were removed from the state of the frame by this event.
	RemovesStateEventIDs []string `json:"removes_state_event_ids,omitempty"`
	// The ID of the event that was output before this event.
	// Or the empty string if this is the first event output for this frame.
	// This is used by consumers to check if they can safely update their
	// current state using the delta supplied in AddsStateEventIDs and
	// RemovesStateEventIDs.
	//
	// If the LastSentEventID doesn't match what they were expecting it to be
	// they can use the LatestEventIDs to request the full current state.
	LastSentEventID string `json:"last_sent_event_id"`
	// The state event IDs that are part of the state at the event, but not
	// part of the current state. Together with the StateBeforeRemovesEventIDs
	// this can be used to construct the state before the event from the
	// current state. The StateBeforeAddsEventIDs and StateBeforeRemovesEventIDs
	// delta is applied after the AddsStateEventIDs and RemovesStateEventIDs.
	//
	// Consumers need to know the state at each event in order to determine
	// which users and servers are allowed to see the event. This information
	// is needed to apply the history visibility rules and to tell which
	// servers we need to push events to over federation.
	//
	// The state is given as a delta against the current state because they are
	// usually either the same state, or differ by just a couple of events.
	StateBeforeAddsEventIDs []string `json:"state_before_adds_event_ids,omitempty"`
	// The state event IDs that are part of the current state, but not part
	// of the state at the event.
	StateBeforeRemovesEventIDs []string `json:"state_before_removes_event_ids,omitempty"`
	// The server name to use to push this event to other servers.
	// Or empty if this event shouldn't be pushed to other servers.
	//
	// This is used by the federation sender component. We need to tell it what
	// event it needs to send because it can't tell on its own. Normally if an
	// event was created on this server then we are responsible for sending it.
	// However there are a couple of exceptions. The first is that when the
	// server joins a remote frame through another coddy server, it is the job
	// of the other coddy server to send the event over federation. The second
	// is the reverse of the first, that is when a remote server joins a frame
	// that we are in over federation using our server it is our responsibility
	// to send the join event to other coddy servers.
	//
	// We encode the server name that the event should be sent using here to
	// future proof the API for virtual hosting.
	SendAsServer string `json:"send_as_server"`
	// The transaction ID of the send request if sent by a local user and one
	// was specified
	TransactionID *TransactionID `json:"transaction_id,omitempty"`
	// The history visibility of the event.
	HistoryVisibility xtools.HistoryVisibility `json:"history_visibility"`
}

func (o *OutputNewFrameEvent) NeededStateEventIDs() ([]*types.HeaderedEvent, []string) {
	addsStateEvents := make([]*types.HeaderedEvent, 0, 1)
	missingEventIDs := make([]string, 0, len(o.AddsStateEventIDs))
	for _, eventID := range o.AddsStateEventIDs {
		if eventID != o.Event.EventID() {
			missingEventIDs = append(missingEventIDs, eventID)
		} else {
			addsStateEvents = append(addsStateEvents, o.Event)
		}
	}
	return addsStateEvents, missingEventIDs
}

// An OutputOldFrameEvent is written when the dataframe receives an old event.
// This will typically happen as a result of getting either missing events
// or backfilling. Downstream components may wish to send these events to
// clients when it is advantageous to do so, but with the consideration that
// the event is likely a historic event.
//
// Old events do not update forward extremities or the current frame state,
// therefore they must not be treated as if they do. Downstream components
// should build their current frame state up from OutputNewFrameEvents only.
type OutputOldFrameEvent struct {
	// The Event.
	Event             *types.HeaderedEvent     `json:"event"`
	HistoryVisibility xtools.HistoryVisibility `json:"history_visibility"`
}

// An OutputNewInviteEvent is written whenever an invite becomes active.
// Invite events can be received outside of an existing frame so have to be
// tracked separately from the frame events themselves.
type OutputNewInviteEvent struct {
	// The frame version of the invited frame.
	FrameVersion xtools.FrameVersion `json:"frame_version"`
	// The "m.frame.member" invite event.
	Event *types.HeaderedEvent `json:"event"`
}

// An OutputRetireInviteEvent is written whenever an existing invite is no longer
// active. An invite stops being active if the user joins the frame or if the
// invite is rejected by the user.
type OutputRetireInviteEvent struct {
	// The ID of the "m.frame.member" invite event.
	EventID string
	// The frame ID of the "m.frame.member" invite event.
	FrameID string
	// The target sender ID of the "m.frame.member" invite event that was retired.
	TargetSenderID spec.SenderID
	// Optional event ID of the event that replaced the invite.
	// This can be empty if the invite was rejected locally and we were unable
	// to reach the server that originally sent the invite.
	RetiredByEventID string
	// The "membership" of the user after retiring the invite. One of "join"
	// "leave" or "ban".
	Membership string
}

// An OutputRedactedEvent is written whenever a redaction has been /validated/.
// Downstream components MUST redact the given event ID if they have stored the
// event JSON. It is guaranteed that this event ID has been seen before.
type OutputRedactedEvent struct {
	// The event ID that was redacted
	RedactedEventID string
	// The value of `unsigned.redacted_because` - the redaction event itself
	RedactedBecause *types.HeaderedEvent
}

// An OutputNewPeek is written whenever a user starts peeking into a frame
// using a given device.
type OutputNewPeek struct {
	FrameID   string
	UserID   string
	DeviceID string
}

// An OutputNewInboundPeek is written whenever a server starts peeking into a frame
type OutputNewInboundPeek struct {
	FrameID string
	PeekID string
	// the event ID at which the peek begins (so we can avoid
	// a race between tracking the state returned by /peek and emitting subsequent
	// peeked events)
	LatestEventID string
	ServerName    spec.ServerName
	// how often we told the peeking server to renew the peek
	RenewalInterval int64
}

// An OutputRetirePeek is written whenever a user stops peeking into a frame.
type OutputRetirePeek struct {
	FrameID   string
	UserID   string
	DeviceID string
}

type OutputPurgeFrame struct {
	FrameID string
}
