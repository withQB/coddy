package api

import (
	"crypto/ed25519"
	"encoding/json"
	"time"

	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

type PerformCreateFrameRequest struct {
	InvitedUsers              []string
	FrameName                  string
	Visibility                string
	Topic                     string
	StatePreset               string
	CreationContent           json.RawMessage
	InitialState              []xtools.FledglingEvent
	FrameAliasName             string
	FrameVersion               xtools.FrameVersion
	PowerLevelContentOverride json.RawMessage
	IsDirect                  bool

	UserDisplayName string
	UserAvatarURL   string
	KeyID           xtools.KeyID
	PrivateKey      ed25519.PrivateKey
	EventTime       time.Time
}

type PerformJoinRequest struct {
	FrameIDOrAlias string                 `json:"frame_id_or_alias"`
	UserID        string                 `json:"user_id"`
	IsGuest       bool                   `json:"is_guest"`
	Content       map[string]interface{} `json:"content"`
	ServerNames   []spec.ServerName      `json:"server_names"`
	Unsigned      map[string]interface{} `json:"unsigned"`
}

type PerformLeaveRequest struct {
	FrameID string
	Leaver spec.UserID
}

type PerformLeaveResponse struct {
	Code    int         `json:"code,omitempty"`
	Message interface{} `json:"message,omitempty"`
}

type InviteInput struct {
	FrameID      spec.FrameID
	Inviter     spec.UserID
	Invitee     spec.UserID
	DisplayName string
	AvatarURL   string
	Reason      string
	IsDirect    bool
	KeyID       xtools.KeyID
	PrivateKey  ed25519.PrivateKey
	EventTime   time.Time
}

type PerformInviteRequest struct {
	InviteInput     InviteInput
	InviteFrameState []xtools.InviteStrippedState `json:"invite_frame_state"`
	SendAsServer    string                       `json:"send_as_server"`
	TransactionID   *TransactionID               `json:"transaction_id"`
}

type PerformPeekRequest struct {
	FrameIDOrAlias string            `json:"frame_id_or_alias"`
	UserID        string            `json:"user_id"`
	DeviceID      string            `json:"device_id"`
	ServerNames   []spec.ServerName `json:"server_names"`
}

// PerformBackfillRequest is a request to PerformBackfill.
type PerformBackfillRequest struct {
	// The frame to backfill
	FrameID string `json:"frame_id"`
	// A map of backwards extremity event ID to a list of its prev_event IDs.
	BackwardsExtremities map[string][]string `json:"backwards_extremities"`
	// The maximum number of events to retrieve.
	Limit int `json:"limit"`
	// The server interested in the events.
	ServerName spec.ServerName `json:"server_name"`
	// Which virtual host are we doing this for?
	VirtualHost spec.ServerName `json:"virtual_host"`
}

// PrevEventIDs returns the prev_event IDs of all backwards extremities, de-duplicated in a lexicographically sorted order.
func (r *PerformBackfillRequest) PrevEventIDs() []string {
	var prevEventIDs []string
	for _, pes := range r.BackwardsExtremities {
		prevEventIDs = append(prevEventIDs, pes...)
	}
	prevEventIDs = xutil.UniqueStrings(prevEventIDs)
	return prevEventIDs
}

// PerformBackfillResponse is a response to PerformBackfill.
type PerformBackfillResponse struct {
	// Missing events, arbritrary order.
	Events            []*types.HeaderedEvent   `json:"events"`
	HistoryVisibility xtools.HistoryVisibility `json:"history_visibility"`
}

type PerformPublishRequest struct {
	FrameID       string
	Visibility   string
	AppserviceID string
	NetworkID    string
}

type PerformInboundPeekRequest struct {
	UserID          string          `json:"user_id"`
	FrameID          string          `json:"frame_id"`
	PeekID          string          `json:"peek_id"`
	ServerName      spec.ServerName `json:"server_name"`
	RenewalInterval int64           `json:"renewal_interval"`
}

type PerformInboundPeekResponse struct {
	// Does the frame exist on this dataframe?
	// If the frame doesn't exist this will be false and StateEvents will be empty.
	FrameExists bool `json:"frame_exists"`
	// The frame version of the frame.
	FrameVersion xtools.FrameVersion `json:"frame_version"`
	// The current state and auth chain events.
	// The lists will be in an arbitrary order.
	StateEvents     []*types.HeaderedEvent `json:"state_events"`
	AuthChainEvents []*types.HeaderedEvent `json:"auth_chain_events"`
	// The event at which this state was captured
	LatestEvent *types.HeaderedEvent `json:"latest_event"`
}

// PerformForgetRequest is a request to PerformForget
type PerformForgetRequest struct {
	FrameID string `json:"frame_id"`
	UserID string `json:"user_id"`
}

type PerformForgetResponse struct{}
