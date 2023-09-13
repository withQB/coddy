package types

import "github.com/withqb/xtools/spec"

const MSigningKeyUpdate = "m.signing_key_update" // TDO: move to xtools

// A JoinedHost is a server that is joined to a coddy frame.
type JoinedHost struct {
	// The MemberEventID of a m.frame.member join event.
	MemberEventID string
	// The domain part of the state key of the m.frame.member join event
	ServerName spec.ServerName
}

type ServerNames []spec.ServerName

func (s ServerNames) Len() int           { return len(s) }
func (s ServerNames) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ServerNames) Less(i, j int) bool { return s[i] < s[j] }

// tracks peeks we're performing on another server over federation
type OutboundPeek struct {
	PeekID            string
	FrameID            string
	ServerName        spec.ServerName
	CreationTimestamp int64
	RenewedTimestamp  int64
	RenewalInterval   int64
}

// tracks peeks other servers are performing on us over federation
type InboundPeek struct {
	PeekID            string
	FrameID            string
	ServerName        spec.ServerName
	CreationTimestamp int64
	RenewedTimestamp  int64
	RenewalInterval   int64
}

type FederationReceiptMRead struct {
	User map[string]FederationReceiptData `json:"m.read"`
}

type FederationReceiptData struct {
	Data     ReceiptTS `json:"data"`
	EventIDs []string  `json:"event_ids"`
}

type ReceiptTS struct {
	TS spec.Timestamp `json:"ts"`
}

type Presence struct {
	Push []PresenceContent `json:"push"`
}

type PresenceContent struct {
	CurrentlyActive bool    `json:"currently_active,omitempty"`
	LastActiveAgo   int64   `json:"last_active_ago"`
	Presence        string  `json:"presence"`
	StatusMsg       *string `json:"status_msg,omitempty"`
	UserID          string  `json:"user_id"`
}
