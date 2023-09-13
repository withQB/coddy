package api

import "github.com/withqb/xtools/fclient"

// ExtraPublicFramesProvider provides a way to inject extra published frames into /publicFrames requests.
type ExtraPublicFramesProvider interface {
	// Frames returns the extra frames. This is called on-demand by clients, so cache appropriately.
	Frames() []fclient.PublicFrame
}

type RegistrationToken struct {
	Token       *string `json:"token"`
	UsesAllowed *int32  `json:"uses_allowed"`
	Pending     *int32  `json:"pending"`
	Completed   *int32  `json:"completed"`
	ExpiryTime  *int64  `json:"expiry_time"`
}
