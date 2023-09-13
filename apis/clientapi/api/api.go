package api

import "github.com/withqb/xtools/fclient"

// ExtraPublicRoomsProvider provides a way to inject extra published rooms into /publicRooms requests.
type ExtraPublicRoomsProvider interface {
	// Rooms returns the extra rooms. This is called on-demand by clients, so cache appropriately.
	Rooms() []fclient.PublicRoom
}

type RegistrationToken struct {
	Token       *string `json:"token"`
	UsesAllowed *int32  `json:"uses_allowed"`
	Pending     *int32  `json:"pending"`
	Completed   *int32  `json:"completed"`
	ExpiryTime  *int64  `json:"expiry_time"`
}
