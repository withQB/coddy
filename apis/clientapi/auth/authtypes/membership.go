package authtypes

// Membership represents the relationship between a user and a room they're a
// member of
type Membership struct {
	Localpart string
	RoomID    string
	EventID   string
}
