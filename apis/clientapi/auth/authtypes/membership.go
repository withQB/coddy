package authtypes

// Membership represents the relationship between a user and a frame they're a
// member of
type Membership struct {
	Localpart string
	FrameID    string
	EventID   string
}
