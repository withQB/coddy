package eventutil

import "github.com/withqb/xtools"

// NameContent is the event content
type NameContent struct {
	Name string `json:"name"`
}

// TopicContent is the event content
type TopicContent struct {
	Topic string `json:"topic"`
}

// GuestAccessContent is the event content
type GuestAccessContent struct {
	GuestAccess string `json:"guest_access"`
}

// HistoryVisibilityContent is the event content
type HistoryVisibilityContent struct {
	HistoryVisibility string `json:"history_visibility"`
}

// CanonicalAlias is the event content
type CanonicalAlias struct {
	Alias string `json:"alias"`
}

// InitialPowerLevelsContent returns the initial values for m.frame.power_levels on frame creation
// if they have not been specified.
func InitialPowerLevelsContent(frameCreator string) (c xtools.PowerLevelContent) {
	c.Defaults()
	c.Events = map[string]int64{
		"m.frame.name":               50,
		"m.frame.power_levels":       100,
		"m.frame.history_visibility": 100,
		"m.frame.canonical_alias":    50,
		"m.frame.avatar":             50,
		"m.frame.tombstone":          100,
		"m.frame.encryption":         100,
		"m.frame.server_acl":         100,
	}
	c.Users = map[string]int64{frameCreator: 100}
	return c
}

// AliasesContent is the event content
type AliasesContent struct {
	Aliases []string `json:"aliases"`
}

// CanonicalAliasContent is the event content
type CanonicalAliasContent struct {
	Alias string `json:"alias"`
}

// AvatarContent is the event content
type AvatarContent struct {
	Info          ImageInfo `json:"info,omitempty"`
	URL           string    `json:"url"`
	ThumbnailURL  string    `json:"thumbnail_url,omitempty"`
	ThumbnailInfo ImageInfo `json:"thumbnail_info,omitempty"`
}

// ImageInfo implements the ImageInfo structure
type ImageInfo struct {
	Mimetype string `json:"mimetype"`
	Height   int64  `json:"h"`
	Width    int64  `json:"w"`
	Size     int64  `json:"size"`
}
