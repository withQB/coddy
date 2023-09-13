package authtypes

// Profile represents the profile for a Matrix account.
type Profile struct {
	Localpart   string `json:"local_part"`
	ServerName  string `json:"server_name,omitempty"` // NOTSPEC: only set by Pinecone user provider
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
}

// FullyQualifiedProfile represents the profile for a Matrix account.
type FullyQualifiedProfile struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
}
