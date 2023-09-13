package api

import (
	"regexp"
)

// GetFrameIDForAliasRequest is a request to GetFrameIDForAlias
type GetFrameIDForAliasRequest struct {
	// Alias we want to lookup
	Alias string `json:"alias"`
	// Should we ask appservices for their aliases as a part of
	// the request?
	IncludeAppservices bool `json:"include_appservices"`
}

// GetFrameIDForAliasResponse is a response to GetFrameIDForAlias
type GetFrameIDForAliasResponse struct {
	// The frame ID the alias refers to
	FrameID string `json:"frame_id"`
}

// GetAliasesForFrameIDRequest is a request to GetAliasesForFrameID
type GetAliasesForFrameIDRequest struct {
	// The frame ID we want to find aliases for
	FrameID string `json:"frame_id"`
}

// GetAliasesForFrameIDResponse is a response to GetAliasesForFrameID
type GetAliasesForFrameIDResponse struct {
	// The aliases the alias refers to
	Aliases []string `json:"aliases"`
}

type AliasEvent struct {
	Alias      string   `json:"alias"`
	AltAliases []string `json:"alt_aliases"`
}

var validateAliasRegex = regexp.MustCompile("^#.*:.+$")

func (a AliasEvent) Valid() bool {
	for _, alias := range a.AltAliases {
		if !validateAliasRegex.MatchString(alias) {
			return false
		}
	}
	return a.Alias == "" || validateAliasRegex.MatchString(a.Alias)
}
