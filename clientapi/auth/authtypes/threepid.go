package authtypes

// ThreePID represents a third-party identifier
type ThreePID struct {
	Address     string `json:"address"`
	Medium      string `json:"medium"`
	AddedAt     int64  `json:"added_at"`
	ValidatedAt int64  `json:"validated_at"`
}
