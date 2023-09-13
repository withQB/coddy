package authtypes

// Flow represents one possible way that the client can authenticate a request.
type Flow struct {
	Stages []LoginType `json:"stages"`
}