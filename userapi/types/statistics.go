package types

type UserStatistics struct {
	RegisteredUsersByType map[string]int64
	R30Users              map[string]int64
	R30UsersV2            map[string]int64
	AllUsers              int64
	NonBridgedUsers       int64
	DailyUsers            int64
	MonthlyUsers          int64
}

type DatabaseEngine struct {
	Engine  string
	Version string
}

type MessageStats struct {
	Messages         int64
	SentMessages     int64
	MessagesE2EE     int64
	SentMessagesE2EE int64
}
