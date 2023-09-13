package perform

import (
	"context"

	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/storage"
)

type Forgetter struct {
	DB storage.Database
}

// PerformForget implements api.RoomServerQueryAPI
func (f *Forgetter) PerformForget(
	ctx context.Context,
	request *api.PerformForgetRequest,
	response *api.PerformForgetResponse,
) error {
	return f.DB.ForgetRoom(ctx, request.UserID, request.RoomID, true)
}
