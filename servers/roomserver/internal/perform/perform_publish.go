package perform

import (
	"context"

	"github.com/withqb/coddy/servers/roomserver/api"
	"github.com/withqb/coddy/servers/roomserver/storage"
)

type Publisher struct {
	DB storage.Database
}

// PerformPublish publishes or unpublishes a room from the room directory. Returns a database error, if any.
func (r *Publisher) PerformPublish(
	ctx context.Context,
	req *api.PerformPublishRequest,
) error {
	return r.DB.PublishRoom(ctx, req.RoomID, req.AppserviceID, req.NetworkID, req.Visibility == "public")
}
