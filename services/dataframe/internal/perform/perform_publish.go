package perform

import (
	"context"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/storage"
)

type Publisher struct {
	DB storage.Database
}

// PerformPublish publishes or unpublishes a frame from the frame directory. Returns a database error, if any.
func (r *Publisher) PerformPublish(
	ctx context.Context,
	req *api.PerformPublishRequest,
) error {
	return r.DB.PublishFrame(ctx, req.FrameID, req.AppserviceID, req.NetworkID, req.Visibility == "public")
}
