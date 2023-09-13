package perform

import (
	"context"

	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/storage"
)

type Forgetter struct {
	DB storage.Database
}

// PerformForget implements api.DataFrameQueryAPI
func (f *Forgetter) PerformForget(
	ctx context.Context,
	request *api.PerformForgetRequest,
	response *api.PerformForgetResponse,
) error {
	return f.DB.ForgetFrame(ctx, request.UserID, request.FrameID, true)
}
