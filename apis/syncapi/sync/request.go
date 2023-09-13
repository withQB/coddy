package sync

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	userapi "github.com/withqb/coddy/apis/userapi/api"
)

const defaultSyncTimeout = time.Duration(0)
const DefaultTimelineLimit = 20

func newSyncRequest(req *http.Request, device userapi.Device, syncDB storage.Database) (*types.SyncRequest, error) {
	timeout := getTimeout(req.URL.Query().Get("timeout"))
	fullState := req.URL.Query().Get("full_state")
	wantFullState := fullState != "" && fullState != "false"
	since, sinceStr := types.StreamingToken{}, req.URL.Query().Get("since")
	if sinceStr != "" {
		var err error
		since, err = types.NewStreamTokenFromString(sinceStr)
		if err != nil {
			return nil, err
		}
	}

	// Create a default filter and apply a stored filter on top of it (if specified)
	filter := synctypes.DefaultFilter()
	filterQuery := req.URL.Query().Get("filter")
	if filterQuery != "" {
		if filterQuery[0] == '{' {
			// Parse the filter from the query string
			if err := json.Unmarshal([]byte(filterQuery), &filter); err != nil {
				return nil, fmt.Errorf("json.Unmarshal: %w", err)
			}
		} else {
			// Try to load the filter from the database
			localpart, _, err := xtools.SplitID('@', device.UserID)
			if err != nil {
				xutil.GetLogger(req.Context()).WithError(err).Error("xtools.SplitID failed")
				return nil, fmt.Errorf("xtools.SplitID: %w", err)
			}
			if err := syncDB.GetFilter(req.Context(), &filter, localpart, filterQuery); err != nil && err != sql.ErrNoRows {
				xutil.GetLogger(req.Context()).WithError(err).Error("syncDB.GetFilter failed")
				return nil, fmt.Errorf("syncDB.GetFilter: %w", err)
			}
		}
	}

	// A loaded filter might have overwritten these values,
	// so set them after loading the filter.
	if since.IsEmpty() {
		// Send as much account data down for complete syncs as possible
		// by default, otherwise clients do weird things while waiting
		// for the rest of the data to trickle down.
		filter.AccountData.Limit = math.MaxInt32
		filter.Room.AccountData.Limit = math.MaxInt32
	}

	logger := xutil.GetLogger(req.Context()).WithFields(logrus.Fields{
		"user_id":   device.UserID,
		"device_id": device.ID,
		"since":     since,
		"timeout":   timeout,
		"limit":     filter.Room.Timeline.Limit,
	})

	return &types.SyncRequest{
		Context:           req.Context(),             //
		Log:               logger,                    //
		Device:            &device,                   //
		Response:          types.NewResponse(),       // Populated by all streams
		Filter:            filter,                    //
		Since:             since,                     //
		Timeout:           timeout,                   //
		Rooms:             make(map[string]string),   // Populated by the PDU stream
		WantFullState:     wantFullState,             //
		MembershipChanges: make(map[string]struct{}), // Populated by the PDU stream
	}, nil
}

func getTimeout(timeoutMS string) time.Duration {
	if timeoutMS == "" {
		return defaultSyncTimeout
	}
	i, err := strconv.Atoi(timeoutMS)
	if err != nil {
		return defaultSyncTimeout
	}
	return time.Duration(i) * time.Millisecond
}
