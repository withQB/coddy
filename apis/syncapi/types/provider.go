package types

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/withqb/coddy/apis/syncapi/synctypes"
	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/xtools/spec"
)

type SyncRequest struct {
	Context       context.Context
	Log           *logrus.Entry
	Device        *userapi.Device
	Response      *Response
	Filter        synctypes.Filter
	Since         StreamingToken
	Timeout       time.Duration
	WantFullState bool

	// Updated by the PDU stream.
	Frames map[string]string
	// Updated by the PDU stream.
	MembershipChanges map[string]struct{}
	// Updated by the PDU stream.
	IgnoredUsers IgnoredUsers
}

func (r *SyncRequest) IsFramePresent(frameID string) bool {
	membership, ok := r.Frames[frameID]
	if !ok {
		return false
	}
	switch membership {
	case spec.Join:
		return true
	case spec.Invite:
		return true
	case spec.Peek:
		return true
	default:
		return false
	}
}
