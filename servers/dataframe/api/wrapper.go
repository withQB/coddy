package api

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// SendEvents to the dataframe The events are written with KindNew.
func SendEvents(
	ctx context.Context, rsAPI InputFrameEventsAPI,
	kind Kind, events []*types.HeaderedEvent,
	virtualHost, origin spec.ServerName,
	sendAsServer spec.ServerName, txnID *TransactionID,
	async bool,
) error {
	ires := make([]InputFrameEvent, len(events))
	for i, event := range events {
		ires[i] = InputFrameEvent{
			Kind:          kind,
			Event:         event,
			Origin:        origin,
			SendAsServer:  string(sendAsServer),
			TransactionID: txnID,
		}
	}
	return SendInputFrameEvents(ctx, rsAPI, virtualHost, ires, async)
}

// SendEventWithState writes an event with the specified kind to the dataframe
// with the state at the event as KindOutlier before it. Will not send any event that is
// marked as `true` in haveEventIDs.
func SendEventWithState(
	ctx context.Context, rsAPI InputFrameEventsAPI,
	virtualHost spec.ServerName, kind Kind,
	state xtools.StateResponse, event *types.HeaderedEvent,
	origin spec.ServerName, haveEventIDs map[string]bool, async bool,
) error {
	outliers := xtools.LineariseStateResponse(event.Version(), state)
	ires := make([]InputFrameEvent, 0, len(outliers))
	for _, outlier := range outliers {
		if haveEventIDs[outlier.EventID()] {
			continue
		}
		ires = append(ires, InputFrameEvent{
			Kind:   KindOutlier,
			Event:  &types.HeaderedEvent{PDU: outlier},
			Origin: origin,
		})
	}

	stateEvents := state.GetStateEvents().UntrustedEvents(event.Version())
	stateEventIDs := make([]string, len(stateEvents))
	for i := range stateEvents {
		stateEventIDs[i] = stateEvents[i].EventID()
	}

	logrus.WithContext(ctx).WithFields(logrus.Fields{
		"frame_id":   event.FrameID(),
		"event_id":  event.EventID(),
		"outliers":  len(ires),
		"state_ids": len(stateEventIDs),
	}).Infof("Submitting %q event to dataframe with state snapshot", event.Type())

	ires = append(ires, InputFrameEvent{
		Kind:          kind,
		Event:         event,
		Origin:        origin,
		HasState:      true,
		StateEventIDs: stateEventIDs,
	})

	return SendInputFrameEvents(ctx, rsAPI, virtualHost, ires, async)
}

// SendInputFrameEvents to the dataframe.
func SendInputFrameEvents(
	ctx context.Context, rsAPI InputFrameEventsAPI,
	virtualHost spec.ServerName,
	ires []InputFrameEvent, async bool,
) error {
	request := InputFrameEventsRequest{
		InputFrameEvents: ires,
		Asynchronous:    async,
		VirtualHost:     virtualHost,
	}
	var response InputFrameEventsResponse
	rsAPI.InputFrameEvents(ctx, &request, &response)
	return response.Err()
}

// GetEvent returns the event or nil, even on errors.
func GetEvent(ctx context.Context, rsAPI QueryEventsAPI, frameID, eventID string) *types.HeaderedEvent {
	var res QueryEventsByIDResponse
	err := rsAPI.QueryEventsByID(ctx, &QueryEventsByIDRequest{
		FrameID:   frameID,
		EventIDs: []string{eventID},
	}, &res)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryEventsByID")
		return nil
	}
	if len(res.Events) != 1 {
		return nil
	}
	return res.Events[0]
}

// GetStateEvent returns the current state event in the frame or nil.
func GetStateEvent(ctx context.Context, rsAPI QueryEventsAPI, frameID string, tuple xtools.StateKeyTuple) *types.HeaderedEvent {
	var res QueryCurrentStateResponse
	err := rsAPI.QueryCurrentState(ctx, &QueryCurrentStateRequest{
		FrameID:      frameID,
		StateTuples: []xtools.StateKeyTuple{tuple},
	}, &res)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryCurrentState")
		return nil
	}
	ev, ok := res.StateEvents[tuple]
	if ok {
		return ev
	}
	return nil
}

// IsServerBannedFromFrame returns whether the server is banned from a frame by server ACLs.
func IsServerBannedFromFrame(ctx context.Context, rsAPI FederationDataframeAPI, frameID string, serverName spec.ServerName) bool {
	req := &QueryServerBannedFromFrameRequest{
		ServerName: serverName,
		FrameID:     frameID,
	}
	res := &QueryServerBannedFromFrameResponse{}
	if err := rsAPI.QueryServerBannedFromFrame(ctx, req, res); err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("Failed to QueryServerBannedFromFrame")
		return true
	}
	return res.Banned
}

// PopulatePublicFrames extracts PublicFrame information for all the provided frame IDs. The IDs are not checked to see if they are visible in the
// published frame directory.
// due to lots of switches
func PopulatePublicFrames(ctx context.Context, frameIDs []string, rsAPI QueryBulkStateContentAPI) ([]fclient.PublicFrame, error) {
	avatarTuple := xtools.StateKeyTuple{EventType: "m.frame.avatar", StateKey: ""}
	nameTuple := xtools.StateKeyTuple{EventType: "m.frame.name", StateKey: ""}
	canonicalTuple := xtools.StateKeyTuple{EventType: spec.MFrameCanonicalAlias, StateKey: ""}
	topicTuple := xtools.StateKeyTuple{EventType: "m.frame.topic", StateKey: ""}
	guestTuple := xtools.StateKeyTuple{EventType: "m.frame.guest_access", StateKey: ""}
	visibilityTuple := xtools.StateKeyTuple{EventType: spec.MFrameHistoryVisibility, StateKey: ""}
	joinRuleTuple := xtools.StateKeyTuple{EventType: spec.MFrameJoinRules, StateKey: ""}

	var stateRes QueryBulkStateContentResponse
	err := rsAPI.QueryBulkStateContent(ctx, &QueryBulkStateContentRequest{
		FrameIDs:        frameIDs,
		AllowWildcards: true,
		StateTuples: []xtools.StateKeyTuple{
			nameTuple, canonicalTuple, topicTuple, guestTuple, visibilityTuple, joinRuleTuple, avatarTuple,
			{EventType: spec.MFrameMember, StateKey: "*"},
		},
	}, &stateRes)
	if err != nil {
		xutil.GetLogger(ctx).WithError(err).Error("QueryBulkStateContent failed")
		return nil, err
	}
	chunk := make([]fclient.PublicFrame, len(frameIDs))
	i := 0
	for frameID, data := range stateRes.Frames {
		pub := fclient.PublicFrame{
			FrameID: frameID,
		}
		joinCount := 0
		var joinRule, guestAccess string
		for tuple, contentVal := range data {
			if tuple.EventType == spec.MFrameMember && contentVal == "join" {
				joinCount++
				continue
			}
			switch tuple {
			case avatarTuple:
				pub.AvatarURL = contentVal
			case nameTuple:
				pub.Name = contentVal
			case topicTuple:
				pub.Topic = contentVal
			case canonicalTuple:
				if _, _, err := xtools.SplitID('#', contentVal); err == nil {
					pub.CanonicalAlias = contentVal
				}
			case visibilityTuple:
				pub.WorldReadable = contentVal == "world_readable"
			// need both of these to determine whether guests can join
			case joinRuleTuple:
				joinRule = contentVal
			case guestTuple:
				guestAccess = contentVal
			}
		}
		if joinRule == spec.Public && guestAccess == "can_join" {
			pub.GuestCanJoin = true
		}
		pub.JoinedMembersCount = joinCount
		chunk[i] = pub
		i++
	}
	return chunk, nil
}
