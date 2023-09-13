package helpers

import (
	"context"
	"fmt"
	"sort"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/state"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
)

// CheckForSoftFail returns true if the event should be soft-failed
// and false otherwise. The return error value should be checked before
// the soft-fail bool.
func CheckForSoftFail(
	ctx context.Context,
	db storage.FrameDatabase,
	frameInfo *types.FrameInfo,
	event *types.HeaderedEvent,
	stateEventIDs []string,
	querier api.QuerySenderIDAPI,
) (bool, error) {
	rewritesState := len(stateEventIDs) > 1

	var authStateEntries []types.StateEntry
	var err error
	if rewritesState {
		authStateEntries, err = db.StateEntriesForEventIDs(ctx, stateEventIDs, true)
		if err != nil {
			return true, fmt.Errorf("StateEntriesForEventIDs failed: %w", err)
		}
	} else {
		// Then get the state entries for the current state snapshot.
		// We'll use this to check if the event is allowed right now.
		frameState := state.NewStateResolution(db, frameInfo, querier)
		authStateEntries, err = frameState.LoadStateAtSnapshot(ctx, frameInfo.StateSnapshotNID())
		if err != nil {
			return true, fmt.Errorf("frameState.LoadStateAtSnapshot: %w", err)
		}
	}

	// As a special case, it's possible that the frame will have no
	// state because we haven't received a m.frame.create event yet.
	// If we're now processing the first create event then never
	// soft-fail it.
	if len(authStateEntries) == 0 && event.Type() == spec.MFrameCreate {
		return false, nil
	}

	// Work out which of the state events we actually need.
	stateNeeded := xtools.StateNeededForAuth(
		[]xtools.PDU{event.PDU},
	)

	// Load the actual auth events from the database.
	authEvents, err := loadAuthEvents(ctx, db, frameInfo.FrameVersion, stateNeeded, authStateEntries)
	if err != nil {
		return true, fmt.Errorf("loadAuthEvents: %w", err)
	}

	// Check if the event is allowed.
	if err = xtools.Allowed(event.PDU, &authEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return querier.QueryUserIDForSender(ctx, frameID, senderID)
	}); err != nil {
		// return true, nil
		return true, err
	}
	return false, nil
}

// GetAuthEvents returns the numeric IDs for the auth events.
func GetAuthEvents(
	ctx context.Context,
	db storage.FrameDatabase,
	frameVersion xtools.FrameVersion,
	event xtools.PDU,
	authEventIDs []string,
) (xtools.AuthEventProvider, error) {
	// Grab the numeric IDs for the supplied auth state events from the database.
	authStateEntries, err := db.StateEntriesForEventIDs(ctx, authEventIDs, true)
	if err != nil {
		return nil, fmt.Errorf("db.StateEntriesForEventIDs: %w", err)
	}
	authStateEntries = types.DeduplicateStateEntries(authStateEntries)

	// Work out which of the state events we actually need.
	stateNeeded := xtools.StateNeededForAuth([]xtools.PDU{event})

	// Load the actual auth events from the database.
	authEvents, err := loadAuthEvents(ctx, db, frameVersion, stateNeeded, authStateEntries)
	if err != nil {
		return nil, fmt.Errorf("loadAuthEvents: %w", err)
	}
	return &authEvents, nil
}

type authEvents struct {
	stateKeyNIDMap map[string]types.EventStateKeyNID
	state          stateEntryMap
	events         EventMap
	valid          bool
}

// Valid verifies that all auth events are from the same frame.
func (ae *authEvents) Valid() bool {
	return ae.valid
}

// Create implements xtools.AuthEventProvider
func (ae *authEvents) Create() (xtools.PDU, error) {
	return ae.lookupEventWithEmptyStateKey(types.MFrameCreateNID), nil
}

// PowerLevels implements xtools.AuthEventProvider
func (ae *authEvents) PowerLevels() (xtools.PDU, error) {
	return ae.lookupEventWithEmptyStateKey(types.MFramePowerLevelsNID), nil
}

// JoinRules implements xtools.AuthEventProvider
func (ae *authEvents) JoinRules() (xtools.PDU, error) {
	return ae.lookupEventWithEmptyStateKey(types.MFrameJoinRulesNID), nil
}

// Memmber implements xtools.AuthEventProvider
func (ae *authEvents) Member(stateKey spec.SenderID) (xtools.PDU, error) {
	return ae.lookupEvent(types.MFrameMemberNID, string(stateKey)), nil
}

// ThirdPartyInvite implements xtools.AuthEventProvider
func (ae *authEvents) ThirdPartyInvite(stateKey string) (xtools.PDU, error) {
	return ae.lookupEvent(types.MFrameThirdPartyInviteNID, stateKey), nil
}

func (ae *authEvents) lookupEventWithEmptyStateKey(typeNID types.EventTypeNID) xtools.PDU {
	eventNID, ok := ae.state.lookup(types.StateKeyTuple{
		EventTypeNID:     typeNID,
		EventStateKeyNID: types.EmptyStateKeyNID,
	})
	if !ok {
		return nil
	}
	event, ok := ae.events.Lookup(eventNID)
	if !ok {
		return nil
	}
	return event.PDU
}

func (ae *authEvents) lookupEvent(typeNID types.EventTypeNID, stateKey string) xtools.PDU {
	stateKeyNID, ok := ae.stateKeyNIDMap[stateKey]
	if !ok {
		return nil
	}
	eventNID, ok := ae.state.lookup(types.StateKeyTuple{
		EventTypeNID:     typeNID,
		EventStateKeyNID: stateKeyNID,
	})
	if !ok {
		return nil
	}
	event, ok := ae.events.Lookup(eventNID)
	if !ok {
		return nil
	}
	return event.PDU
}

// loadAuthEvents loads the events needed for authentication from the supplied frame state.
func loadAuthEvents(
	ctx context.Context,
	db state.StateResolutionStorage,
	frameVersion xtools.FrameVersion,
	needed xtools.StateNeeded,
	state []types.StateEntry,
) (result authEvents, err error) {
	result.valid = true
	// Look up the numeric IDs for the state keys needed for auth.
	var neededStateKeys []string
	neededStateKeys = append(neededStateKeys, needed.Member...)
	neededStateKeys = append(neededStateKeys, needed.ThirdPartyInvite...)
	if result.stateKeyNIDMap, err = db.EventStateKeyNIDs(ctx, neededStateKeys); err != nil {
		return
	}

	// Load the events we need.
	result.state = state
	var eventNIDs []types.EventNID
	keyTuplesNeeded := stateKeyTuplesNeeded(result.stateKeyNIDMap, needed)
	for _, keyTuple := range keyTuplesNeeded {
		eventNID, ok := result.state.lookup(keyTuple)
		if ok {
			eventNIDs = append(eventNIDs, eventNID)
		}
	}

	if result.events, err = db.Events(ctx, frameVersion, eventNIDs); err != nil {
		return
	}
	frameID := ""
	for _, ev := range result.events {
		if frameID == "" {
			frameID = ev.FrameID()
		}
		if ev.FrameID() != frameID {
			result.valid = false
			break
		}
	}
	return
}

// stateKeyTuplesNeeded works out which numeric state key tuples we need to authenticate some events.
func stateKeyTuplesNeeded(
	stateKeyNIDMap map[string]types.EventStateKeyNID,
	stateNeeded xtools.StateNeeded,
) []types.StateKeyTuple {
	var keyTuples []types.StateKeyTuple
	if stateNeeded.Create {
		keyTuples = append(keyTuples, types.StateKeyTuple{
			EventTypeNID:     types.MFrameCreateNID,
			EventStateKeyNID: types.EmptyStateKeyNID,
		})
	}
	if stateNeeded.PowerLevels {
		keyTuples = append(keyTuples, types.StateKeyTuple{
			EventTypeNID:     types.MFramePowerLevelsNID,
			EventStateKeyNID: types.EmptyStateKeyNID,
		})
	}
	if stateNeeded.JoinRules {
		keyTuples = append(keyTuples, types.StateKeyTuple{
			EventTypeNID:     types.MFrameJoinRulesNID,
			EventStateKeyNID: types.EmptyStateKeyNID,
		})
	}
	for _, member := range stateNeeded.Member {
		stateKeyNID, ok := stateKeyNIDMap[member]
		if ok {
			keyTuples = append(keyTuples, types.StateKeyTuple{
				EventTypeNID:     types.MFrameMemberNID,
				EventStateKeyNID: stateKeyNID,
			})
		}
	}
	for _, token := range stateNeeded.ThirdPartyInvite {
		stateKeyNID, ok := stateKeyNIDMap[token]
		if ok {
			keyTuples = append(keyTuples, types.StateKeyTuple{
				EventTypeNID:     types.MFrameThirdPartyInviteNID,
				EventStateKeyNID: stateKeyNID,
			})
		}
	}
	return keyTuples
}

// Map from event type, state key tuple to numeric event ID.
// Implemented using binary search on a sorted array.
type stateEntryMap []types.StateEntry

// lookup an entry in the event map.
func (m stateEntryMap) lookup(stateKey types.StateKeyTuple) (eventNID types.EventNID, ok bool) {
	// Since the list is sorted we can implement this using binary search.
	// This is faster than using a hash map.
	// We don't have to worry about pathological cases because the keys are fixed
	// size and are controlled by us.
	list := []types.StateEntry(m)
	i := sort.Search(len(list), func(i int) bool {
		return !list[i].StateKeyTuple.LessThan(stateKey)
	})
	if i < len(list) && list[i].StateKeyTuple == stateKey {
		ok = true
		eventNID = list[i].EventNID
	}
	return
}

// Map from numeric event ID to event.
// Implemented using binary search on a sorted array.
type EventMap []types.Event

// lookup an entry in the event map.
func (m EventMap) Lookup(eventNID types.EventNID) (event *types.Event, ok bool) {
	// Since the list is sorted we can implement this using binary search.
	// This is faster than using a hash map.
	// We don't have to worry about pathological cases because the keys are fixed
	// size are controlled by us.
	list := []types.Event(m)
	i := sort.Search(len(list), func(i int) bool {
		return list[i].EventNID >= eventNID
	})
	if i < len(list) && list[i].EventNID == eventNID {
		ok = true
		event = &list[i]
	}
	return
}
