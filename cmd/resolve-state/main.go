package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe"
	"github.com/withqb/coddy/servers/dataframe/state"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// This is a utility for inspecting state snapshots and running state resolution
// against real snapshots in an actual database.
// It takes one or more state snapshot NIDs as arguments, along with a frame version
// to use for unmarshalling events, and will produce resolved output.
//
// Usage: ./resolve-state --frameversion=version snapshot [snapshot ...]
//   e.g. ./resolve-state --frameversion=5 1254 1235 1282

var frameVersion = flag.String("frameversion", "5", "the frame version to parse events as")
var filterType = flag.String("filtertype", "", "the event types to filter on")
var difference = flag.Bool("difference", false, "whether to calculate the difference between snapshots")

// nolint:gocyclo
func main() {
	ctx := context.Background()
	cfg := setup.ParseFlags(true)
	cfg.Logging = append(cfg.Logging[:0], config.LogrusHook{
		Type:  "std",
		Level: "error",
	})
	cfg.ClientAPI.RegistrationDisabled = true

	args := flag.Args()

	fmt.Println("Frame version", *frameVersion)

	snapshotNIDs := []types.StateSnapshotNID{}
	for _, arg := range args {
		if i, err := strconv.Atoi(arg); err == nil {
			snapshotNIDs = append(snapshotNIDs, types.StateSnapshotNID(i))
		}
	}

	fmt.Println("Fetching", len(snapshotNIDs), "snapshot NIDs")

	processCtx := process.NewProcessContext()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	dataframeDB, err := storage.Open(
		processCtx.Context(), cm, &cfg.DataFrame.Database,
		caching.NewRistrettoCache(128*1024*1024, time.Hour, true),
	)
	if err != nil {
		panic(err)
	}

	natsInstance := &jetstream.NATSInstance{}
	rsAPI := dataframe.NewInternalAPI(processCtx, cfg, cm,
		natsInstance, caching.NewRistrettoCache(128*1024*1024, time.Hour, true), false)

	frameInfo := &types.FrameInfo{
		FrameVersion: xtools.FrameVersion(*frameVersion),
	}
	stateres := state.NewStateResolution(dataframeDB, frameInfo, rsAPI)

	if *difference {
		if len(snapshotNIDs) != 2 {
			panic("need exactly two state snapshot NIDs to calculate difference")
		}
		var removed, added []types.StateEntry
		removed, added, err = stateres.DifferenceBetweeenStateSnapshots(ctx, snapshotNIDs[0], snapshotNIDs[1])
		if err != nil {
			panic(err)
		}

		eventNIDMap := map[types.EventNID]struct{}{}
		for _, entry := range append(removed, added...) {
			eventNIDMap[entry.EventNID] = struct{}{}
		}

		eventNIDs := make([]types.EventNID, 0, len(eventNIDMap))
		for eventNID := range eventNIDMap {
			eventNIDs = append(eventNIDs, eventNID)
		}

		var eventEntries []types.Event
		eventEntries, err = dataframeDB.Events(ctx, frameInfo.FrameVersion, eventNIDs)
		if err != nil {
			panic(err)
		}

		events := make(map[types.EventNID]xtools.PDU, len(eventEntries))
		for _, entry := range eventEntries {
			events[entry.EventNID] = entry.PDU
		}

		if len(removed) > 0 {
			fmt.Println("Removed:")
			for _, r := range removed {
				event := events[r.EventNID]
				fmt.Println()
				fmt.Printf("* %s %s %q\n", event.EventID(), event.Type(), *event.StateKey())
				fmt.Printf("  %s\n", string(event.Content()))
			}
		}

		if len(removed) > 0 && len(added) > 0 {
			fmt.Println()
		}

		if len(added) > 0 {
			fmt.Println("Added:")
			for _, a := range added {
				event := events[a.EventNID]
				fmt.Println()
				fmt.Printf("* %s %s %q\n", event.EventID(), event.Type(), *event.StateKey())
				fmt.Printf("  %s\n", string(event.Content()))
			}
		}

		return
	}

	var stateEntries []types.StateEntry
	for _, snapshotNID := range snapshotNIDs {
		var entries []types.StateEntry
		entries, err = stateres.LoadStateAtSnapshot(ctx, snapshotNID)
		if err != nil {
			panic(err)
		}
		stateEntries = append(stateEntries, entries...)
	}

	eventNIDMap := map[types.EventNID]struct{}{}
	for _, entry := range stateEntries {
		eventNIDMap[entry.EventNID] = struct{}{}
	}

	eventNIDs := make([]types.EventNID, 0, len(eventNIDMap))
	for eventNID := range eventNIDMap {
		eventNIDs = append(eventNIDs, eventNID)
	}

	fmt.Println("Fetching", len(eventNIDMap), "state events")
	eventEntries, err := dataframeDB.Events(ctx, frameInfo.FrameVersion, eventNIDs)
	if err != nil {
		panic(err)
	}

	authEventIDMap := make(map[string]struct{})
	events := make([]xtools.PDU, len(eventEntries))
	for i := range eventEntries {
		events[i] = eventEntries[i].PDU
		for _, authEventID := range eventEntries[i].AuthEventIDs() {
			authEventIDMap[authEventID] = struct{}{}
		}
	}

	authEventIDs := make([]string, 0, len(authEventIDMap))
	for authEventID := range authEventIDMap {
		authEventIDs = append(authEventIDs, authEventID)
	}

	fmt.Println("Fetching", len(authEventIDs), "auth events")
	authEventEntries, err := dataframeDB.EventsFromIDs(ctx, frameInfo, authEventIDs)
	if err != nil {
		panic(err)
	}

	authEvents := make([]xtools.PDU, len(authEventEntries))
	for i := range authEventEntries {
		authEvents[i] = authEventEntries[i].PDU
	}

	fmt.Println("Resolving state")
	var resolved Events
	resolved, err = xtools.ResolveConflicts(
		xtools.FrameVersion(*frameVersion), events, authEvents, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		},
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Resolved state contains", len(resolved), "events")
	sort.Sort(resolved)
	filteringEventType := *filterType
	count := 0
	for _, event := range resolved {
		if filteringEventType != "" && event.Type() != filteringEventType {
			continue
		}
		count++
		fmt.Println()
		fmt.Printf("* %s %s %q\n", event.EventID(), event.Type(), *event.StateKey())
		fmt.Printf("  %s\n", string(event.Content()))
	}

	fmt.Println()
	fmt.Println("Returned", count, "state events after filtering")
}

type Events []xtools.PDU

func (e Events) Len() int {
	return len(e)
}

func (e Events) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e Events) Less(i, j int) bool {
	typeDelta := strings.Compare(e[i].Type(), e[j].Type())
	if typeDelta < 0 {
		return true
	}
	if typeDelta > 0 {
		return false
	}
	stateKeyDelta := strings.Compare(*e[i].StateKey(), *e[j].StateKey())
	return stateKeyDelta < 0
}
