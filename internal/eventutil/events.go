package eventutil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/xtools"
)

// ErrFrameNoExists is returned when trying to lookup the state of a frame that
// doesn't exist
var errFrameNoExists = fmt.Errorf("frame does not exist")

type ErrFrameNoExists struct{}

func (e ErrFrameNoExists) Error() string {
	return errFrameNoExists.Error()
}

func (e ErrFrameNoExists) Unwrap() error {
	return errFrameNoExists
}

// QueryAndBuildEvent builds a Matrix event using the event builder and dataframe query
// API client provided. If also fills dataframe query API response (if provided)
// in case the function calling FillBuilder needs to use it.
// Returns ErrFrameNoExists if the state of the frame could not be retrieved because
// the frame doesn't exist
// Returns an error if something else went wrong
func QueryAndBuildEvent(
	ctx context.Context,
	proto *xtools.ProtoEvent,
	identity *fclient.SigningIdentity, evTime time.Time,
	rsAPI api.QueryLatestEventsAndStateAPI, queryRes *api.QueryLatestEventsAndStateResponse,
) (*types.HeaderedEvent, error) {
	if queryRes == nil {
		queryRes = &api.QueryLatestEventsAndStateResponse{}
	}

	eventsNeeded, err := queryRequiredEventsForBuilder(ctx, proto, rsAPI, queryRes)
	if err != nil {
		// This can pass through a ErrFrameNoExists to the caller
		return nil, err
	}
	return BuildEvent(ctx, proto, identity, evTime, eventsNeeded, queryRes)
}

// BuildEvent builds a Matrix event from the builder and QueryLatestEventsAndStateResponse
// provided.
func BuildEvent(
	ctx context.Context,
	proto *xtools.ProtoEvent,
	identity *fclient.SigningIdentity, evTime time.Time,
	eventsNeeded *xtools.StateNeeded, queryRes *api.QueryLatestEventsAndStateResponse,
) (*types.HeaderedEvent, error) {
	if err := addPrevEventsToEvent(proto, eventsNeeded, queryRes); err != nil {
		return nil, err
	}

	verImpl, err := xtools.GetFrameVersion(queryRes.FrameVersion)
	if err != nil {
		return nil, err
	}
	builder := verImpl.NewEventBuilderFromProtoEvent(proto)

	event, err := builder.Build(
		evTime, identity.ServerName, identity.KeyID,
		identity.PrivateKey,
	)
	if err != nil {
		return nil, err
	}

	return &types.HeaderedEvent{PDU: event}, nil
}

// queryRequiredEventsForBuilder queries the dataframe for auth/prev events needed for this builder.
func queryRequiredEventsForBuilder(
	ctx context.Context,
	proto *xtools.ProtoEvent,
	rsAPI api.QueryLatestEventsAndStateAPI, queryRes *api.QueryLatestEventsAndStateResponse,
) (*xtools.StateNeeded, error) {
	eventsNeeded, err := xtools.StateNeededForProtoEvent(proto)
	if err != nil {
		return nil, fmt.Errorf("xtools.StateNeededForProtoEvent: %w", err)
	}

	if len(eventsNeeded.Tuples()) == 0 {
		return nil, errors.New("expecting state tuples for event builder, got none")
	}

	// Ask the dataframe for information about this frame
	queryReq := api.QueryLatestEventsAndStateRequest{
		FrameID:       proto.FrameID,
		StateToFetch: eventsNeeded.Tuples(),
	}
	return &eventsNeeded, rsAPI.QueryLatestEventsAndState(ctx, &queryReq, queryRes)
}

// addPrevEventsToEvent fills out the prev_events and auth_events fields in builder
func addPrevEventsToEvent(
	builder *xtools.ProtoEvent,
	eventsNeeded *xtools.StateNeeded,
	queryRes *api.QueryLatestEventsAndStateResponse,
) error {
	if !queryRes.FrameExists {
		return ErrFrameNoExists{}
	}

	builder.Depth = queryRes.Depth

	authEvents := xtools.NewAuthEvents(nil)

	for i := range queryRes.StateEvents {
		err := authEvents.AddEvent(queryRes.StateEvents[i].PDU)
		if err != nil {
			return fmt.Errorf("authEvents.AddEvent: %w", err)
		}
	}

	refs, err := eventsNeeded.AuthEventReferences(&authEvents)
	if err != nil {
		return fmt.Errorf("eventsNeeded.AuthEventReferences: %w", err)
	}

	builder.AuthEvents, builder.PrevEvents = truncateAuthAndPrevEvents(refs, queryRes.LatestEvents)

	return nil
}

// truncateAuthAndPrevEvents limits the number of events we add into
// an event as prev_events or auth_events.
// NOTSPEC: The limits here feel a bit arbitrary but they are currently
// here because of https://github.com/withqb/coddy-doc/issues/2307
// and because Synapse will just drop events that don't comply.
func truncateAuthAndPrevEvents(auth, prev []string) (
	truncAuth, truncPrev []string,
) {
	truncAuth, truncPrev = auth, prev
	if len(truncAuth) > 10 {
		truncAuth = truncAuth[:10]
	}
	if len(truncPrev) > 20 {
		truncPrev = truncPrev[:20]
	}
	return
}

// RedactEvent redacts the given event and sets the unsigned field appropriately. This should be used by
// downstream components to the dataframe when an OutputTypeRedactedEvent occurs.
func RedactEvent(ctx context.Context, redactionEvent, redactedEvent xtools.PDU, querier api.QuerySenderIDAPI) error {
	// sanity check
	if redactionEvent.Type() != spec.MFrameRedaction {
		return fmt.Errorf("RedactEvent: redactionEvent isn't a redaction event, is '%s'", redactionEvent.Type())
	}
	redactedEvent.Redact()
	validFrameID, err := spec.NewFrameID(redactionEvent.FrameID())
	if err != nil {
		return err
	}
	senderID, err := querier.QueryUserIDForSender(ctx, *validFrameID, redactionEvent.SenderID())
	if err != nil {
		return err
	}
	redactedBecause := synctypes.ToClientEvent(redactionEvent, synctypes.FormatSync, senderID.String(), redactionEvent.StateKey(), redactionEvent.Unsigned())
	if err := redactedEvent.SetUnsignedField("redacted_because", redactedBecause); err != nil {
		return err
	}
	// NOTSPEC: sytest relies on this unspecced field existing :(
	if err := redactedEvent.SetUnsignedField("redacted_by", redactionEvent.EventID()); err != nil {
		return err
	}
	return nil
}
