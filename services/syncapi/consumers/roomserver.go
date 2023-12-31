package consumers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/withqb/coddy/internal/fulltext"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/api"
	rstypes "github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/coddy/services/syncapi/notifier"
	"github.com/withqb/coddy/services/syncapi/storage"
	"github.com/withqb/coddy/services/syncapi/streams"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
	"github.com/withqb/xtools/spec"
)

// OutputFrameEventConsumer consumes events that originated in the frame server.
type OutputFrameEventConsumer struct {
	ctx          context.Context
	cfg          *config.SyncAPI
	rsAPI        api.SyncDataframeAPI
	jetstream    nats.JetStreamContext
	durable      string
	topic        string
	db           storage.Database
	pduStream    streams.StreamProvider
	inviteStream streams.StreamProvider
	notifier     *notifier.Notifier
	fts          fulltext.Indexer
}

// NewOutputFrameEventConsumer creates a new OutputFrameEventConsumer. Call Start() to begin consuming from frame servers.
func NewOutputFrameEventConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	store storage.Database,
	notifier *notifier.Notifier,
	pduStream streams.StreamProvider,
	inviteStream streams.StreamProvider,
	rsAPI api.SyncDataframeAPI,
	fts *fulltext.Search,
) *OutputFrameEventConsumer {
	return &OutputFrameEventConsumer{
		ctx:          process.Context(),
		cfg:          cfg,
		jetstream:    js,
		topic:        cfg.Coddy.JetStream.Prefixed(jetstream.OutputFrameEvent),
		durable:      cfg.Coddy.JetStream.Durable("SyncAPIDataFrameConsumer"),
		db:           store,
		notifier:     notifier,
		pduStream:    pduStream,
		inviteStream: inviteStream,
		rsAPI:        rsAPI,
		fts:          fts,
	}
}

// Start consuming from frame servers
func (s *OutputFrameEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called when the sync server receives a new event from the frame server output log.
// It is not safe for this function to be called from multiple goroutines, or else the
// sync stream position may race and be incorrectly calculated.
func (s *OutputFrameEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// Parse out the event JSON
	var err error
	var output api.OutputEvent
	if err = json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("dataframe output log: message parse failure")
		return true
	}

	switch output.Type {
	case api.OutputTypeNewFrameEvent:
		// Ignore redaction events. We will add them to the database when they are
		// validated (when we receive OutputTypeRedactedEvent)
		event := output.NewFrameEvent.Event
		if event.Type() == spec.MFrameRedaction && event.StateKey() == nil {
			// in the special case where the event redacts itself, just pass the message through because
			// we will never see the other part of the pair
			if event.Redacts() != event.EventID() {
				return true
			}
		}
		err = s.onNewFrameEvent(s.ctx, *output.NewFrameEvent)
	case api.OutputTypeOldFrameEvent:
		err = s.onOldFrameEvent(s.ctx, *output.OldFrameEvent)
	case api.OutputTypeNewInviteEvent:
		s.onNewInviteEvent(s.ctx, *output.NewInviteEvent)
	case api.OutputTypeRetireInviteEvent:
		s.onRetireInviteEvent(s.ctx, *output.RetireInviteEvent)
	case api.OutputTypeNewPeek:
		s.onNewPeek(s.ctx, *output.NewPeek)
	case api.OutputTypeRetirePeek:
		s.onRetirePeek(s.ctx, *output.RetirePeek)
	case api.OutputTypeRedactedEvent:
		err = s.onRedactEvent(s.ctx, *output.RedactedEvent)
	case api.OutputTypePurgeFrame:
		err = s.onPurgeFrame(s.ctx, *output.PurgeFrame)
		if err != nil {
			logrus.WithField("frame_id", output.PurgeFrame.FrameID).WithError(err).Error("failed to purge frame from sync API")
			return true // non-fatal, as otherwise we end up in a loop of trying to purge the frame
		}
	default:
		log.WithField("type", output.Type).Debug(
			"dataframe output log: ignoring unknown output type",
		)
	}
	if err != nil {
		if errors.As(err, new(base64.CorruptInputError)) {
			// no matter how often we retry this event, we will always get this error, discard the event
			return true
		}
		log.WithFields(log.Fields{
			"type": output.Type,
		}).WithError(err).Error("dataframe output log: failed to process event")
		sentry.CaptureException(err)
		return false
	}

	return true
}

func (s *OutputFrameEventConsumer) onRedactEvent(
	ctx context.Context, msg api.OutputRedactedEvent,
) error {
	err := s.db.RedactEvent(ctx, msg.RedactedEventID, msg.RedactedBecause, s.rsAPI)
	if err != nil {
		log.WithError(err).Error("RedactEvent error'd")
		return err
	}

	if err = s.db.RedactRelations(ctx, msg.RedactedBecause.FrameID(), msg.RedactedEventID); err != nil {
		log.WithFields(log.Fields{
			"frame_id":           msg.RedactedBecause.FrameID(),
			"event_id":          msg.RedactedBecause.EventID(),
			"redacted_event_id": msg.RedactedEventID,
		}).WithError(err).Warn("failed to redact relations")
		return err
	}

	// fake a frame event so we notify clients about the redaction, as if it were
	// a normal event.
	return s.onNewFrameEvent(ctx, api.OutputNewFrameEvent{
		Event: msg.RedactedBecause,
	})
}

func (s *OutputFrameEventConsumer) onNewFrameEvent(
	ctx context.Context, msg api.OutputNewFrameEvent,
) error {
	ev := msg.Event
	addsStateEvents, missingEventIDs := msg.NeededStateEventIDs()

	// Work out the list of events we need to find out about. Either
	// they will be the event supplied in the request, we will find it
	// in the sync API database or we'll need to ask the dataframe.
	knownEventIDs := make(map[string]bool, len(msg.AddsStateEventIDs))
	for _, eventID := range missingEventIDs {
		knownEventIDs[eventID] = false
	}

	// Look the events up in the database. If we know them, add them into
	// the set of adds state events.
	if len(missingEventIDs) > 0 {
		alreadyKnown, err := s.db.Events(ctx, missingEventIDs)
		if err != nil {
			return fmt.Errorf("s.db.Events: %w", err)
		}
		for _, knownEvent := range alreadyKnown {
			knownEventIDs[knownEvent.EventID()] = true
			addsStateEvents = append(addsStateEvents, knownEvent)
		}
	}

	// Now work out if there are any remaining events we don't know. For
	// these we will need to ask the dataframe for help.
	missingEventIDs = missingEventIDs[:0]
	for eventID, known := range knownEventIDs {
		if !known {
			missingEventIDs = append(missingEventIDs, eventID)
		}
	}

	// Ask the dataframe and add in the rest of the results into the set.
	// Finally, work out if there are any more events missing.
	if len(missingEventIDs) > 0 {
		eventsReq := &api.QueryEventsByIDRequest{
			FrameID:   ev.FrameID(),
			EventIDs: missingEventIDs,
		}
		eventsRes := &api.QueryEventsByIDResponse{}
		if err := s.rsAPI.QueryEventsByID(ctx, eventsReq, eventsRes); err != nil {
			return fmt.Errorf("s.rsAPI.QueryEventsByID: %w", err)
		}
		for _, event := range eventsRes.Events {
			addsStateEvents = append(addsStateEvents, event)
			knownEventIDs[event.EventID()] = true
		}

		// This should never happen because this would imply that the
		// dataframe has sent us adds_state_event_ids for events that it
		// also doesn't know about, but let's just be sure.
		for eventID, found := range knownEventIDs {
			if !found {
				return fmt.Errorf("event %s is missing", eventID)
			}
		}
	}

	ev, err := s.updateStateEvent(ev)
	if err != nil {
		return err
	}

	for i := range addsStateEvents {
		addsStateEvents[i], err = s.updateStateEvent(addsStateEvents[i])
		if err != nil {
			return err
		}
	}

	if msg.RewritesState {
		if err = s.db.PurgeFrameState(ctx, ev.FrameID()); err != nil {
			return fmt.Errorf("s.db.PurgeFrame: %w", err)
		}
	}

	validFrameID, err := spec.NewFrameID(ev.FrameID())
	if err != nil {
		return err
	}

	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, ev.SenderID())
	if err != nil {
		return err
	}

	ev.UserID = *userID

	pduPos, err := s.db.WriteEvent(ctx, ev, addsStateEvents, msg.AddsStateEventIDs, msg.RemovesStateEventIDs, msg.TransactionID, false, msg.HistoryVisibility)
	if err != nil {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			"event_id":   ev.EventID(),
			"event":      string(ev.JSON()),
			log.ErrorKey: err,
			"add":        msg.AddsStateEventIDs,
			"del":        msg.RemovesStateEventIDs,
		}).Panicf("dataframe output log: write new event failure")
		return nil
	}
	if err = s.writeFTS(ev, pduPos); err != nil {
		log.WithFields(log.Fields{
			"event_id": ev.EventID(),
			"type":     ev.Type(),
		}).WithError(err).Warn("failed to index fulltext element")
	}

	if pduPos, err = s.notifyJoinedPeeks(ctx, ev, pduPos); err != nil {
		log.WithError(err).Errorf("failed to notifyJoinedPeeks for PDU pos %d", pduPos)
		return err
	}

	if err = s.db.UpdateRelations(ctx, ev); err != nil {
		log.WithFields(log.Fields{
			"event_id": ev.EventID(),
			"type":     ev.Type(),
		}).WithError(err).Warn("failed to update relations")
		return err
	}

	s.pduStream.Advance(pduPos)
	s.notifier.OnNewEvent(ev, ev.FrameID(), nil, types.StreamingToken{PDUPosition: pduPos})

	return nil
}

func (s *OutputFrameEventConsumer) onOldFrameEvent(
	ctx context.Context, msg api.OutputOldFrameEvent,
) error {
	ev := msg.Event

	// TDO: The state key check when excluding from sync is designed
	// to stop us from lying to clients with old state, whilst still
	// allowing normal timeline events through. This is an absolute
	// hack but until we have some better strategy for dealing with
	// old events in the sync API, this should at least prevent us
	// from confusing clients into thinking they've joined/left frames.

	validFrameID, err := spec.NewFrameID(ev.FrameID())
	if err != nil {
		return err
	}

	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, ev.SenderID())
	if err != nil {
		return err
	}
	ev.UserID = *userID

	pduPos, err := s.db.WriteEvent(ctx, ev, []*rstypes.HeaderedEvent{}, []string{}, []string{}, nil, ev.StateKey() != nil, msg.HistoryVisibility)
	if err != nil {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			"event_id":   ev.EventID(),
			"event":      string(ev.JSON()),
			log.ErrorKey: err,
		}).Panicf("dataframe output log: write old event failure")
		return nil
	}

	if err = s.writeFTS(ev, pduPos); err != nil {
		log.WithFields(log.Fields{
			"event_id": ev.EventID(),
			"type":     ev.Type(),
		}).WithError(err).Warn("failed to index fulltext element")
	}

	if err = s.db.UpdateRelations(ctx, ev); err != nil {
		log.WithFields(log.Fields{
			"frame_id":  ev.FrameID(),
			"event_id": ev.EventID(),
			"type":     ev.Type(),
		}).WithError(err).Warn("failed to update relations")
		return err
	}

	if pduPos, err = s.notifyJoinedPeeks(ctx, ev, pduPos); err != nil {
		log.WithError(err).Errorf("failed to notifyJoinedPeeks for PDU pos %d", pduPos)
		return err
	}

	s.pduStream.Advance(pduPos)
	s.notifier.OnNewEvent(ev, ev.FrameID(), nil, types.StreamingToken{PDUPosition: pduPos})

	return nil
}

func (s *OutputFrameEventConsumer) notifyJoinedPeeks(ctx context.Context, ev *rstypes.HeaderedEvent, sp types.StreamPosition) (types.StreamPosition, error) {
	if ev.Type() != spec.MFrameMember {
		return sp, nil
	}
	membership, err := ev.Membership()
	if err != nil {
		return sp, fmt.Errorf("ev.Membership: %w", err)
	}
	// TDO: check that it's a join and not a profile change (means unmarshalling prev_content)
	if membership == spec.Join {
		// check it's a local join
		if ev.StateKey() == nil {
			return sp, fmt.Errorf("unexpected nil state_key")
		}

		validFrameID, err := spec.NewFrameID(ev.FrameID())
		if err != nil {
			return sp, err
		}
		userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*ev.StateKey()))
		if err != nil || userID == nil {
			return sp, fmt.Errorf("failed getting userID for sender: %w", err)
		}
		if !s.cfg.Coddy.IsLocalServerName(userID.Domain()) {
			return sp, nil
		}

		// cancel any peeks for it
		peekSP, peekErr := s.db.DeletePeeks(ctx, ev.FrameID(), *ev.StateKey())
		if peekErr != nil {
			return sp, fmt.Errorf("s.db.DeletePeeks: %w", peekErr)
		}
		if peekSP > 0 {
			sp = peekSP
		}
	}
	return sp, nil
}

func (s *OutputFrameEventConsumer) onNewInviteEvent(
	ctx context.Context, msg api.OutputNewInviteEvent,
) {
	if msg.Event.StateKey() == nil {
		return
	}

	validFrameID, err := spec.NewFrameID(msg.Event.FrameID())
	if err != nil {
		return
	}
	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*msg.Event.StateKey()))
	if err != nil || userID == nil {
		return
	}
	if !s.cfg.Coddy.IsLocalServerName(userID.Domain()) {
		return
	}

	msg.Event.UserID = *userID

	pduPos, err := s.db.AddInviteEvent(ctx, msg.Event)
	if err != nil {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			"event_id":   msg.Event.EventID(),
			"event":      string(msg.Event.JSON()),
			"pdupos":     pduPos,
			log.ErrorKey: err,
		}).Errorf("dataframe output log: write invite failure")
		return
	}

	s.inviteStream.Advance(pduPos)
	s.notifier.OnNewInvite(types.StreamingToken{InvitePosition: pduPos}, *msg.Event.StateKey())
}

func (s *OutputFrameEventConsumer) onRetireInviteEvent(
	ctx context.Context, msg api.OutputRetireInviteEvent,
) {
	pduPos, err := s.db.RetireInviteEvent(ctx, msg.EventID)
	// It's possible we just haven't heard of this invite yet, so
	// we should not panic if we try to retire it.
	if err != nil && err != sql.ErrNoRows {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			"event_id":   msg.EventID,
			log.ErrorKey: err,
		}).Errorf("dataframe output log: remove invite failure")
		return
	}

	// Only notify clients about retired invite events, if the user didn't accept the invite.
	// The PDU stream will also receive an event about accepting the invitation, so there should
	// be a "smooth" transition from invite -> join, and not invite -> leave -> join
	if msg.Membership == spec.Join {
		return
	}

	// Notify any active sync requests that the invite has been retired.
	s.inviteStream.Advance(pduPos)
	validFrameID, err := spec.NewFrameID(msg.FrameID)
	if err != nil {
		log.WithFields(log.Fields{
			"event_id":   msg.EventID,
			"frame_id":    msg.FrameID,
			log.ErrorKey: err,
		}).Errorf("frameID is invalid")
		return
	}
	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, msg.TargetSenderID)
	if err != nil || userID == nil {
		log.WithFields(log.Fields{
			"event_id":   msg.EventID,
			"sender_id":  msg.TargetSenderID,
			log.ErrorKey: err,
		}).Errorf("failed to find userID for sender")
		return
	}
	s.notifier.OnNewInvite(types.StreamingToken{InvitePosition: pduPos}, userID.String())
}

func (s *OutputFrameEventConsumer) onNewPeek(
	ctx context.Context, msg api.OutputNewPeek,
) {
	sp, err := s.db.AddPeek(ctx, msg.FrameID, msg.UserID, msg.DeviceID)
	if err != nil {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			log.ErrorKey: err,
		}).Errorf("dataframe output log: write peek failure")
		return
	}

	// tell the notifier about the new peek so it knows to wake up new devices
	// TDO: This only works because the peeks table is reusing the same
	// index as PDUs, but we should fix this
	s.pduStream.Advance(sp)
	s.notifier.OnNewPeek(msg.FrameID, msg.UserID, msg.DeviceID, types.StreamingToken{PDUPosition: sp})
}

func (s *OutputFrameEventConsumer) onRetirePeek(
	ctx context.Context, msg api.OutputRetirePeek,
) {
	sp, err := s.db.DeletePeek(ctx, msg.FrameID, msg.UserID, msg.DeviceID)
	if err != nil {
		// panic rather than continue with an inconsistent database
		log.WithFields(log.Fields{
			log.ErrorKey: err,
		}).Errorf("dataframe output log: write peek failure")
		return
	}

	// tell the notifier about the new peek so it knows to wake up new devices
	// TDO: This only works because the peeks table is reusing the same
	// index as PDUs, but we should fix this
	s.pduStream.Advance(sp)
	s.notifier.OnRetirePeek(msg.FrameID, msg.UserID, msg.DeviceID, types.StreamingToken{PDUPosition: sp})
}

func (s *OutputFrameEventConsumer) onPurgeFrame(
	ctx context.Context, req api.OutputPurgeFrame,
) error {
	logrus.WithField("frame_id", req.FrameID).Warn("Purging frame from sync API")

	if err := s.db.PurgeFrame(ctx, req.FrameID); err != nil {
		logrus.WithField("frame_id", req.FrameID).WithError(err).Error("failed to purge frame from sync API")
		return err
	} else {
		logrus.WithField("frame_id", req.FrameID).Warn("Frame purged from sync API")
		return nil
	}
}

func (s *OutputFrameEventConsumer) updateStateEvent(event *rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error) {
	event.StateKeyResolved = event.StateKey()
	if event.StateKey() == nil {
		return event, nil
	}
	stateKey := *event.StateKey()

	snapshot, err := s.db.NewDatabaseSnapshot(s.ctx)
	if err != nil {
		return nil, err
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return event, err
	}

	sKeyUser := ""
	if stateKey != "" {
		var sku *spec.UserID
		sku, err = s.rsAPI.QueryUserIDForSender(s.ctx, *validFrameID, spec.SenderID(stateKey))
		if err == nil && sku != nil {
			sKeyUser = sku.String()
			event.StateKeyResolved = &sKeyUser
		}
	}

	prevEvent, err := snapshot.GetStateEvent(
		s.ctx, event.FrameID(), event.Type(), sKeyUser,
	)
	if err != nil {
		return event, err
	}

	userID, err := s.rsAPI.QueryUserIDForSender(s.ctx, *validFrameID, event.SenderID())
	if err != nil {
		return event, err
	}

	event.UserID = *userID

	if prevEvent == nil || prevEvent.EventID() == event.EventID() {
		return event, nil
	}

	prev := synctypes.PrevEventRef{
		PrevContent:   prevEvent.Content(),
		ReplacesState: prevEvent.EventID(),
		PrevSenderID:  string(prevEvent.SenderID()),
	}

	event.PDU, err = event.SetUnsigned(prev)
	succeeded = true
	return event, err
}

func (s *OutputFrameEventConsumer) writeFTS(ev *rstypes.HeaderedEvent, pduPosition types.StreamPosition) error {
	if !s.cfg.Fulltext.Enabled {
		return nil
	}
	e := fulltext.IndexElement{
		EventID:        ev.EventID(),
		FrameID:         ev.FrameID(),
		StreamPosition: int64(pduPosition),
	}
	e.SetContentType(ev.Type())

	switch ev.Type() {
	case "m.frame.message":
		e.Content = gjson.GetBytes(ev.Content(), "body").String()
	case spec.MFrameName:
		e.Content = gjson.GetBytes(ev.Content(), "name").String()
	case spec.MFrameTopic:
		e.Content = gjson.GetBytes(ev.Content(), "topic").String()
	case spec.MFrameRedaction:
		log.Tracef("Redacting event: %s", ev.Redacts())
		if err := s.fts.Delete(ev.Redacts()); err != nil {
			return fmt.Errorf("failed to delete entry from fulltext index: %w", err)
		}
		return nil
	default:
		return nil
	}
	if e.Content != "" {
		log.Tracef("Indexing element: %+v", e)
		if err := s.fts.Index(e); err != nil {
			return err
		}
	}
	return nil
}
