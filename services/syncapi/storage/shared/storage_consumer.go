package shared

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"

	rstypes "github.com/withqb/coddy/services/dataframe/types"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/xtools/spec"

	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"

	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/syncapi/storage/tables"
	"github.com/withqb/coddy/services/syncapi/synctypes"
	"github.com/withqb/coddy/services/syncapi/types"
)

// Database is a temporary struct until we have made syncserver.go the same for both pq/sqlite
// For now this contains the shared functions
type Database struct {
	DB                  *sql.DB
	Writer              sqlutil.Writer
	Invites             tables.Invites
	Peeks               tables.Peeks
	AccountData         tables.AccountData
	OutputEvents        tables.Events
	Topology            tables.Topology
	CurrentFrameState    tables.CurrentFrameState
	BackwardExtremities tables.BackwardsExtremities
	SendToDevice        tables.SendToDevice
	Filter              tables.Filter
	Receipts            tables.Receipts
	Memberships         tables.Memberships
	NotificationData    tables.NotificationData
	Ignores             tables.Ignores
	Presence            tables.Presence
	Relations           tables.Relations
}

func (d *Database) NewDatabaseSnapshot(ctx context.Context) (*DatabaseTransaction, error) {
	txn, err := d.DB.BeginTx(ctx, &sql.TxOptions{
		// Set the isolation level so that we see a snapshot of the database.
		// In PostgreSQL repeatable read transactions will see a snapshot taken
		// at the first query, and since the transaction is read-only it can't
		// run into any serialisation errors.
		// https://www.postgresql.org/docs/9.5/static/transaction-iso.html#XACT-REPEATABLE-READ
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, err
	}
	return &DatabaseTransaction{
		Database: d,
		ctx:      ctx,
		txn:      txn,
	}, nil
}

func (d *Database) NewDatabaseTransaction(ctx context.Context) (*DatabaseTransaction, error) {
	txn, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &DatabaseTransaction{
		Database: d,
		ctx:      ctx,
		txn:      txn,
	}, nil
}

func (d *Database) Events(ctx context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error) {
	streamEvents, err := d.OutputEvents.SelectEvents(ctx, nil, eventIDs, nil, false)
	if err != nil {
		return nil, err
	}

	// We don't include a device here as we only include transaction IDs in
	// incremental syncs.
	return d.StreamEventsToEvents(ctx, nil, streamEvents, nil), nil
}

func (d *Database) StreamEventsToEvents(ctx context.Context, device *userapi.Device, in []types.StreamEvent, rsAPI api.SyncDataframeAPI) []*rstypes.HeaderedEvent {
	out := make([]*rstypes.HeaderedEvent, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i].HeaderedEvent
		if device != nil && in[i].TransactionID != nil {
			userID, err := spec.NewUserID(device.UserID, true)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"event_id": out[i].EventID(),
				}).WithError(err).Warnf("failed to add transaction ID to event")
				continue
			}
			frameID, err := spec.NewFrameID(in[i].FrameID())
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"event_id": out[i].EventID(),
				}).WithError(err).Warnf("Frame ID is invalid")
				continue
			}
			deviceSenderID, err := rsAPI.QuerySenderIDForUser(ctx, *frameID, *userID)
			if err != nil || deviceSenderID == nil {
				logrus.WithFields(logrus.Fields{
					"event_id": out[i].EventID(),
				}).WithError(err).Warnf("failed to add transaction ID to event")
				continue
			}
			if *deviceSenderID == in[i].SenderID() && device.SessionID == in[i].TransactionID.SessionID {
				err := out[i].SetUnsignedField(
					"transaction_id", in[i].TransactionID.TransactionID,
				)
				if err != nil {
					logrus.WithFields(logrus.Fields{
						"event_id": out[i].EventID(),
					}).WithError(err).Warnf("failed to add transaction ID to event")
				}
			}
		}
	}
	return out
}

// AddInviteEvent stores a new invite event for a user.
// If the invite was successfully stored this returns the stream ID it was stored at.
// Returns an error if there was a problem communicating with the database.
func (d *Database) AddInviteEvent(
	ctx context.Context, inviteEvent *rstypes.HeaderedEvent,
) (sp types.StreamPosition, err error) {
	_ = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.Invites.InsertInviteEvent(ctx, txn, inviteEvent)
		return err
	})
	return
}

// RetireInviteEvent removes an old invite event from the database.
// Returns an error if there was a problem communicating with the database.
func (d *Database) RetireInviteEvent(
	ctx context.Context, inviteEventID string,
) (sp types.StreamPosition, err error) {
	_ = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.Invites.DeleteInviteEvent(ctx, txn, inviteEventID)
		return err
	})
	return
}

// AddPeek tracks the fact that a user has started peeking.
// If the peek was successfully stored this returns the stream ID it was stored at.
// Returns an error if there was a problem communicating with the database.
func (d *Database) AddPeek(
	ctx context.Context, frameID, userID, deviceID string,
) (sp types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.Peeks.InsertPeek(ctx, txn, frameID, userID, deviceID)
		return err
	})
	return
}

// DeletePeek tracks the fact that a user has stopped peeking from the specified
// device. If the peeks was successfully deleted this returns the stream ID it was
// stored at. Returns an error if there was a problem communicating with the database.
func (d *Database) DeletePeek(
	ctx context.Context, frameID, userID, deviceID string,
) (sp types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.Peeks.DeletePeek(ctx, txn, frameID, userID, deviceID)
		return err
	})
	if err == sql.ErrNoRows {
		sp = 0
		err = nil
	}
	return
}

// DeletePeeks tracks the fact that a user has stopped peeking from all devices
// If the peeks was successfully deleted this returns the stream ID it was stored at.
// Returns an error if there was a problem communicating with the database.
func (d *Database) DeletePeeks(
	ctx context.Context, frameID, userID string,
) (sp types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.Peeks.DeletePeeks(ctx, txn, frameID, userID)
		return err
	})
	if err == sql.ErrNoRows {
		sp = 0
		err = nil
	}
	return
}

// UpsertAccountData keeps track of new or updated account data, by saving the type
// of the new/updated data, and the user ID and frame ID the data is related to (empty)
// frame ID means the data isn't specific to any frame)
// If no data with the given type, user ID and frame ID exists in the database,
// creates a new row, else update the existing one
// Returns an error if there was an issue with the upsert
func (d *Database) UpsertAccountData(
	ctx context.Context, userID, frameID, dataType string,
) (sp types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		sp, err = d.AccountData.InsertAccountData(ctx, txn, userID, frameID, dataType)
		return err
	})
	return
}

// handleBackwardExtremities adds this event as a backwards extremity if and only if we do not have all of
// the events listed in the event's 'prev_events'. This function also updates the backwards extremities table
// to account for the fact that the given event is no longer a backwards extremity, but may be marked as such.
// This function should always be called within a sqlutil.Writer for safety in SQLite.
func (d *Database) handleBackwardExtremities(ctx context.Context, txn *sql.Tx, ev *rstypes.HeaderedEvent) error {
	if err := d.BackwardExtremities.DeleteBackwardExtremity(ctx, txn, ev.FrameID(), ev.EventID()); err != nil {
		return err
	}

	// Check if we have all of the event's previous events. If an event is
	// missing, add it to the frame's backward extremities.
	prevEvents, err := d.OutputEvents.SelectEvents(ctx, txn, ev.PrevEventIDs(), nil, false)
	if err != nil {
		return err
	}
	var found bool
	for _, eID := range ev.PrevEventIDs() {
		found = false
		for _, prevEv := range prevEvents {
			if eID == prevEv.EventID() {
				found = true
			}
		}

		// If the event is missing, consider it a backward extremity.
		if !found {
			if err = d.BackwardExtremities.InsertsBackwardExtremity(ctx, txn, ev.FrameID(), ev.EventID(), eID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *Database) WriteEvent(
	ctx context.Context,
	ev *rstypes.HeaderedEvent,
	addStateEvents []*rstypes.HeaderedEvent,
	addStateEventIDs, removeStateEventIDs []string,
	transactionID *api.TransactionID, excludeFromSync bool,
	historyVisibility xtools.HistoryVisibility,
) (pduPosition types.StreamPosition, returnErr error) {
	returnErr = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		var err error
		ev.Visibility = historyVisibility
		pos, err := d.OutputEvents.InsertEvent(
			ctx, txn, ev, addStateEventIDs, removeStateEventIDs, transactionID, excludeFromSync, historyVisibility,
		)
		if err != nil {
			return fmt.Errorf("d.OutputEvents.InsertEvent: %w", err)
		}
		pduPosition = pos
		var topoPosition types.StreamPosition
		if topoPosition, err = d.Topology.InsertEventInTopology(ctx, txn, ev, pos); err != nil {
			return fmt.Errorf("d.Topology.InsertEventInTopology: %w", err)
		}

		if err = d.handleBackwardExtremities(ctx, txn, ev); err != nil {
			return fmt.Errorf("d.handleBackwardExtremities: %w", err)
		}

		if len(addStateEvents) == 0 && len(removeStateEventIDs) == 0 {
			// Nothing to do, the event may have just been a message event.
			return nil
		}
		for i := range addStateEvents {
			addStateEvents[i].Visibility = historyVisibility
		}
		return d.updateFrameState(ctx, txn, removeStateEventIDs, addStateEvents, pduPosition, topoPosition)
	})

	return pduPosition, returnErr
}

// This function should always be called within a sqlutil.Writer for safety in SQLite.
func (d *Database) updateFrameState(
	ctx context.Context, txn *sql.Tx,
	removedEventIDs []string,
	addedEvents []*rstypes.HeaderedEvent,
	pduPosition types.StreamPosition,
	topoPosition types.StreamPosition,
) error {
	// remove first, then add, as we do not ever delete state, but do replace state which is a remove followed by an add.
	for _, eventID := range removedEventIDs {
		if err := d.CurrentFrameState.DeleteFrameStateByEventID(ctx, txn, eventID); err != nil {
			return fmt.Errorf("d.CurrentFrameState.DeleteFrameStateByEventID: %w", err)
		}
	}

	for _, event := range addedEvents {
		if event.StateKey() == nil {
			// ignore non state events
			continue
		}
		var membership *string
		if event.Type() == "m.frame.member" {
			value, err := event.Membership()
			if err != nil {
				return fmt.Errorf("event.Membership: %w", err)
			}
			membership = &value
			if err = d.Memberships.UpsertMembership(ctx, txn, event, pduPosition, topoPosition); err != nil {
				return fmt.Errorf("d.Memberships.UpsertMembership: %w", err)
			}
		}

		if err := d.CurrentFrameState.UpsertFrameState(ctx, txn, event, membership, pduPosition); err != nil {
			return fmt.Errorf("d.CurrentFrameState.UpsertFrameState: %w", err)
		}
	}

	return nil
}

func (d *Database) GetFilter(
	ctx context.Context, target *synctypes.Filter, localpart string, filterID string,
) error {
	return d.Filter.SelectFilter(ctx, nil, target, localpart, filterID)
}

func (d *Database) PutFilter(
	ctx context.Context, localpart string, filter *synctypes.Filter,
) (string, error) {
	var filterID string
	var err error
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		filterID, err = d.Filter.InsertFilter(ctx, txn, filter, localpart)
		return err
	})
	return filterID, err
}

func (d *Database) RedactEvent(ctx context.Context, redactedEventID string, redactedBecause *rstypes.HeaderedEvent, querier api.QuerySenderIDAPI) error {
	redactedEvents, err := d.Events(ctx, []string{redactedEventID})
	if err != nil {
		return err
	}
	if len(redactedEvents) == 0 {
		logrus.WithField("event_id", redactedEventID).WithField("redaction_event", redactedBecause.EventID()).Warnf("missing redacted event for redaction")
		return nil
	}
	eventToRedact := redactedEvents[0].PDU
	redactionEvent := redactedBecause.PDU
	if err = eventutil.RedactEvent(ctx, redactionEvent, eventToRedact, querier); err != nil {
		return err
	}

	newEvent := &rstypes.HeaderedEvent{PDU: eventToRedact}
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.OutputEvents.UpdateEventJSON(ctx, txn, newEvent)
	})
	return err
}

// fetchStateEvents converts the set of event IDs into a set of events. It will fetch any which are missing from the database.
// Returns a map of frame ID to list of events.
func (d *Database) fetchStateEvents(
	ctx context.Context, txn *sql.Tx,
	frameIDToEventIDSet map[string]map[string]bool,
	eventIDToEvent map[string]types.StreamEvent,
) (map[string][]types.StreamEvent, error) {
	stateBetween := make(map[string][]types.StreamEvent)
	missingEvents := make(map[string][]string)
	for frameID, ids := range frameIDToEventIDSet {
		events := stateBetween[frameID]
		for id, need := range ids {
			if !need {
				continue // deleted state
			}
			e, ok := eventIDToEvent[id]
			if ok {
				events = append(events, e)
			} else {
				m := missingEvents[frameID]
				m = append(m, id)
				missingEvents[frameID] = m
			}
		}
		stateBetween[frameID] = events
	}

	if len(missingEvents) > 0 {
		// This happens when add_state_ids has an event ID which is not in the provided range.
		// We need to explicitly fetch them.
		allMissingEventIDs := []string{}
		for _, missingEvIDs := range missingEvents {
			allMissingEventIDs = append(allMissingEventIDs, missingEvIDs...)
		}
		evs, err := d.fetchMissingStateEvents(ctx, txn, allMissingEventIDs)
		if err != nil {
			return nil, err
		}
		// we know we got them all otherwise an error would've been returned, so just loop the events
		for _, ev := range evs {
			frameID := ev.FrameID()
			stateBetween[frameID] = append(stateBetween[frameID], ev)
		}
	}
	return stateBetween, nil
}

func (d *Database) fetchMissingStateEvents(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) ([]types.StreamEvent, error) {
	// Fetch from the events table first so we pick up the stream ID for the
	// event.
	events, err := d.OutputEvents.SelectEvents(ctx, txn, eventIDs, nil, false)
	if err != nil {
		return nil, err
	}

	have := map[string]bool{}
	for _, event := range events {
		have[event.EventID()] = true
	}
	var missing []string
	for _, eventID := range eventIDs {
		if !have[eventID] {
			missing = append(missing, eventID)
		}
	}
	if len(missing) == 0 {
		return events, nil
	}

	// If they are missing from the events table then they should be state
	// events that we received from outside the main event stream.
	// These should be in the frame state table.
	stateEvents, err := d.CurrentFrameState.SelectEventsWithEventIDs(ctx, txn, missing)

	if err != nil {
		return nil, err
	}
	if len(stateEvents) != len(missing) {
		logrus.WithContext(ctx).Warnf("failed to map all event IDs to events (got %d, wanted %d)", len(stateEvents), len(missing))

		// TDO: Why is this happening? It's probably the dataframe. Uncomment
		// this error again when we work out what it is and fix it, otherwise we
		// just end up returning lots of 500s to the client and that breaks
		// pretty much everything, rather than just sending what we have.
		//return nil, fmt.Errorf("failed to map all event IDs to events: (got %d, wanted %d)", len(stateEvents), len(missing))
	}
	events = append(events, stateEvents...)
	return events, nil
}

func (d *Database) StoreNewSendForDeviceMessage(
	ctx context.Context, userID, deviceID string, event xtools.SendToDeviceEvent,
) (newPos types.StreamPosition, err error) {
	j, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	// Delegate the database write task to the SendToDeviceWriter. It'll guarantee
	// that we don't lock the table for writes in more than one place.
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		newPos, err = d.SendToDevice.InsertSendToDeviceMessage(
			ctx, txn, userID, deviceID, string(j),
		)
		return err
	})
	if err != nil {
		return 0, err
	}
	return newPos, nil
}

func (d *Database) CleanSendToDeviceUpdates(
	ctx context.Context,
	userID, deviceID string, before types.StreamPosition,
) (err error) {
	if err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.SendToDevice.DeleteSendToDeviceMessages(ctx, txn, userID, deviceID, before)
	}); err != nil {
		logrus.WithError(err).Errorf("failed to clean up old send-to-device messages for user %q device %q", userID, deviceID)
		return err
	}
	return nil
}

// getMembershipFromEvent returns the value of content.membership iff the event is a state event
// with type 'm.frame.member' and state_key of userID. Otherwise, an empty string is returned.
func getMembershipFromEvent(ctx context.Context, ev xtools.PDU, userID string, rsAPI api.SyncDataframeAPI) (string, string) {
	if ev.StateKey() == nil || *ev.StateKey() == "" {
		return "", ""
	}
	fullUser, err := spec.NewUserID(userID, true)
	if err != nil {
		return "", ""
	}
	frameID, err := spec.NewFrameID(ev.FrameID())
	if err != nil {
		return "", ""
	}
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, *frameID, *fullUser)
	if err != nil || senderID == nil {
		return "", ""
	}

	if ev.Type() != "m.frame.member" || !ev.StateKeyEquals(string(*senderID)) {
		return "", ""
	}
	membership, err := ev.Membership()
	if err != nil {
		return "", ""
	}
	prevMembership := gjson.GetBytes(ev.Unsigned(), "prev_content.membership").Str
	return membership, prevMembership
}

// StoreReceipt stores user receipts
func (d *Database) StoreReceipt(ctx context.Context, frameId, receiptType, userId, eventId string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		pos, err = d.Receipts.UpsertReceipt(ctx, txn, frameId, receiptType, userId, eventId, timestamp)
		return err
	})
	return
}

func (d *Database) UpsertFrameUnreadNotificationCounts(ctx context.Context, userID, frameID string, notificationCount, highlightCount int) (pos types.StreamPosition, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		pos, err = d.NotificationData.UpsertFrameUnreadCounts(ctx, txn, userID, frameID, notificationCount, highlightCount)
		return err
	})
	return
}

func (d *Database) SelectContextEvent(ctx context.Context, frameID, eventID string) (int, rstypes.HeaderedEvent, error) {
	return d.OutputEvents.SelectContextEvent(ctx, nil, frameID, eventID)
}

func (d *Database) SelectContextBeforeEvent(ctx context.Context, id int, frameID string, filter *synctypes.FrameEventFilter) ([]*rstypes.HeaderedEvent, error) {
	return d.OutputEvents.SelectContextBeforeEvent(ctx, nil, id, frameID, filter)
}
func (d *Database) SelectContextAfterEvent(ctx context.Context, id int, frameID string, filter *synctypes.FrameEventFilter) (int, []*rstypes.HeaderedEvent, error) {
	return d.OutputEvents.SelectContextAfterEvent(ctx, nil, id, frameID, filter)
}

func (d *Database) IgnoresForUser(ctx context.Context, userID string) (*types.IgnoredUsers, error) {
	return d.Ignores.SelectIgnores(ctx, nil, userID)
}

func (d *Database) UpdateIgnoresForUser(ctx context.Context, userID string, ignores *types.IgnoredUsers) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.Ignores.UpsertIgnores(ctx, txn, userID, ignores)
	})
}

func (d *Database) UpdatePresence(ctx context.Context, userID string, presence types.Presence, statusMsg *string, lastActiveTS spec.Timestamp, fromSync bool) (types.StreamPosition, error) {
	var pos types.StreamPosition
	var err error
	_ = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		pos, err = d.Presence.UpsertPresence(ctx, txn, userID, statusMsg, presence, lastActiveTS, fromSync)
		return nil
	})
	return pos, err
}

func (d *Database) GetPresences(ctx context.Context, userIDs []string) ([]*types.PresenceInternal, error) {
	return d.Presence.GetPresenceForUsers(ctx, nil, userIDs)
}

func (d *Database) SelectMembershipForUser(ctx context.Context, frameID, userID string, pos int64) (membership string, topologicalPos int, err error) {
	return d.Memberships.SelectMembershipForUser(ctx, nil, frameID, userID, pos)
}

func (d *Database) ReIndex(ctx context.Context, limit, afterID int64) (map[int64]rstypes.HeaderedEvent, error) {
	return d.OutputEvents.ReIndex(ctx, nil, limit, afterID, []string{
		spec.MFrameName,
		spec.MFrameTopic,
		"m.frame.message",
	})
}

func (d *Database) UpdateRelations(ctx context.Context, event *rstypes.HeaderedEvent) error {
	// No need to unmarshal if the event is a redaction
	if event.Type() == spec.MFrameRedaction {
		return nil
	}
	var content xtools.RelationContent
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		logrus.WithError(err).Error("unable to unmarshal relation content")
		return nil
	}
	switch {
	case content.Relations == nil:
		return nil
	case content.Relations.EventID == "":
		return nil
	case content.Relations.RelationType == "":
		return nil
	default:
		return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
			return d.Relations.InsertRelation(
				ctx, txn, event.FrameID(), content.Relations.EventID,
				event.EventID(), event.Type(), content.Relations.RelationType,
			)
		})
	}
}

func (d *Database) RedactRelations(ctx context.Context, frameID, redactedEventID string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.Relations.DeleteRelation(ctx, txn, frameID, redactedEventID)
	})
}

func (d *Database) SelectMemberships(
	ctx context.Context,
	frameID string, pos types.TopologyToken,
	membership, notMembership *string,
) (eventIDs []string, err error) {
	return d.Memberships.SelectMemberships(ctx, nil, frameID, pos, membership, notMembership)
}
