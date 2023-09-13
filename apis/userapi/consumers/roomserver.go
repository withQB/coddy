package consumers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"

	"github.com/withqb/coddy/apis/syncapi/synctypes"
	"github.com/withqb/coddy/apis/syncapi/types"
	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/apis/userapi/producers"
	"github.com/withqb/coddy/apis/userapi/storage"
	"github.com/withqb/coddy/apis/userapi/storage/tables"
	userAPITypes "github.com/withqb/coddy/apis/userapi/types"
	"github.com/withqb/coddy/apis/userapi/util"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/internal/pushgateway"
	"github.com/withqb/coddy/internal/pushrules"
	rsapi "github.com/withqb/coddy/servers/dataframe/api"
	rstypes "github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

type OutputFrameEventConsumer struct {
	ctx          context.Context
	cfg          *config.UserAPI
	rsAPI        rsapi.UserDataframeAPI
	jetstream    nats.JetStreamContext
	durable      string
	db           storage.UserDatabase
	topic        string
	pgClient     pushgateway.Client
	syncProducer *producers.SyncAPI
	msgCounts    map[spec.ServerName]userAPITypes.MessageStats
	frameCounts   map[spec.ServerName]map[string]bool // map from serverName to map from rommID to "isEncrypted"
	lastUpdate   time.Time
	countsLock   sync.Mutex
	serverName   spec.ServerName
}

func NewOutputFrameEventConsumer(
	process *process.ProcessContext,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	store storage.UserDatabase,
	pgClient pushgateway.Client,
	rsAPI rsapi.UserDataframeAPI,
	syncProducer *producers.SyncAPI,
) *OutputFrameEventConsumer {
	return &OutputFrameEventConsumer{
		ctx:          process.Context(),
		cfg:          cfg,
		jetstream:    js,
		db:           store,
		durable:      cfg.Matrix.JetStream.Durable("UserAPIDataFrameConsumer"),
		topic:        cfg.Matrix.JetStream.Prefixed(jetstream.OutputFrameEvent),
		pgClient:     pgClient,
		rsAPI:        rsAPI,
		syncProducer: syncProducer,
		msgCounts:    map[spec.ServerName]userAPITypes.MessageStats{},
		frameCounts:   map[spec.ServerName]map[string]bool{},
		lastUpdate:   time.Now(),
		countsLock:   sync.Mutex{},
		serverName:   cfg.Matrix.ServerName,
	}
}

func (s *OutputFrameEventConsumer) Start() error {
	if err := jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	); err != nil {
		return err
	}
	return nil
}

func (s *OutputFrameEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// Only handle events we care about
	if rsapi.OutputType(msg.Header.Get(jetstream.FrameEventType)) != rsapi.OutputTypeNewFrameEvent {
		return true
	}
	var output rsapi.OutputEvent
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("dataframe output log: message parse failure")
		return true
	}
	event := output.NewFrameEvent.Event
	if event == nil {
		log.Errorf("userapi consumer: expected event")
		return true
	}

	if s.cfg.Matrix.ReportStats.Enabled {
		go s.storeMessageStats(ctx, event.Type(), string(event.SenderID()), event.FrameID())
	}

	log.WithFields(log.Fields{
		"event_id":   event.EventID(),
		"event_type": event.Type(),
	}).Tracef("Received message from dataframe: %#v", output)

	metadata, err := msg.Metadata()
	if err != nil {
		return true
	}

	if err := s.processMessage(ctx, event, uint64(spec.AsTimestamp(metadata.Timestamp))); err != nil {
		log.WithFields(log.Fields{
			"event_id": event.EventID(),
		}).WithError(err).Errorf("userapi consumer: process frame event failure")
	}

	return true
}

func (s *OutputFrameEventConsumer) storeMessageStats(ctx context.Context, eventType, eventSender, frameID string) {
	s.countsLock.Lock()
	defer s.countsLock.Unlock()

	// reset the frameCounts on a day change
	if s.lastUpdate.Day() != time.Now().Day() {
		s.frameCounts[s.serverName] = make(map[string]bool)
		s.lastUpdate = time.Now()
	}

	_, sender, err := xtools.SplitID('@', eventSender)
	if err != nil {
		return
	}
	msgCount := s.msgCounts[s.serverName]
	frameCount := s.frameCounts[s.serverName]
	if frameCount == nil {
		frameCount = make(map[string]bool)
	}
	switch eventType {
	case "m.frame.message":
		frameCount[frameID] = false
		msgCount.Messages++
		if sender == s.serverName {
			msgCount.SentMessages++
		}
	case "m.frame.encrypted":
		frameCount[frameID] = true
		msgCount.MessagesE2EE++
		if sender == s.serverName {
			msgCount.SentMessagesE2EE++
		}
	default:
		return
	}
	s.msgCounts[s.serverName] = msgCount
	s.frameCounts[s.serverName] = frameCount

	for serverName, stats := range s.msgCounts {
		var normalFrames, encryptedFrames int64 = 0, 0
		for _, isEncrypted := range s.frameCounts[s.serverName] {
			if isEncrypted {
				encryptedFrames++
			} else {
				normalFrames++
			}
		}
		err := s.db.UpsertDailyFramesMessages(ctx, serverName, stats, normalFrames, encryptedFrames)
		if err != nil {
			log.WithError(err).Errorf("failed to upsert daily messages")
		}
		// Clear stats if we successfully stored it
		if err == nil {
			stats.Messages = 0
			stats.SentMessages = 0
			stats.MessagesE2EE = 0
			stats.SentMessagesE2EE = 0
			s.msgCounts[serverName] = stats
		}
	}
}

func (s *OutputFrameEventConsumer) handleFrameUpgrade(ctx context.Context, oldFrameID, newFrameID string, localMembers []*localMembership, frameSize int) error {
	for _, membership := range localMembers {
		// Copy any existing push rules from old -> new frame
		if err := s.copyPushrules(ctx, oldFrameID, newFrameID, membership.Localpart, membership.Domain); err != nil {
			return err
		}

		// preserve m.direct frame state
		if err := s.updateMDirect(ctx, oldFrameID, newFrameID, membership.Localpart, membership.Domain, frameSize); err != nil {
			return err
		}

		// copy existing m.tag entries, if any
		if err := s.copyTags(ctx, oldFrameID, newFrameID, membership.Localpart, membership.Domain); err != nil {
			return err
		}
	}
	return nil
}

func (s *OutputFrameEventConsumer) copyPushrules(ctx context.Context, oldFrameID, newFrameID string, localpart string, serverName spec.ServerName) error {
	pushRules, err := s.db.QueryPushRules(ctx, localpart, serverName)
	if err != nil {
		return fmt.Errorf("failed to query pushrules for user: %w", err)
	}
	if pushRules == nil {
		return nil
	}

	for _, frameRule := range pushRules.Global.Frame {
		if frameRule.RuleID != oldFrameID {
			continue
		}
		cpRool := *frameRule
		cpRool.RuleID = newFrameID
		pushRules.Global.Frame = append(pushRules.Global.Frame, &cpRool)
		rules, err := json.Marshal(pushRules)
		if err != nil {
			return err
		}
		if err = s.db.SaveAccountData(ctx, localpart, serverName, "", "m.push_rules", rules); err != nil {
			return fmt.Errorf("failed to update pushrules: %w", err)
		}
	}
	return nil
}

// updateMDirect copies the "is_direct" flag from oldFrameID to newROomID
func (s *OutputFrameEventConsumer) updateMDirect(ctx context.Context, oldFrameID, newFrameID, localpart string, serverName spec.ServerName, frameSize int) error {
	// this is most likely not a DM, so skip updating m.direct state
	if frameSize > 2 {
		return nil
	}
	// Get direct message state
	directChatsRaw, err := s.db.GetAccountDataByType(ctx, localpart, serverName, "", "m.direct")
	if err != nil {
		return fmt.Errorf("failed to get m.direct from database: %w", err)
	}
	directChats := gjson.ParseBytes(directChatsRaw)
	newDirectChats := make(map[string][]string)
	// iterate over all userID -> frameIDs
	directChats.ForEach(func(userID, frameIDs gjson.Result) bool {
		var found bool
		for _, frameID := range frameIDs.Array() {
			newDirectChats[userID.Str] = append(newDirectChats[userID.Str], frameID.Str)
			// add the new frameID to m.direct
			if frameID.Str == oldFrameID {
				found = true
				newDirectChats[userID.Str] = append(newDirectChats[userID.Str], newFrameID)
			}
		}
		// Only hit the database if we found the old frame as a DM for this user
		if found {
			var data []byte
			data, err = json.Marshal(newDirectChats)
			if err != nil {
				return true
			}
			if err = s.db.SaveAccountData(ctx, localpart, serverName, "", "m.direct", data); err != nil {
				return true
			}
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("failed to update m.direct state")
	}
	return nil
}

func (s *OutputFrameEventConsumer) copyTags(ctx context.Context, oldFrameID, newFrameID, localpart string, serverName spec.ServerName) error {
	tag, err := s.db.GetAccountDataByType(ctx, localpart, serverName, oldFrameID, "m.tag")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if tag == nil {
		return nil
	}
	return s.db.SaveAccountData(ctx, localpart, serverName, newFrameID, "m.tag", tag)
}

func (s *OutputFrameEventConsumer) processMessage(ctx context.Context, event *rstypes.HeaderedEvent, streamPos uint64) error {
	members, frameSize, err := s.localFrameMembers(ctx, event.FrameID())
	if err != nil {
		return fmt.Errorf("s.localFrameMembers: %w", err)
	}

	switch {
	case event.Type() == spec.MFrameMember:
		sender := spec.UserID{}
		validFrameID, frameErr := spec.NewFrameID(event.FrameID())
		if frameErr != nil {
			return frameErr
		}
		userID, queryErr := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
		if queryErr == nil && userID != nil {
			sender = *userID
		}

		sk := event.StateKey()
		if sk != nil && *sk != "" {
			skUserID, queryErr := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*sk))
			if queryErr == nil && skUserID != nil {
				skString := skUserID.String()
				sk = &skString
			} else {
				return fmt.Errorf("queryUserIDForSender: userID unknown for %s", *sk)
			}
		}
		cevent := synctypes.ToClientEvent(event, synctypes.FormatAll, sender.String(), sk, event.Unsigned())
		var member *localMembership
		member, err = newLocalMembership(&cevent)
		if err != nil {
			return fmt.Errorf("newLocalMembership: %w", err)
		}
		if member.Membership == spec.Invite && member.Domain == s.cfg.Matrix.ServerName {
			// localFrameMembers only adds joined members. An invite
			// should also be pushed to the target user.
			members = append(members, member)
		}
	case event.Type() == "m.frame.tombstone" && event.StateKeyEquals(""):
		// Handle frame upgrades
		oldFrameID := event.FrameID()
		newFrameID := gjson.GetBytes(event.Content(), "replacement_frame").Str
		if err = s.handleFrameUpgrade(ctx, oldFrameID, newFrameID, members, frameSize); err != nil {
			// while inconvenient, this shouldn't stop us from sending push notifications
			log.WithError(err).Errorf("UserAPI: failed to handle frame upgrade for users")
		}

	}

	// TDO: run in parallel with localFrameMembers.
	frameName, err := s.frameName(ctx, event)
	if err != nil {
		return fmt.Errorf("s.frameName: %w", err)
	}

	log.WithFields(log.Fields{
		"event_id":    event.EventID(),
		"frame_id":     event.FrameID(),
		"num_members": len(members),
		"frame_size":   frameSize,
	}).Tracef("Notifying members")

	// Notification.UserIsTarget is a per-member field, so we
	// cannot group all users in a single request.
	//
	// TDO: does it have to be set? It's not required, and
	// removing it means we can send all notifications to
	// e.g. Element's Push gateway in one go.
	for _, mem := range members {
		if err := s.notifyLocal(ctx, event, mem, frameSize, frameName, streamPos); err != nil {
			log.WithFields(log.Fields{
				"localpart": mem.Localpart,
			}).WithError(err).Error("Unable to push to local user")
			continue
		}
	}

	return nil
}

type localMembership struct {
	xtools.MemberContent
	UserID    string
	Localpart string
	Domain    spec.ServerName
}

func newLocalMembership(event *synctypes.ClientEvent) (*localMembership, error) {
	if event.StateKey == nil {
		return nil, fmt.Errorf("missing state_key")
	}

	var member localMembership
	if err := json.Unmarshal(event.Content, &member.MemberContent); err != nil {
		return nil, err
	}

	localpart, domain, err := xtools.SplitID('@', *event.StateKey)
	if err != nil {
		return nil, err
	}

	member.UserID = *event.StateKey
	member.Localpart = localpart
	member.Domain = domain
	return &member, nil
}

// localFrameMembers fetches the current local members of a frame, and
// the total number of members.
func (s *OutputFrameEventConsumer) localFrameMembers(ctx context.Context, frameID string) ([]*localMembership, int, error) {
	// Get only locally joined users to avoid unmarshalling and caching
	// membership events we only use to calculate the frame size.
	req := &rsapi.QueryMembershipsForFrameRequest{
		FrameID:     frameID,
		JoinedOnly: true,
		LocalOnly:  true,
	}
	var res rsapi.QueryMembershipsForFrameResponse
	if err := s.rsAPI.QueryMembershipsForFrame(ctx, req, &res); err != nil {
		return nil, 0, err
	}

	// Since we only queried locally joined users above,
	// we also need to ask the dataframe about the joined user count.
	totalCount, err := s.rsAPI.JoinedUserCount(ctx, frameID)
	if err != nil {
		return nil, 0, err
	}

	var members []*localMembership
	for _, event := range res.JoinEvents {
		// Filter out invalid join events
		if event.StateKey == nil {
			continue
		}
		if *event.StateKey == "" {
			continue
		}
		// We're going to trust the Query from above to really just return
		// local users
		member, err := newLocalMembership(&event)
		if err != nil {
			log.WithError(err).Errorf("Parsing MemberContent")
			continue
		}

		members = append(members, member)
	}

	return members, totalCount, nil
}

// frameName returns the name in the event (if type==m.frame.name), or
// looks it up in dataframe. If there is no name,
// m.frame.canonical_alias is consulted. Returns an empty string if the
// frame has no name.
func (s *OutputFrameEventConsumer) frameName(ctx context.Context, event *rstypes.HeaderedEvent) (string, error) {
	if event.Type() == spec.MFrameName {
		name, err := unmarshalFrameName(event)
		if err != nil {
			return "", err
		}

		if name != "" {
			return name, nil
		}
	}

	req := &rsapi.QueryCurrentStateRequest{
		FrameID:      event.FrameID(),
		StateTuples: []xtools.StateKeyTuple{frameNameTuple, canonicalAliasTuple},
	}
	var res rsapi.QueryCurrentStateResponse

	if err := s.rsAPI.QueryCurrentState(ctx, req, &res); err != nil {
		return "", nil
	}

	if eventS := res.StateEvents[frameNameTuple]; eventS != nil {
		return unmarshalFrameName(eventS)
	}

	if event.Type() == spec.MFrameCanonicalAlias {
		alias, err := unmarshalCanonicalAlias(event)
		if err != nil {
			return "", err
		}

		if alias != "" {
			return alias, nil
		}
	}

	if event = res.StateEvents[canonicalAliasTuple]; event != nil {
		return unmarshalCanonicalAlias(event)
	}

	return "", nil
}

var (
	canonicalAliasTuple = xtools.StateKeyTuple{EventType: spec.MFrameCanonicalAlias}
	frameNameTuple       = xtools.StateKeyTuple{EventType: spec.MFrameName}
)

func unmarshalFrameName(event *rstypes.HeaderedEvent) (string, error) {
	var nc eventutil.NameContent
	if err := json.Unmarshal(event.Content(), &nc); err != nil {
		return "", fmt.Errorf("unmarshaling NameContent: %w", err)
	}

	return nc.Name, nil
}

func unmarshalCanonicalAlias(event *rstypes.HeaderedEvent) (string, error) {
	var cac eventutil.CanonicalAliasContent
	if err := json.Unmarshal(event.Content(), &cac); err != nil {
		return "", fmt.Errorf("unmarshaling CanonicalAliasContent: %w", err)
	}

	return cac.Alias, nil
}

// notifyLocal finds the right push actions for a local user, given an event.
func (s *OutputFrameEventConsumer) notifyLocal(ctx context.Context, event *rstypes.HeaderedEvent, mem *localMembership, frameSize int, frameName string, streamPos uint64) error {
	actions, err := s.evaluatePushRules(ctx, event, mem, frameSize)
	if err != nil {
		return fmt.Errorf("s.evaluatePushRules: %w", err)
	}
	a, tweaks, err := pushrules.ActionsToTweaks(actions)
	if err != nil {
		return fmt.Errorf("pushrules.ActionsToTweaks: %w", err)
	}
	// TDO: support coalescing.
	if a != pushrules.NotifyAction && a != pushrules.CoalesceAction {
		log.WithFields(log.Fields{
			"event_id":  event.EventID(),
			"frame_id":   event.FrameID(),
			"localpart": mem.Localpart,
		}).Tracef("Push rule evaluation rejected the event")
		return nil
	}

	devicesByURLAndFormat, profileTag, err := s.localPushDevices(ctx, mem.Localpart, mem.Domain, tweaks)
	if err != nil {
		return fmt.Errorf("s.localPushDevices: %w", err)
	}

	sender := spec.UserID{}
	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return err
	}
	userID, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
	if err == nil && userID != nil {
		sender = *userID
	}

	sk := event.StateKey()
	if sk != nil && *sk != "" {
		skUserID, queryErr := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, spec.SenderID(*event.StateKey()))
		if queryErr == nil && skUserID != nil {
			skString := skUserID.String()
			sk = &skString
		}
	}
	n := &api.Notification{
		Actions: actions,
		// UNSPEC: the spec doesn't say this is a ClientEvent, but the
		// fields seem to match. frame_id should be missing, which
		// matches the behaviour of FormatSync.
		Event: synctypes.ToClientEvent(event, synctypes.FormatSync, sender.String(), sk, event.Unsigned()),
		// TDO: this is per-device, but it's not part of the primary
		// key. So inserting one notification per profile tag doesn't
		// make sense. What is this supposed to be? Sytests require it
		// to "work", but they only use a single device.
		ProfileTag: profileTag,
		FrameID:     event.FrameID(),
		TS:         spec.AsTimestamp(time.Now()),
	}
	if err = s.db.InsertNotification(ctx, mem.Localpart, mem.Domain, event.EventID(), streamPos, tweaks, n); err != nil {
		return fmt.Errorf("s.db.InsertNotification: %w", err)
	}

	if err = s.syncProducer.GetAndSendNotificationData(ctx, mem.UserID, event.FrameID()); err != nil {
		return fmt.Errorf("s.syncProducer.GetAndSendNotificationData: %w", err)
	}

	// We do this after InsertNotification. Thus, this should always return >=1.
	userNumUnreadNotifs, err := s.db.GetNotificationCount(ctx, mem.Localpart, mem.Domain, tables.AllNotifications)
	if err != nil {
		return fmt.Errorf("s.db.GetNotificationCount: %w", err)
	}

	log.WithFields(log.Fields{
		"event_id":   event.EventID(),
		"frame_id":    event.FrameID(),
		"localpart":  mem.Localpart,
		"num_urls":   len(devicesByURLAndFormat),
		"num_unread": userNumUnreadNotifs,
	}).Trace("Notifying single member")

	// Push gateways are out of our control, and we cannot risk
	// looking up the server on a misbehaving push gateway. Each user
	// receives a goroutine now that all internal API calls have been
	// made.
	//
	// TDO: think about bounding this to one per user, and what
	// ordering guarantees we must provide.
	go func() {
		// This background processing cannot be tied to a request.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var rejected []*pushgateway.Device
		for url, fmts := range devicesByURLAndFormat {
			for format, devices := range fmts {
				// TDO: support "email".
				if !strings.HasPrefix(url, "http") {
					continue
				}

				// UNSPEC: the specification suggests there can be
				// more than one device per request. There is at least
				// one Sytest that expects one HTTP request per
				// device, rather than per URL. For now, we must
				// notify each one separately.
				for _, dev := range devices {
					rej, err := s.notifyHTTP(ctx, event, url, format, []*pushgateway.Device{dev}, mem.Localpart, frameName, int(userNumUnreadNotifs))
					if err != nil {
						log.WithFields(log.Fields{
							"event_id":  event.EventID(),
							"localpart": mem.Localpart,
						}).WithError(err).Errorf("Unable to notify HTTP pusher")
						continue
					}
					rejected = append(rejected, rej...)
				}
			}
		}

		if len(rejected) > 0 {
			s.deleteRejectedPushers(ctx, rejected, mem.Localpart, mem.Domain)
		}
	}()

	return nil
}

// evaluatePushRules fetches and evaluates the push rules of a local
// user. Returns actions (including dont_notify).
func (s *OutputFrameEventConsumer) evaluatePushRules(ctx context.Context, event *rstypes.HeaderedEvent, mem *localMembership, frameSize int) ([]*pushrules.Action, error) {
	user := ""
	validFrameID, err := spec.NewFrameID(event.FrameID())
	if err != nil {
		return nil, err
	}
	sender, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
	if err == nil {
		user = sender.String()
	}
	if user == mem.UserID {
		// SPEC: Homeservers MUST NOT notify the Push Gateway for
		// events that the user has sent themselves.
		return nil, nil
	}

	// Get accountdata to check if the event.Sender() is ignored by mem.LocalPart
	data, err := s.db.GetAccountDataByType(ctx, mem.Localpart, mem.Domain, "", "m.ignored_user_list")
	if err != nil {
		return nil, err
	}
	if data != nil {
		ignored := types.IgnoredUsers{}
		err = json.Unmarshal(data, &ignored)
		if err != nil {
			return nil, err
		}
		if _, ok := ignored.List[sender.String()]; ok {
			return nil, fmt.Errorf("user %s is ignored", sender.String())
		}
	}
	ruleSets, err := s.db.QueryPushRules(ctx, mem.Localpart, mem.Domain)
	if err != nil {
		return nil, err
	}

	ec := &ruleSetEvalContext{
		ctx:      ctx,
		rsAPI:    s.rsAPI,
		mem:      mem,
		frameID:   event.FrameID(),
		frameSize: frameSize,
	}
	eval := pushrules.NewRuleSetEvaluator(ec, &ruleSets.Global)
	rule, err := eval.MatchEvent(event.PDU, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
		return s.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
	})
	if err != nil {
		return nil, err
	}
	if rule == nil {
		// SPEC: If no rules match an event, the homeserver MUST NOT
		// notify the Push Gateway for that event.
		return nil, nil
	}

	log.WithFields(log.Fields{
		"event_id":  event.EventID(),
		"frame_id":   event.FrameID(),
		"localpart": mem.Localpart,
		"rule_id":   rule.RuleID,
	}).Trace("Matched a push rule")

	return rule.Actions, nil
}

type ruleSetEvalContext struct {
	ctx      context.Context
	rsAPI    rsapi.UserDataframeAPI
	mem      *localMembership
	frameID   string
	frameSize int
}

func (rse *ruleSetEvalContext) UserDisplayName() string { return rse.mem.DisplayName }

func (rse *ruleSetEvalContext) FrameMemberCount() (int, error) { return rse.frameSize, nil }

func (rse *ruleSetEvalContext) HasPowerLevel(senderID spec.SenderID, levelKey string) (bool, error) {
	req := &rsapi.QueryLatestEventsAndStateRequest{
		FrameID: rse.frameID,
		StateToFetch: []xtools.StateKeyTuple{
			{EventType: spec.MFramePowerLevels},
		},
	}
	var res rsapi.QueryLatestEventsAndStateResponse
	if err := rse.rsAPI.QueryLatestEventsAndState(rse.ctx, req, &res); err != nil {
		return false, err
	}
	for _, ev := range res.StateEvents {
		if ev.Type() != spec.MFramePowerLevels {
			continue
		}

		plc, err := xtools.NewPowerLevelContentFromEvent(ev.PDU)
		if err != nil {
			return false, err
		}
		return plc.UserLevel(senderID) >= plc.NotificationLevel(levelKey), nil
	}
	return true, nil
}

// localPushDevices pushes to the configured devices of a local
// user. The map keys are [url][format].
func (s *OutputFrameEventConsumer) localPushDevices(ctx context.Context, localpart string, serverName spec.ServerName, tweaks map[string]interface{}) (map[string]map[string][]*pushgateway.Device, string, error) {
	pusherDevices, err := util.GetPushDevices(ctx, localpart, serverName, tweaks, s.db)
	if err != nil {
		return nil, "", fmt.Errorf("util.GetPushDevices: %w", err)
	}

	var profileTag string
	devicesByURL := make(map[string]map[string][]*pushgateway.Device, len(pusherDevices))
	for _, pusherDevice := range pusherDevices {
		if profileTag == "" {
			profileTag = pusherDevice.Pusher.ProfileTag
		}

		url := pusherDevice.URL
		if devicesByURL[url] == nil {
			devicesByURL[url] = make(map[string][]*pushgateway.Device, 2)
		}
		devicesByURL[url][pusherDevice.Format] = append(devicesByURL[url][pusherDevice.Format], &pusherDevice.Device)
	}

	return devicesByURL, profileTag, nil
}

// notifyHTTP performs a notificatation to a Push Gateway.
func (s *OutputFrameEventConsumer) notifyHTTP(ctx context.Context, event *rstypes.HeaderedEvent, url, format string, devices []*pushgateway.Device, localpart, frameName string, userNumUnreadNotifs int) ([]*pushgateway.Device, error) {
	logger := log.WithFields(log.Fields{
		"event_id":    event.EventID(),
		"url":         url,
		"localpart":   localpart,
		"num_devices": len(devices),
	})

	var req pushgateway.NotifyRequest
	switch format {
	case "event_id_only":
		req = pushgateway.NotifyRequest{
			Notification: pushgateway.Notification{
				Counts: &pushgateway.Counts{
					Unread: userNumUnreadNotifs,
				},
				Devices: devices,
				EventID: event.EventID(),
				FrameID:  event.FrameID(),
			},
		}

	default:
		validFrameID, err := spec.NewFrameID(event.FrameID())
		if err != nil {
			return nil, err
		}
		sender, err := s.rsAPI.QueryUserIDForSender(ctx, *validFrameID, event.SenderID())
		if err != nil {
			logger.WithError(err).Errorf("failed to get userID for sender %s", event.SenderID())
			return nil, err
		}
		req = pushgateway.NotifyRequest{
			Notification: pushgateway.Notification{
				Content: event.Content(),
				Counts: &pushgateway.Counts{
					Unread: userNumUnreadNotifs,
				},
				Devices:  devices,
				EventID:  event.EventID(),
				ID:       event.EventID(),
				FrameID:   event.FrameID(),
				FrameName: frameName,
				Sender:   sender.String(),
				Type:     event.Type(),
			},
		}
		if mem, memberErr := event.Membership(); memberErr == nil {
			req.Notification.Membership = mem
		}
		userID, err := spec.NewUserID(fmt.Sprintf("@%s:%s", localpart, s.cfg.Matrix.ServerName), true)
		if err != nil {
			logger.WithError(err).Errorf("failed to convert local user to userID %s", localpart)
			return nil, err
		}
		frameID, err := spec.NewFrameID(event.FrameID())
		if err != nil {
			logger.WithError(err).Errorf("event frameID is invalid %s", event.FrameID())
			return nil, err
		}

		localSender, err := s.rsAPI.QuerySenderIDForUser(ctx, *frameID, *userID)
		if err != nil {
			logger.WithError(err).Errorf("failed to get local user senderID for frame %s: %s", userID.String(), event.FrameID())
			return nil, err
		} else if localSender == nil {
			logger.WithError(err).Errorf("failed to get local user senderID for frame %s: %s", userID.String(), event.FrameID())
			return nil, fmt.Errorf("no sender ID for user %s in %s", userID.String(), frameID.String())
		}
		if event.StateKey() != nil && *event.StateKey() == string(*localSender) {
			req.Notification.UserIsTarget = true
		}
	}

	logger.Tracef("Notifying push gateway %s", url)
	var res pushgateway.NotifyResponse
	if err := s.pgClient.Notify(ctx, url, &req, &res); err != nil {
		logger.WithError(err).Errorf("failed to notify push gateway %s", url)
		return nil, err
	}
	logger.WithField("num_rejected", len(res.Rejected)).Trace("Push gateway result")

	if len(res.Rejected) == 0 {
		return nil, nil
	}

	devMap := make(map[string]*pushgateway.Device, len(devices))
	for _, d := range devices {
		devMap[d.PushKey] = d
	}
	rejected := make([]*pushgateway.Device, 0, len(res.Rejected))
	for _, pushKey := range res.Rejected {
		d := devMap[pushKey]
		if d != nil {
			rejected = append(rejected, d)
		}
	}

	return rejected, nil
}

// deleteRejectedPushers deletes the pushers associated with the given devices.
func (s *OutputFrameEventConsumer) deleteRejectedPushers(ctx context.Context, devices []*pushgateway.Device, localpart string, serverName spec.ServerName) {
	log.WithFields(log.Fields{
		"localpart":   localpart,
		"app_id0":     devices[0].AppID,
		"num_devices": len(devices),
	}).Warnf("Deleting pushers rejected by the HTTP push gateway")

	for _, d := range devices {
		if err := s.db.RemovePusher(ctx, d.AppID, d.PushKey, localpart, serverName); err != nil {
			log.WithFields(log.Fields{
				"localpart": localpart,
			}).WithError(err).Errorf("Unable to delete rejected pusher")
		}
	}
}
