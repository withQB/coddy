package notifier

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/syncapi/storage"
	"github.com/withqb/coddy/apis/syncapi/types"
	"github.com/withqb/coddy/internal/sqlutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	rstypes "github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools/spec"
)

// NOTE: ALL FUNCTIONS IN THIS FILE PREFIXED WITH _ ARE NOT THREAD-SAFE
// AND MUST ONLY BE CALLED WHEN THE NOTIFIER LOCK IS HELD!

// Notifier will wake up sleeping requests when there is some new data.
// It does not tell requests what that data is, only the sync position which
// they can use to get at it. This is done to prevent races whereby we tell the caller
// the event, but the token has already advanced by the time they fetch it, resulting
// in missed events.
type Notifier struct {
	lock  *sync.RWMutex
	rsAPI api.SyncDataframeAPI
	// A map of FrameID => Set<UserID> : Must only be accessed by the OnNewEvent goroutine
	frameIDToJoinedUsers map[string]*userIDSet
	// A map of FrameID => Set<UserID> : Must only be accessed by the OnNewEvent goroutine
	frameIDToPeekingDevices map[string]peekingDeviceSet
	// The latest sync position
	currPos types.StreamingToken
	// A map of user_id => device_id => UserStream which can be used to wake a given user's /sync request.
	userDeviceStreams map[string]map[string]*UserDeviceStream
	// The last time we cleaned out stale entries from the userStreams map
	lastCleanUpTime time.Time
	// This map is reused to prevent allocations and GC pressure in SharedUsers.
	_sharedUserMap map[string]struct{}
	_wakeupUserMap map[string]struct{}
}

// NewNotifier creates a new notifier set to the given sync position.
// In order for this to be of any use, the Notifier needs to be told all frames and
// the joined users within each of them by calling Notifier.Load(*storage.SyncServerDatabase).
func NewNotifier(rsAPI api.SyncDataframeAPI) *Notifier {
	return &Notifier{
		rsAPI:                  rsAPI,
		frameIDToJoinedUsers:    make(map[string]*userIDSet),
		frameIDToPeekingDevices: make(map[string]peekingDeviceSet),
		userDeviceStreams:      make(map[string]map[string]*UserDeviceStream),
		lock:                   &sync.RWMutex{},
		lastCleanUpTime:        time.Now(),
		_sharedUserMap:         map[string]struct{}{},
		_wakeupUserMap:         map[string]struct{}{},
	}
}

// SetCurrentPosition sets the current streaming positions.
// This must be called directly after NewNotifier and initialising the streams.
func (n *Notifier) SetCurrentPosition(currPos types.StreamingToken) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos = currPos
}

// OnNewEvent is called when a new event is received from the frame server. Must only be
// called from a single goroutine, to avoid races between updates which could set the
// current sync position incorrectly.
// Chooses which user sync streams to update by a provided xtools.PDU
// (based on the users in the event's frame),
// a frameID directly, or a list of user IDs, prioritised by parameter ordering.
// posUpdate contains the latest position(s) for one or more types of events.
// If a position in posUpdate is 0, it means no updates are available of that type.
// Typically a consumer supplies a posUpdate with the latest sync position for the
// event type it handles, leaving other fields as 0.
func (n *Notifier) OnNewEvent(
	ev *rstypes.HeaderedEvent, frameID string, userIDs []string,
	posUpdate types.StreamingToken,
) {
	// update the current position then notify relevant /sync streams.
	// This needs to be done PRIOR to waking up users as they will read this value.
	n.lock.Lock()
	defer n.lock.Unlock()
	n.currPos.ApplyUpdates(posUpdate)
	n._removeEmptyUserStreams()

	if ev != nil {
		validFrameID, err := spec.NewFrameID(ev.FrameID())
		if err != nil {
			log.WithError(err).WithField("event_id", ev.EventID()).Errorf(
				"Notifier.OnNewEvent: FrameID is invalid",
			)
			return
		}
		// Map this event's frame_id to a list of joined users, and wake them up.
		usersToNotify := n._joinedUsers(ev.FrameID())
		// Map this event's frame_id to a list of peeking devices, and wake them up.
		peekingDevicesToNotify := n._peekingDevices(ev.FrameID())
		// If this is an invite, also add in the invitee to this list.
		if ev.Type() == "m.frame.member" && ev.StateKey() != nil {
			targetUserID, err := n.rsAPI.QueryUserIDForSender(context.Background(), *validFrameID, spec.SenderID(*ev.StateKey()))
			if err != nil || targetUserID == nil {
				log.WithError(err).WithField("event_id", ev.EventID()).Errorf(
					"Notifier.OnNewEvent: Failed to find the userID for this event",
				)
			} else {
				membership, err := ev.Membership()
				if err != nil {
					log.WithError(err).WithField("event_id", ev.EventID()).Errorf(
						"Notifier.OnNewEvent: Failed to unmarshal member event",
					)
				} else {
					// Keep the joined user map up-to-date
					switch membership {
					case spec.Invite:
						usersToNotify = append(usersToNotify, targetUserID.String())
					case spec.Join:
						// Manually append the new user's ID so they get notified
						// along all members in the frame
						usersToNotify = append(usersToNotify, targetUserID.String())
						n._addJoinedUser(ev.FrameID(), targetUserID.String())
					case spec.Leave:
						fallthrough
					case spec.Ban:
						n._removeJoinedUser(ev.FrameID(), targetUserID.String())
					}
				}
			}
		}

		n._wakeupUsers(usersToNotify, peekingDevicesToNotify, n.currPos)
	} else if frameID != "" {
		n._wakeupUsers(n._joinedUsers(frameID), n._peekingDevices(frameID), n.currPos)
	} else if len(userIDs) > 0 {
		n._wakeupUsers(userIDs, nil, n.currPos)
	} else {
		log.WithFields(log.Fields{
			"posUpdate": posUpdate.String,
		}).Warn("Notifier.OnNewEvent called but caller supplied no user to wake up")
	}
}

func (n *Notifier) OnNewAccountData(
	userID string, posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers([]string{userID}, nil, posUpdate)
}

func (n *Notifier) OnNewPeek(
	frameID, userID, deviceID string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._addPeekingDevice(frameID, userID, deviceID)

	// we don't wake up devices here given the dataframe consumer will do this shortly afterwards
	// by calling OnNewEvent.
}

func (n *Notifier) OnRetirePeek(
	frameID, userID, deviceID string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._removePeekingDevice(frameID, userID, deviceID)

	// we don't wake up devices here given the dataframe consumer will do this shortly afterwards
	// by calling OnRetireEvent.
}

func (n *Notifier) OnNewSendToDevice(
	userID string, deviceIDs []string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUserDevice(userID, deviceIDs, n.currPos)
}

// OnNewReceipt updates the current position
func (n *Notifier) OnNewTyping(
	frameID string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers(n._joinedUsers(frameID), nil, n.currPos)
}

// OnNewReceipt updates the current position
func (n *Notifier) OnNewReceipt(
	frameID string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers(n._joinedUsers(frameID), nil, n.currPos)
}

func (n *Notifier) OnNewKeyChange(
	posUpdate types.StreamingToken, wakeUserID, keyChangeUserID string,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers([]string{wakeUserID}, nil, n.currPos)
}

func (n *Notifier) OnNewInvite(
	posUpdate types.StreamingToken, wakeUserID string,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers([]string{wakeUserID}, nil, n.currPos)
}

func (n *Notifier) OnNewNotificationData(
	userID string,
	posUpdate types.StreamingToken,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	n._wakeupUsers([]string{userID}, nil, n.currPos)
}

func (n *Notifier) OnNewPresence(
	posUpdate types.StreamingToken, userID string,
) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.currPos.ApplyUpdates(posUpdate)
	sharedUsers := n._sharedUsers(userID)
	sharedUsers = append(sharedUsers, userID)

	n._wakeupUsers(sharedUsers, nil, n.currPos)
}

func (n *Notifier) SharedUsers(userID string) []string {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n._sharedUsers(userID)
}

func (n *Notifier) _sharedUsers(userID string) []string {
	n._sharedUserMap[userID] = struct{}{}
	for frameID, users := range n.frameIDToJoinedUsers {
		if ok := users.isIn(userID); !ok {
			continue
		}
		for _, userID := range n._joinedUsers(frameID) {
			n._sharedUserMap[userID] = struct{}{}
		}
	}
	sharedUsers := make([]string, 0, len(n._sharedUserMap)+1)
	for userID := range n._sharedUserMap {
		sharedUsers = append(sharedUsers, userID)
		delete(n._sharedUserMap, userID)
	}
	return sharedUsers
}

func (n *Notifier) IsSharedUser(userA, userB string) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	var okA, okB bool
	for _, users := range n.frameIDToJoinedUsers {
		okA = users.isIn(userA)
		if !okA {
			continue
		}
		okB = users.isIn(userB)
		if okA && okB {
			return true
		}
	}
	return false
}

// GetListener returns a UserStreamListener that can be used to wait for
// updates for a user. Must be closed.
// notify for anything before sincePos
func (n *Notifier) GetListener(req types.SyncRequest) UserDeviceStreamListener {
	// Do what synapse does: https://github.com/withqb/synapse/blob/v0.20.0/synapse/notifier.py#L298
	// - Bucket request into a lookup map keyed off a list of joined frame IDs and separately a user ID
	// - Incoming events wake requests for a matching frame ID
	// - Incoming events wake requests for a matching user ID (needed for invites)

	// TODO: v1 /events 'peeking' has an 'explicit frame ID' which is also tracked,
	//       but given we don't do /events, let's pretend it doesn't exist.

	n.lock.Lock()
	defer n.lock.Unlock()

	n._removeEmptyUserStreams()

	return n._fetchUserDeviceStream(req.Device.UserID, req.Device.ID, true).GetListener(req.Context)
}

// Load the membership states required to notify users correctly.
func (n *Notifier) Load(ctx context.Context, db storage.Database) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	snapshot, err := db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return err
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	frameToUsers, err := snapshot.AllJoinedUsersInFrames(ctx)
	if err != nil {
		return err
	}
	n.setUsersJoinedToFrames(frameToUsers)

	frameToPeekingDevices, err := snapshot.AllPeekingDevicesInFrames(ctx)
	if err != nil {
		return err
	}
	n.setPeekingDevices(frameToPeekingDevices)

	succeeded = true
	return nil
}

// LoadFrames loads the membership states required to notify users correctly.
func (n *Notifier) LoadFrames(ctx context.Context, db storage.Database, frameIDs []string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	snapshot, err := db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return err
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	frameToUsers, err := snapshot.AllJoinedUsersInFrame(ctx, frameIDs)
	if err != nil {
		return err
	}
	n.setUsersJoinedToFrames(frameToUsers)

	succeeded = true
	return nil
}

// CurrentPosition returns the current sync position
func (n *Notifier) CurrentPosition() types.StreamingToken {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.currPos
}

// setUsersJoinedToFrames marks the given users as 'joined' to the given frames, such that new events from
// these frames will wake the given users /sync requests. This should be called prior to ANY calls to
// OnNewEvent (eg on startup) to prevent racing.
func (n *Notifier) setUsersJoinedToFrames(frameIDToUserIDs map[string][]string) {
	// This is just the bulk form of addJoinedUser
	for frameID, userIDs := range frameIDToUserIDs {
		if _, ok := n.frameIDToJoinedUsers[frameID]; !ok {
			n.frameIDToJoinedUsers[frameID] = newUserIDSet(len(userIDs))
		}
		for _, userID := range userIDs {
			n.frameIDToJoinedUsers[frameID].add(userID)
		}
		n.frameIDToJoinedUsers[frameID].precompute()
	}
}

// setPeekingDevices marks the given devices as peeking in the given frames, such that new events from
// these frames will wake the given devices' /sync requests. This should be called prior to ANY calls to
// OnNewEvent (eg on startup) to prevent racing.
func (n *Notifier) setPeekingDevices(frameIDToPeekingDevices map[string][]types.PeekingDevice) {
	// This is just the bulk form of addPeekingDevice
	for frameID, peekingDevices := range frameIDToPeekingDevices {
		if _, ok := n.frameIDToPeekingDevices[frameID]; !ok {
			n.frameIDToPeekingDevices[frameID] = make(peekingDeviceSet, len(peekingDevices))
		}
		for _, peekingDevice := range peekingDevices {
			n.frameIDToPeekingDevices[frameID].add(peekingDevice)
		}
	}
}

// _wakeupUsers will wake up the sync strems for all of the devices for all of the
// specified user IDs, and also the specified peekingDevices
func (n *Notifier) _wakeupUsers(userIDs []string, peekingDevices []types.PeekingDevice, newPos types.StreamingToken) {
	for _, userID := range userIDs {
		n._wakeupUserMap[userID] = struct{}{}
	}
	for userID := range n._wakeupUserMap {
		for _, stream := range n._fetchUserStreams(userID) {
			if stream == nil {
				continue
			}
			stream.Broadcast(newPos) // wake up all goroutines Wait()ing on this stream
		}
		delete(n._wakeupUserMap, userID)
	}

	for _, peekingDevice := range peekingDevices {
		// TODO: don't bother waking up for devices whose users we already woke up
		if stream := n._fetchUserDeviceStream(peekingDevice.UserID, peekingDevice.DeviceID, false); stream != nil {
			stream.Broadcast(newPos) // wake up all goroutines Wait()ing on this stream
		}
	}
}

// _wakeupUserDevice will wake up the sync stream for a specific user device. Other
// device streams will be left alone.
// nolint:unused
func (n *Notifier) _wakeupUserDevice(userID string, deviceIDs []string, newPos types.StreamingToken) {
	for _, deviceID := range deviceIDs {
		if stream := n._fetchUserDeviceStream(userID, deviceID, false); stream != nil {
			stream.Broadcast(newPos) // wake up all goroutines Wait()ing on this stream
		}
	}
}

// _fetchUserDeviceStream retrieves a stream unique to the given device. If makeIfNotExists is true,
// a stream will be made for this device if one doesn't exist and it will be returned. This
// function does not wait for data to be available on the stream.
func (n *Notifier) _fetchUserDeviceStream(userID, deviceID string, makeIfNotExists bool) *UserDeviceStream {
	_, ok := n.userDeviceStreams[userID]
	if !ok {
		if !makeIfNotExists {
			return nil
		}
		n.userDeviceStreams[userID] = map[string]*UserDeviceStream{}
	}
	stream, ok := n.userDeviceStreams[userID][deviceID]
	if !ok {
		if !makeIfNotExists {
			return nil
		}
		// TODO: Unbounded growth of streams (1 per user)
		if stream = NewUserDeviceStream(userID, deviceID, n.currPos); stream != nil {
			n.userDeviceStreams[userID][deviceID] = stream
		}
	}
	return stream
}

// _fetchUserStreams retrieves all streams for the given user. If makeIfNotExists is true,
// a stream will be made for this user if one doesn't exist and it will be returned. This
// function does not wait for data to be available on the stream.
func (n *Notifier) _fetchUserStreams(userID string) []*UserDeviceStream {
	user, ok := n.userDeviceStreams[userID]
	if !ok {
		return []*UserDeviceStream{}
	}
	streams := make([]*UserDeviceStream, 0, len(user))
	for _, stream := range user {
		streams = append(streams, stream)
	}
	return streams
}

func (n *Notifier) _addJoinedUser(frameID, userID string) {
	if _, ok := n.frameIDToJoinedUsers[frameID]; !ok {
		n.frameIDToJoinedUsers[frameID] = newUserIDSet(8)
	}
	n.frameIDToJoinedUsers[frameID].add(userID)
	n.frameIDToJoinedUsers[frameID].precompute()
}

func (n *Notifier) _removeJoinedUser(frameID, userID string) {
	if _, ok := n.frameIDToJoinedUsers[frameID]; !ok {
		n.frameIDToJoinedUsers[frameID] = newUserIDSet(8)
	}
	n.frameIDToJoinedUsers[frameID].remove(userID)
	n.frameIDToJoinedUsers[frameID].precompute()
}

func (n *Notifier) JoinedUsers(frameID string) (userIDs []string) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n._joinedUsers(frameID)
}

func (n *Notifier) _joinedUsers(frameID string) (userIDs []string) {
	if _, ok := n.frameIDToJoinedUsers[frameID]; !ok {
		return
	}
	return n.frameIDToJoinedUsers[frameID].values()
}

func (n *Notifier) _addPeekingDevice(frameID, userID, deviceID string) {
	if _, ok := n.frameIDToPeekingDevices[frameID]; !ok {
		n.frameIDToPeekingDevices[frameID] = make(peekingDeviceSet)
	}
	n.frameIDToPeekingDevices[frameID].add(types.PeekingDevice{UserID: userID, DeviceID: deviceID})
}

func (n *Notifier) _removePeekingDevice(frameID, userID, deviceID string) {
	if _, ok := n.frameIDToPeekingDevices[frameID]; !ok {
		n.frameIDToPeekingDevices[frameID] = make(peekingDeviceSet)
	}
	// XXX: is this going to work as a key?
	n.frameIDToPeekingDevices[frameID].remove(types.PeekingDevice{UserID: userID, DeviceID: deviceID})
}

func (n *Notifier) PeekingDevices(frameID string) (peekingDevices []types.PeekingDevice) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n._peekingDevices(frameID)
}

func (n *Notifier) _peekingDevices(frameID string) (peekingDevices []types.PeekingDevice) {
	if _, ok := n.frameIDToPeekingDevices[frameID]; !ok {
		return
	}
	return n.frameIDToPeekingDevices[frameID].values()
}

// _removeEmptyUserStreams iterates through the user stream map and removes any
// that have been empty for a certain amount of time. This is a crude way of
// ensuring that the userStreams map doesn't grow forver.
// This should be called when the notifier gets called for whatever reason,
// the function itself is responsible for ensuring it doesn't iterate too
// often.
func (n *Notifier) _removeEmptyUserStreams() {
	// Only clean up  now and again
	now := time.Now()
	if n.lastCleanUpTime.Add(time.Minute).After(now) {
		return
	}
	n.lastCleanUpTime = now

	deleteBefore := now.Add(-5 * time.Minute)
	for user, byUser := range n.userDeviceStreams {
		for device, stream := range byUser {
			if stream.TimeOfLastNonEmpty().Before(deleteBefore) {
				delete(n.userDeviceStreams[user], device)
			}
			if len(n.userDeviceStreams[user]) == 0 {
				delete(n.userDeviceStreams, user)
			}
		}
	}
}

// A string set, mainly existing for improving clarity of structs in this file.
type userIDSet struct {
	sync.Mutex
	set         map[string]struct{}
	precomputed []string
}

func newUserIDSet(cap int) *userIDSet {
	return &userIDSet{
		set:         make(map[string]struct{}, cap),
		precomputed: nil,
	}
}

func (s *userIDSet) add(str string) {
	s.Lock()
	defer s.Unlock()
	s.set[str] = struct{}{}
	s.precomputed = s.precomputed[:0] // invalidate cache
}

func (s *userIDSet) remove(str string) {
	s.Lock()
	defer s.Unlock()
	delete(s.set, str)
	s.precomputed = s.precomputed[:0] // invalidate cache
}

func (s *userIDSet) precompute() {
	s.Lock()
	defer s.Unlock()
	s.precomputed = s.values()
}

func (s *userIDSet) isIn(str string) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := s.set[str]
	return ok
}

func (s *userIDSet) values() (vals []string) {
	if len(s.precomputed) > 0 {
		return s.precomputed // only return if not invalidated
	}
	vals = make([]string, 0, len(s.set))
	for str := range s.set {
		vals = append(vals, str)
	}
	return
}

// A set of PeekingDevices, similar to userIDSet

type peekingDeviceSet map[types.PeekingDevice]struct{}

func (s peekingDeviceSet) add(d types.PeekingDevice) {
	s[d] = struct{}{}
}

// nolint:unused
func (s peekingDeviceSet) remove(d types.PeekingDevice) {
	delete(s, d)
}

func (s peekingDeviceSet) values() (vals []types.PeekingDevice) {
	vals = make([]types.PeekingDevice, 0, len(s))
	for d := range s {
		vals = append(vals, d)
	}
	return
}
