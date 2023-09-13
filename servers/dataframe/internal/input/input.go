// Package input contains the code processes new frame events
package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	userapi "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"

	"github.com/Arceliar/phony"
	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"

	fedapi "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/servers/dataframe/acls"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/query"
	"github.com/withqb/coddy/servers/dataframe/producers"
	"github.com/withqb/coddy/servers/dataframe/storage"
	"github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// Inputer is responsible for consuming from the dataframe input
// streams and processing the events. All input events are queued
// into a single NATS stream and the order is preserved strictly.
// The `frame_id` message header will contain the frame ID which will
// be used to assign the pending event to a per-frame worker.
//
// The input API maintains an ephemeral headers-only consumer. It
// will speed through the stream working out which frame IDs are
// pending and create durable consumers for them. The durable
// consumer will then be used for each frame worker goroutine to
// fetch events one by one and process them. Each frame having a
// durable consumer of its own means there is no head-of-line
// blocking between frames. Filtering ensures that each durable
// consumer only receives events for the frame it is interested in.
//
// The ephemeral consumer closely tracks the newest events. The
// per-frame durable consumers will only progress through the stream
// as events are processed.
//
//	      A BC *  -> positions of each consumer (* = ephemeral)
//	      ⌄ ⌄⌄ ⌄
//	ABAABCAABCAA  -> newest (letter = subject for each message)
//
// In this example, A is still processing an event but has two
// pending events to process afterwards. Both B and C are caught
// up, so they will do nothing until a new event comes in for B
// or C.
type Inputer struct {
	Cfg                 *config.DataFrame
	ProcessContext      *process.ProcessContext
	DB                  storage.FrameDatabase
	NATSClient          *nats.Conn
	JetStream           nats.JetStreamContext
	Durable             nats.SubOpt
	ServerName          spec.ServerName
	SigningIdentity     func(ctx context.Context, frameID spec.FrameID, senderID spec.UserID) (fclient.SigningIdentity, error)
	FSAPI               fedapi.DataframeFederationAPI
	RSAPI               api.DataframeInternalAPI
	KeyRing             xtools.JSONVerifier
	ACLs                *acls.ServerACLs
	InputFrameEventTopic string
	OutputProducer      *producers.FrameEventProducer
	workers             sync.Map // frame ID -> *worker

	Queryer       *query.Queryer
	UserAPI       userapi.DataframeUserAPI
	EnableMetrics bool
}

// If a frame consumer is inactive for a while then we will allow NATS
// to clean it up. This stops us from holding onto durable consumers
// indefinitely for frames that might no longer be active, since they do
// have an interest overhead in the NATS Server. If the frame becomes
// active again then we'll recreate the consumer anyway.
const inactiveThreshold = time.Hour * 24

type worker struct {
	phony.Inbox
	sync.Mutex
	r            *Inputer
	frameID       string
	subscription *nats.Subscription
}

func (r *Inputer) startWorkerForFrame(frameID string) {
	v, loaded := r.workers.LoadOrStore(frameID, &worker{
		r:      r,
		frameID: frameID,
	})
	w := v.(*worker)
	w.Lock()
	defer w.Unlock()
	if !loaded || w.subscription == nil {
		consumer := r.Cfg.Matrix.JetStream.Prefixed("FrameInput" + jetstream.Tokenise(w.frameID))
		subject := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputFrameEventSubj(w.frameID))

		// Create the consumer. We do this as a specific step rather than
		// letting PullSubscribe create it for us because we need the consumer
		// to outlive the subscription. If we do it this way, we can Bind in the
		// next step, and when we Unsubscribe, the consumer continues to live. If
		// we leave PullSubscribe to create the durable consumer, Unsubscribe will
		// delete it because it thinks it "owns" it, which in turn breaks the
		// interest-based retention storage policy.
		// If the durable consumer already exists, this is effectively a no-op.
		// Another interesting tid-bit here: the ACK policy is set to "all" so that
		// if we acknowledge a message, we also acknowledge everything that comes
		// before it. This is necessary because otherwise our consumer will never
		// acknowledge things we filtered out for other subjects and therefore they
		// will linger around forever.
		if _, err := w.r.JetStream.AddConsumer(
			r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputFrameEvent),
			&nats.ConsumerConfig{
				Durable:           consumer,
				AckPolicy:         nats.AckAllPolicy,
				DeliverPolicy:     nats.DeliverAllPolicy,
				FilterSubject:     subject,
				AckWait:           MaximumMissingProcessingTime + (time.Second * 10),
				InactiveThreshold: inactiveThreshold,
			},
		); err != nil {
			logrus.WithError(err).Errorf("failed to create consumer for frame %q", w.frameID)
			return
		}

		// Bind to our durable consumer. We want to receive all messages waiting
		// for this subject and we want to manually acknowledge them, so that we
		// can ensure they are only cleaned up when we are done processing them.
		sub, err := w.r.JetStream.PullSubscribe(
			subject, consumer,
			nats.ManualAck(),
			nats.DeliverAll(),
			nats.AckWait(MaximumMissingProcessingTime+(time.Second*10)),
			nats.Bind(r.InputFrameEventTopic, consumer),
			nats.InactiveThreshold(inactiveThreshold),
		)
		if err != nil {
			logrus.WithError(err).Errorf("failed to subscribe to stream for frame %q", w.frameID)
			return
		}

		// Go and start pulling messages off the queue.
		w.subscription = sub
		w.Act(nil, w._next)
	}
}

// Start creates an ephemeral non-durable consumer on the dataframe
// input topic. It is configured to deliver us headers only because we
// don't actually care about the contents of the message at this point,
// we only care about the `frame_id` field. Once a message arrives, we
// will look to see if we have a worker for that frame which has its
// own consumer. If we don't, we'll start one.
func (r *Inputer) Start() error {
	if r.EnableMetrics {
		prometheus.MustRegister(dataframeInputBackpressure, processFrameEventDuration)
	}
	_, err := r.JetStream.Subscribe(
		"", // This is blank because we specified it in BindStream.
		func(m *nats.Msg) {
			frameID := m.Header.Get(jetstream.FrameID)
			r.startWorkerForFrame(frameID)
			_ = m.Ack()
		},
		nats.HeadersOnly(),
		nats.DeliverAll(),
		nats.AckExplicit(),
		nats.ReplayInstant(),
		nats.BindStream(r.InputFrameEventTopic),
	)

	// Make sure that the frame consumers have the right config.
	stream := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputFrameEvent)
	for consumer := range r.JetStream.Consumers(stream) {
		switch {
		case consumer.Config.Durable == "":
			continue // Ignore ephemeral consumers
		case consumer.Config.InactiveThreshold != inactiveThreshold:
			consumer.Config.InactiveThreshold = inactiveThreshold
			if _, cerr := r.JetStream.UpdateConsumer(stream, &consumer.Config); cerr != nil {
				logrus.WithError(cerr).Warnf("failed to update inactive threshold on consumer %q", consumer.Name)
			}
		}
	}

	return err
}

// _next is called by the worker for the frame. It must only be called
// by the actor embedded into the worker.
func (w *worker) _next() {
	// Look up what the next event is that's waiting to be processed.
	ctx, cancel := context.WithTimeout(w.r.ProcessContext.Context(), time.Minute)
	defer cancel()
	if scope := sentry.CurrentHub().Scope(); scope != nil {
		scope.SetTag("frame_id", w.frameID)
	}
	msgs, err := w.subscription.Fetch(1, nats.Context(ctx))
	switch err {
	case nil:
		// Make sure that once we're done here, we queue up another call
		// to _next in the inbox.
		defer w.Act(nil, w._next)

		// If no error was reported, but we didn't get exactly one message,
		// then skip over this and try again on the next iteration.
		if len(msgs) != 1 {
			return
		}

	case context.DeadlineExceeded, context.Canceled:
		// The context exceeded, so we've been waiting for more than a
		// minute for activity in this frame. At this point we will shut
		// down the subscriber to free up resources. It'll get started
		// again if new activity happens.
		if err = w.subscription.Unsubscribe(); err != nil {
			logrus.WithError(err).Errorf("failed to unsubscribe to stream for frame %q", w.frameID)
		}
		w.Lock()
		w.subscription = nil
		w.Unlock()
		return

	default:
		// Something went wrong while trying to fetch the next event
		// from the queue. In which case, we'll shut down the subscriber
		// and wait to be notified about new frame activity again. Maybe
		// the problem will be corrected by then.
		logrus.WithError(err).Errorf("failed to get next stream message for frame %q", w.frameID)
		if err = w.subscription.Unsubscribe(); err != nil {
			logrus.WithError(err).Errorf("failed to unsubscribe to stream for frame %q", w.frameID)
		}
		w.Lock()
		w.subscription = nil
		w.Unlock()
		return
	}

	// Try to unmarshal the input frame event. If the JSON unmarshalling
	// fails then we'll terminate the message — this notifies NATS that
	// we are done with the message and never want to see it again.
	msg := msgs[0]
	var inputFrameEvent api.InputFrameEvent
	if err = json.Unmarshal(msg.Data, &inputFrameEvent); err != nil {
		_ = msg.Term()
		return
	}

	if scope := sentry.CurrentHub().Scope(); scope != nil {
		scope.SetTag("event_id", inputFrameEvent.Event.EventID())
	}
	dataframeInputBackpressure.With(prometheus.Labels{"frame_id": w.frameID}).Inc()
	defer dataframeInputBackpressure.With(prometheus.Labels{"frame_id": w.frameID}).Dec()

	// Process the frame event. If something goes wrong then we'll tell
	// NATS to terminate the message. We'll store the error result as
	// a string, because we might want to return that to the caller if
	// it was a synchronous request.
	var errString string
	if err = w.r.processFrameEvent(
		w.r.ProcessContext.Context(),
		spec.ServerName(msg.Header.Get("virtual_host")),
		&inputFrameEvent,
	); err != nil {
		switch err.(type) {
		case types.RejectedError:
			// Don't send events that were rejected to Sentry
			logrus.WithError(err).WithFields(logrus.Fields{
				"frame_id":  w.frameID,
				"event_id": inputFrameEvent.Event.EventID(),
				"type":     inputFrameEvent.Event.Type(),
			}).Warn("Dataframe rejected event")
		default:
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				sentry.CaptureException(err)
			}
			logrus.WithError(err).WithFields(logrus.Fields{
				"frame_id":  w.frameID,
				"event_id": inputFrameEvent.Event.EventID(),
				"type":     inputFrameEvent.Event.Type(),
			}).Warn("Dataframe failed to process event")
		}
		_ = msg.Term()
		errString = err.Error()
	} else {
		_ = msg.Ack()
	}

	// If it was a synchronous input request then the "sync" field
	// will be present in the message. That means that someone is
	// waiting for a response. The temporary inbox name is present in
	// that field, so send back the error string (if any). If there
	// was no error then we'll return a blank message, which means
	// that everything was OK.
	if replyTo := msg.Header.Get("sync"); replyTo != "" {
		if err = w.r.NATSClient.Publish(replyTo, []byte(errString)); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"frame_id":  w.frameID,
				"event_id": inputFrameEvent.Event.EventID(),
				"type":     inputFrameEvent.Event.Type(),
			}).Warn("Dataframe failed to respond for sync event")
		}
	}
}

// queueInputFrameEvents queues events into the dataframe input
// stream in NATS.
func (r *Inputer) queueInputFrameEvents(
	ctx context.Context,
	request *api.InputFrameEventsRequest,
) (replySub *nats.Subscription, err error) {
	// If the request is synchronous then we need to create a
	// temporary inbox to wait for responses on, and then create
	// a subscription to it. If it's asynchronous then we won't
	// bother, so these values will remain empty.
	var replyTo string
	if !request.Asynchronous {
		replyTo = nats.NewInbox()
		replySub, err = r.NATSClient.SubscribeSync(replyTo)
		if err != nil {
			return nil, fmt.Errorf("r.NATSClient.SubscribeSync: %w", err)
		}
		if replySub == nil {
			// This shouldn't ever happen, but it doesn't hurt to check
			// because we can potentially avoid a nil pointer panic later
			// if it did for some reason.
			return nil, fmt.Errorf("expected a subscription to the temporary inbox")
		}
	}

	// For each event, marshal the input frame event and then
	// send it into the input queue.
	for _, e := range request.InputFrameEvents {
		frameID := e.Event.FrameID()
		subj := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputFrameEventSubj(frameID))
		msg := &nats.Msg{
			Subject: subj,
			Header:  nats.Header{},
		}
		msg.Header.Set("frame_id", frameID)
		if replyTo != "" {
			msg.Header.Set("sync", replyTo)
		}
		msg.Header.Set("virtual_host", string(request.VirtualHost))
		msg.Data, err = json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %w", err)
		}
		if _, err = r.JetStream.PublishMsg(msg, nats.Context(ctx)); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"frame_id":  frameID,
				"event_id": e.Event.EventID(),
				"subj":     subj,
			}).Error("Dataframe failed to queue async event")
			return nil, fmt.Errorf("r.JetStream.PublishMsg: %w", err)
		}
	}
	return
}

// InputFrameEvents implements api.DataframeInternalAPI
func (r *Inputer) InputFrameEvents(
	ctx context.Context,
	request *api.InputFrameEventsRequest,
	response *api.InputFrameEventsResponse,
) {
	// Queue up the event into the dataframe.
	replySub, err := r.queueInputFrameEvents(ctx, request)
	if err != nil {
		response.ErrMsg = err.Error()
		return
	}

	// If we aren't waiting for synchronous responses then we can
	// give up here, there is nothing further to do.
	if replySub == nil {
		return
	}

	// Otherwise, we'll want to sit and wait for the responses
	// from the dataframe. There will be one response for every
	// input we submitted. The last error value we receive will
	// be the one returned as the error string.
	defer replySub.Drain() // nolint:errcheck
	for i := 0; i < len(request.InputFrameEvents); i++ {
		msg, err := replySub.NextMsgWithContext(ctx)
		if err != nil {
			response.ErrMsg = err.Error()
			return
		}
		if len(msg.Data) > 0 {
			response.ErrMsg = string(msg.Data)
		}
	}
}

var dataframeInputBackpressure = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "dendrite",
		Subsystem: "dataframe",
		Name:      "input_backpressure",
		Help:      "How many events are queued for input for a given frame",
	},
	[]string{"frame_id"},
)
