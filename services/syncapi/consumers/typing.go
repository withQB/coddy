// Copyright 2019 Alex Chen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumers

import (
	"context"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal/caching"
	"github.com/withqb/coddy/services/syncapi/notifier"
	"github.com/withqb/coddy/services/syncapi/streams"
	"github.com/withqb/coddy/services/syncapi/types"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// OutputTypingEventConsumer consumes events that originated in the EDU server.
type OutputTypingEventConsumer struct {
	ctx       context.Context
	jetstream nats.JetStreamContext
	durable   string
	topic     string
	eduCache  *caching.EDUCache
	stream    streams.StreamProvider
	notifier  *notifier.Notifier
}

// NewOutputTypingEventConsumer creates a new OutputTypingEventConsumer.
// Call Start() to begin consuming from the EDU server.
func NewOutputTypingEventConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	eduCache *caching.EDUCache,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputTypingEventConsumer {
	return &OutputTypingEventConsumer{
		ctx:       process.Context(),
		jetstream: js,
		topic:     cfg.Coddy.JetStream.Prefixed(jetstream.OutputTypingEvent),
		durable:   cfg.Coddy.JetStream.Durable("SyncAPITypingConsumer"),
		eduCache:  eduCache,
		notifier:  notifier,
		stream:    stream,
	}
}

// Start consuming typing events.
func (s *OutputTypingEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputTypingEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	frameID := msg.Header.Get(jetstream.FrameID)
	userID := msg.Header.Get(jetstream.UserID)
	typing, err := strconv.ParseBool(msg.Header.Get("typing"))
	if err != nil {
		log.WithError(err).Errorf("output log: typing parse failure")
		return true
	}
	timeout, err := strconv.Atoi(msg.Header.Get("timeout_ms"))
	if err != nil {
		log.WithError(err).Errorf("output log: timeout_ms parse failure")
		return true
	}

	log.WithFields(log.Fields{
		"frame_id": frameID,
		"user_id": userID,
		"typing":  typing,
		"timeout": timeout,
	}).Debug("syncapi received EDU data from client api")

	var typingPos types.StreamPosition
	if typing {
		expiry := time.Now().Add(time.Duration(timeout) * time.Millisecond)
		typingPos = types.StreamPosition(
			s.eduCache.AddTypingUser(userID, frameID, &expiry),
		)
	} else {
		typingPos = types.StreamPosition(
			s.eduCache.RemoveUser(userID, frameID),
		)
	}

	s.stream.Advance(typingPos)
	s.notifier.OnNewTyping(frameID, types.StreamingToken{TypingPosition: typingPos})

	return true
}
