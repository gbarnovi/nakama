// Copyright 2018 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"github.com/heroiclabs/nakama-common/rtapi"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
)

// Deferred message expected to be batched with other deferred messages.
// All deferred messages in a batch are expected to be for the same stream/mode and share a logger context.
type DeferredMessage struct {
	PresenceIDs []*PresenceID
	Envelope    *rtapi.Envelope
	Reliable    bool
}

// MessageRouter is responsible for sending a message to a list of presences or to an entire stream.
type MessageRouter interface {
	SendToPresenceIDs(*zap.Logger, []*PresenceID, *rtapi.Envelope, bool)
	SendToStream(*zap.Logger, PresenceStream, *rtapi.Envelope, bool)
	SendDeferred(*zap.Logger, []*DeferredMessage)
	Share(logger *zap.Logger, data SSA) error
	SendToPresenceIDsNew(*zap.Logger, []*PresenceID, *rtapi.Envelope, bool, bool)
	SendToPresenceIDsNewA(*zap.Logger, []*PresenceID, *rtapi.Envelope, bool)
}

type LocalMessageRouter struct {
	protojsonMarshaler *protojson.MarshalOptions
	sessionRegistry    SessionRegistry
	tracker            Tracker
	redis              *redis.Client
}

type SSA struct {
	SenderHost  string          `json:"senderHost"`
	Envelope    *rtapi.Envelope `json:"envelope"`
	PresenceIDs []*PresenceID   `json:"presenceIDs"`
	Reliable    bool            `json:"reliable"`
}

func (r *LocalMessageRouter) SendToPresenceIDsNewA(logger *zap.Logger, ids []*PresenceID, envelope *rtapi.Envelope, b bool) {
	r.SendToPresenceIDsNew(logger, ids, envelope, b, false)
}

func (r *LocalMessageRouter) SendToPresenceIDsNew(logger *zap.Logger, presenceIDs []*PresenceID, envelope *rtapi.Envelope, reliable bool, gossip bool) {
	if gossip {
		if err := r.Share(logger, SSA{
			SenderHost:  os.Getenv("HOSTNAME"),
			Envelope:    envelope,
			PresenceIDs: presenceIDs,
			Reliable:    reliable,
		}); err != nil {
			logger.Error("error gossiping, %v", zap.Error(err))
		}
	}

	if len(presenceIDs) == 0 {
		return
	}

	// Prepare payload variables but do not initialize until we hit a session that needs them to avoid unnecessary work.
	var payloadProtobuf []byte
	var payloadJSON []byte

	for _, presenceID := range presenceIDs {
		session := r.sessionRegistry.Get(presenceID.SessionID)
		if session == nil {
			logger.Debug("No session to route to", zap.String("sid", presenceID.SessionID.String()))
			continue
		}

		var err error
		switch session.Format() {
		case SessionFormatProtobuf:
			if payloadProtobuf == nil {
				// Marshal the payload now that we know this format is needed.
				payloadProtobuf, err = proto.Marshal(envelope)
				if err != nil {
					logger.Error("Could not marshal message", zap.Error(err))
					return
				}
			}
			err = session.SendBytes(payloadProtobuf, reliable)
		case SessionFormatJson:
			fallthrough
		default:
			if payloadJSON == nil {
				// Marshal the payload now that we know this format is needed.
				if buf, err := r.protojsonMarshaler.Marshal(envelope); err == nil {
					payloadJSON = buf
				} else {
					logger.Error("Could not marshal message", zap.Error(err))
					return
				}
			}
			err = session.SendBytes(payloadJSON, reliable)
		}
		if err != nil {
			logger.Error("Failed to route message", zap.String("sid", presenceID.SessionID.String()), zap.Error(err))
		}
	}
}

func (r *LocalMessageRouter) Share(logger *zap.Logger, data SSA) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		logger.Error("error: ", zap.Error(err))
		return err
	}

	return r.redis.Publish(context.Background(), "sharing", encoded).Err()
}

func NewLocalMessageRouter(logger *zap.Logger, sessionRegistry SessionRegistry, tracker Tracker, protojsonMarshaler *protojson.MarshalOptions) MessageRouter {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "host.docker.internal:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	localMessageRouter := &LocalMessageRouter{
		protojsonMarshaler: protojsonMarshaler,
		sessionRegistry:    sessionRegistry,
		tracker:            tracker,
		redis:              rdb,
	}

	go func(localMessageRouter *LocalMessageRouter) {
		// There is no error because go-redis automatically reconnects on error.
		pubsub := rdb.Subscribe(context.Background(), "sharing")

		// Close the subscription when we are done.
		defer pubsub.Close()

		ch := pubsub.Channel()

		for msg := range ch {
			b := &SSA{}
			err := json.Unmarshal([]byte(msg.Payload), b)
			if err != nil {
				logger.Error("err: ", zap.Error(err))
			}

			localMessageRouter.SendToPresenceIDsNewA(logger, b.PresenceIDs, b.Envelope, b.Reliable)

		}
	}(localMessageRouter)

	return localMessageRouter
}

func (r *LocalMessageRouter) SendToPresenceIDs(logger *zap.Logger, presenceIDs []*PresenceID, envelope *rtapi.Envelope, reliable bool) {
	r.SendToPresenceIDsNew(logger, presenceIDs, envelope, reliable, true)
}

func (r *LocalMessageRouter) SendToStream(logger *zap.Logger, stream PresenceStream, envelope *rtapi.Envelope, reliable bool) {
	presenceIDs := r.tracker.ListPresenceIDByStream(stream)
	r.SendToPresenceIDs(logger, presenceIDs, envelope, reliable)
}

func (r *LocalMessageRouter) SendDeferred(logger *zap.Logger, messages []*DeferredMessage) {
	for _, message := range messages {
		r.SendToPresenceIDs(logger, message.PresenceIDs, message.Envelope, message.Reliable)
	}
}
