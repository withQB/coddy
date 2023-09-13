package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/apis/federationapi/producers"
	"github.com/withqb/coddy/apis/federationapi/types"
	syncTypes "github.com/withqb/coddy/apis/syncapi/types"
	userAPI "github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/servers/dataframe/api"
	rstypes "github.com/withqb/coddy/servers/dataframe/types"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

var (
	PDUCountTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dendrite",
			Subsystem: "federationapi",
			Name:      "recv_pdus",
			Help:      "Number of incoming PDUs from remote servers with labels for success",
		},
		[]string{"status"}, // 'success' or 'total'
	)
	EDUCountTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dendrite",
			Subsystem: "federationapi",
			Name:      "recv_edus",
			Help:      "Number of incoming EDUs from remote servers",
		},
	)
)

type TxnReq struct {
	xtools.Transaction
	rsAPI                  api.FederationDataframeAPI
	userAPI                userAPI.FederationUserAPI
	ourServerName          spec.ServerName
	keys                   xtools.JSONVerifier
	framesMu                *MutexByFrame
	producer               *producers.SyncAPIProducer
	inboundPresenceEnabled bool
}

func NewTxnReq(
	rsAPI api.FederationDataframeAPI,
	userAPI userAPI.FederationUserAPI,
	ourServerName spec.ServerName,
	keys xtools.JSONVerifier,
	framesMu *MutexByFrame,
	producer *producers.SyncAPIProducer,
	inboundPresenceEnabled bool,
	pdus []json.RawMessage,
	edus []xtools.EDU,
	origin spec.ServerName,
	transactionID xtools.TransactionID,
	destination spec.ServerName,
) TxnReq {
	t := TxnReq{
		rsAPI:                  rsAPI,
		userAPI:                userAPI,
		ourServerName:          ourServerName,
		keys:                   keys,
		framesMu:                framesMu,
		producer:               producer,
		inboundPresenceEnabled: inboundPresenceEnabled,
	}

	t.PDUs = pdus
	t.EDUs = edus
	t.Origin = origin
	t.TransactionID = transactionID
	t.Destination = destination

	return t
}

func (t *TxnReq) ProcessTransaction(ctx context.Context) (*fclient.RespSend, *xutil.JSONResponse) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if t.producer != nil {
			t.processEDUs(ctx)
		}
	}()

	results := make(map[string]fclient.PDUResult)
	frameVersions := make(map[string]xtools.FrameVersion)
	getFrameVersion := func(frameID string) xtools.FrameVersion {
		if v, ok := frameVersions[frameID]; ok {
			return v
		}
		frameVersion, err := t.rsAPI.QueryFrameVersionForFrame(ctx, frameID)
		if err != nil {
			xutil.GetLogger(ctx).WithError(err).Debug("Transaction: Failed to query frame version for frame", frameID)
			return ""
		}
		frameVersions[frameID] = frameVersion
		return frameVersion
	}

	for _, pdu := range t.PDUs {
		PDUCountTotal.WithLabelValues("total").Inc()
		var header struct {
			FrameID string `json:"frame_id"`
		}
		if err := json.Unmarshal(pdu, &header); err != nil {
			xutil.GetLogger(ctx).WithError(err).Debug("Transaction: Failed to extract frame ID from event")
			// We don't know the event ID at this point so we can't return the
			// failure in the PDU results
			continue
		}
		frameVersion := getFrameVersion(header.FrameID)
		verImpl, err := xtools.GetFrameVersion(frameVersion)
		if err != nil {
			continue
		}
		event, err := verImpl.NewEventFromUntrustedJSON(pdu)
		if err != nil {
			if _, ok := err.(xtools.BadJSONError); ok {
				// Frame version 6 states that homeservers should strictly enforce canonical JSON
				// on PDUs.
				//
				// This enforces that the entire transaction is rejected if a single bad PDU is
				// sent. It is unclear if this is the correct behaviour or not.
				//
				// See https://github.com/withqb/synapse/issues/7543
				return nil, &xutil.JSONResponse{
					Code: 400,
					JSON: spec.BadJSON("PDU contains bad JSON"),
				}
			}
			xutil.GetLogger(ctx).WithError(err).Debugf("Transaction: Failed to parse event JSON of event %s", string(pdu))
			continue
		}
		if event.Type() == spec.MFrameCreate && event.StateKeyEquals("") {
			continue
		}
		if api.IsServerBannedFromFrame(ctx, t.rsAPI, event.FrameID(), t.Origin) {
			results[event.EventID()] = fclient.PDUResult{
				Error: "Forbidden by server ACLs",
			}
			continue
		}
		if err = xtools.VerifyEventSignatures(ctx, event, t.keys, func(frameID spec.FrameID, senderID spec.SenderID) (*spec.UserID, error) {
			return t.rsAPI.QueryUserIDForSender(ctx, frameID, senderID)
		}); err != nil {
			xutil.GetLogger(ctx).WithError(err).Debugf("Transaction: Couldn't validate signature of event %q", event.EventID())
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		// pass the event to the dataframe which will do auth checks
		// If the event fail auth checks, gmsl.NotAllowed error will be returned which we be silently
		// discarded by the caller of this function
		if err = api.SendEvents(
			ctx,
			t.rsAPI,
			api.KindNew,
			[]*rstypes.HeaderedEvent{
				{PDU: event},
			},
			t.Destination,
			t.Origin,
			api.DoNotSendToOtherServers,
			nil,
			true,
		); err != nil {
			xutil.GetLogger(ctx).WithError(err).Errorf("Transaction: Couldn't submit event %q to input queue: %s", event.EventID(), err)
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		results[event.EventID()] = fclient.PDUResult{}
		PDUCountTotal.WithLabelValues("success").Inc()
	}

	wg.Wait()
	return &fclient.RespSend{PDUs: results}, nil
}

// nolint:gocyclo
func (t *TxnReq) processEDUs(ctx context.Context) {
	for _, e := range t.EDUs {
		EDUCountTotal.Inc()
		switch e.Type {
		case spec.MTyping:
			var typingPayload struct {
				FrameID string `json:"frame_id"`
				UserID string `json:"user_id"`
				Typing bool   `json:"typing"`
			}
			if err := json.Unmarshal(e.Content, &typingPayload); err != nil {
				xutil.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal typing event")
				continue
			}
			if _, serverName, err := xtools.SplitID('@', typingPayload.UserID); err != nil {
				continue
			} else if serverName == t.ourServerName {
				continue
			} else if serverName != t.Origin {
				continue
			}
			if err := t.producer.SendTyping(ctx, typingPayload.UserID, typingPayload.FrameID, typingPayload.Typing, 30*1000); err != nil {
				xutil.GetLogger(ctx).WithError(err).Error("Failed to send typing event to JetStream")
			}
		case spec.MDirectToDevice:
			var directPayload xtools.ToDeviceMessage
			if err := json.Unmarshal(e.Content, &directPayload); err != nil {
				xutil.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal send-to-device events")
				continue
			}
			if _, serverName, err := xtools.SplitID('@', directPayload.Sender); err != nil {
				continue
			} else if serverName == t.ourServerName {
				continue
			} else if serverName != t.Origin {
				continue
			}
			for userID, byUser := range directPayload.Messages {
				for deviceID, message := range byUser {
					// TODO: check that the user and the device actually exist here
					if err := t.producer.SendToDevice(ctx, directPayload.Sender, userID, deviceID, directPayload.Type, message); err != nil {
						sentry.CaptureException(err)
						xutil.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{
							"sender":    directPayload.Sender,
							"user_id":   userID,
							"device_id": deviceID,
						}).Error("Failed to send send-to-device event to JetStream")
					}
				}
			}
		case spec.MDeviceListUpdate:
			if err := t.producer.SendDeviceListUpdate(ctx, e.Content, t.Origin); err != nil {
				sentry.CaptureException(err)
				xutil.GetLogger(ctx).WithError(err).Error("failed to InputDeviceListUpdate")
			}
		case spec.MReceipt:
			payload := map[string]types.FederationReceiptMRead{}

			if err := json.Unmarshal(e.Content, &payload); err != nil {
				xutil.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal receipt event")
				continue
			}

			for frameID, receipt := range payload {
				for userID, mread := range receipt.User {
					_, domain, err := xtools.SplitID('@', userID)
					if err != nil {
						xutil.GetLogger(ctx).WithError(err).Debug("Failed to split domain from receipt event sender")
						continue
					}
					if t.Origin != domain {
						xutil.GetLogger(ctx).Debugf("Dropping receipt event where sender domain (%q) doesn't match origin (%q)", domain, t.Origin)
						continue
					}
					if err := t.processReceiptEvent(ctx, userID, frameID, "m.read", mread.Data.TS, mread.EventIDs); err != nil {
						xutil.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{
							"sender":  t.Origin,
							"user_id": userID,
							"frame_id": frameID,
							"events":  mread.EventIDs,
						}).Error("Failed to send receipt event to JetStream")
						continue
					}
				}
			}
		case types.MSigningKeyUpdate:
			if err := t.producer.SendSigningKeyUpdate(ctx, e.Content, t.Origin); err != nil {
				sentry.CaptureException(err)
				logrus.WithError(err).Errorf("Failed to process signing key update")
			}
		case spec.MPresence:
			if t.inboundPresenceEnabled {
				if err := t.processPresence(ctx, e); err != nil {
					logrus.WithError(err).Errorf("Failed to process presence update")
				}
			}
		default:
			xutil.GetLogger(ctx).WithField("type", e.Type).Debug("Unhandled EDU")
		}
	}
}

// processPresence handles m.receipt events
func (t *TxnReq) processPresence(ctx context.Context, e xtools.EDU) error {
	payload := types.Presence{}
	if err := json.Unmarshal(e.Content, &payload); err != nil {
		return err
	}
	for _, content := range payload.Push {
		if _, serverName, err := xtools.SplitID('@', content.UserID); err != nil {
			continue
		} else if serverName == t.ourServerName {
			continue
		} else if serverName != t.Origin {
			continue
		}
		presence, ok := syncTypes.PresenceFromString(content.Presence)
		if !ok {
			continue
		}
		if err := t.producer.SendPresence(ctx, content.UserID, presence, content.StatusMsg, content.LastActiveAgo); err != nil {
			return err
		}
	}
	return nil
}

// processReceiptEvent sends receipt events to JetStream
func (t *TxnReq) processReceiptEvent(ctx context.Context,
	userID, frameID, receiptType string,
	timestamp spec.Timestamp,
	eventIDs []string,
) error {
	if _, serverName, err := xtools.SplitID('@', userID); err != nil {
		return nil
	} else if serverName == t.ourServerName {
		return nil
	} else if serverName != t.Origin {
		return nil
	}
	// store every event
	for _, eventID := range eventIDs {
		if err := t.producer.SendReceipt(ctx, userID, frameID, eventID, receiptType, timestamp); err != nil {
			return fmt.Errorf("unable to set receipt event: %w", err)
		}
	}

	return nil
}
