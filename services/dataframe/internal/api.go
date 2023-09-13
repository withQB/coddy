package internal

import (
	"context"
	"crypto/ed25519"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"

	"github.com/withqb/coddy/internal/caching"
	asAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/coddy/services/dataframe/acls"
	"github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/internal/input"
	"github.com/withqb/coddy/services/dataframe/internal/perform"
	"github.com/withqb/coddy/services/dataframe/internal/query"
	"github.com/withqb/coddy/services/dataframe/producers"
	"github.com/withqb/coddy/services/dataframe/storage"
	"github.com/withqb/coddy/services/dataframe/types"
	fsAPI "github.com/withqb/coddy/services/federationapi/api"
	userapi "github.com/withqb/coddy/services/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/coddy/setup/jetstream"
	"github.com/withqb/coddy/setup/process"
)

// DataframeInternalAPI is an implementation of api.DataframeInternalAPI
type DataframeInternalAPI struct {
	*input.Inputer
	*query.Queryer
	*perform.Inviter
	*perform.Joiner
	*perform.Peeker
	*perform.InboundPeeker
	*perform.Unpeeker
	*perform.Leaver
	*perform.Publisher
	*perform.Backfiller
	*perform.Forgetter
	*perform.Upgrader
	*perform.Admin
	*perform.Creator
	ProcessContext         *process.ProcessContext
	DB                     storage.Database
	Cfg                    *config.Dendrite
	Cache                  caching.DataFrameCaches
	ServerName             spec.ServerName
	KeyRing                xtools.JSONVerifier
	ServerACLs             *acls.ServerACLs
	fsAPI                  fsAPI.DataframeFederationAPI
	asAPI                  asAPI.AppServiceInternalAPI
	NATSClient             *nats.Conn
	JetStream              nats.JetStreamContext
	Durable                string
	InputFrameEventTopic    string // JetStream topic for new input frame events
	OutputProducer         *producers.FrameEventProducer
	PerspectiveServerNames []spec.ServerName
	enableMetrics          bool
	defaultFrameVersion     xtools.FrameVersion
}

func NewDataframeAPI(
	processContext *process.ProcessContext, dendriteCfg *config.Dendrite, dataframeDB storage.Database,
	js nats.JetStreamContext, nc *nats.Conn, caches caching.DataFrameCaches, enableMetrics bool,
) *DataframeInternalAPI {
	var perspectiveServerNames []spec.ServerName
	for _, kp := range dendriteCfg.FederationAPI.KeyPerspectives {
		perspectiveServerNames = append(perspectiveServerNames, kp.ServerName)
	}

	serverACLs := acls.NewServerACLs(dataframeDB)
	producer := &producers.FrameEventProducer{
		Topic:     string(dendriteCfg.Global.JetStream.Prefixed(jetstream.OutputFrameEvent)),
		JetStream: js,
		ACLs:      serverACLs,
	}
	a := &DataframeInternalAPI{
		ProcessContext:         processContext,
		DB:                     dataframeDB,
		Cfg:                    dendriteCfg,
		Cache:                  caches,
		ServerName:             dendriteCfg.Global.ServerName,
		PerspectiveServerNames: perspectiveServerNames,
		InputFrameEventTopic:    dendriteCfg.Global.JetStream.Prefixed(jetstream.InputFrameEvent),
		OutputProducer:         producer,
		JetStream:              js,
		NATSClient:             nc,
		Durable:                dendriteCfg.Global.JetStream.Durable("DataframeInputConsumer"),
		ServerACLs:             serverACLs,
		enableMetrics:          enableMetrics,
		defaultFrameVersion:     dendriteCfg.DataFrame.DefaultFrameVersion,
		// perform-er structs + queryer struct get initialised when we have a federation sender to use
	}
	return a
}

// SetFederationInputAPI passes in a federation input API reference so that we can
// avoid the chicken-and-egg problem of both the dataframe input API and the
// federation input API being interdependent.
func (r *DataframeInternalAPI) SetFederationAPI(fsAPI fsAPI.DataframeFederationAPI, keyRing *xtools.KeyRing) {
	r.fsAPI = fsAPI
	r.KeyRing = keyRing

	r.Queryer = &query.Queryer{
		DB:                r.DB,
		Cache:             r.Cache,
		IsLocalServerName: r.Cfg.Global.IsLocalServerName,
		ServerACLs:        r.ServerACLs,
		Cfg:               r.Cfg,
		FSAPI:             fsAPI,
	}

	r.Inputer = &input.Inputer{
		Cfg:                 &r.Cfg.DataFrame,
		ProcessContext:      r.ProcessContext,
		DB:                  r.DB,
		InputFrameEventTopic: r.InputFrameEventTopic,
		OutputProducer:      r.OutputProducer,
		JetStream:           r.JetStream,
		NATSClient:          r.NATSClient,
		Durable:             nats.Durable(r.Durable),
		ServerName:          r.ServerName,
		SigningIdentity:     r.SigningIdentityFor,
		FSAPI:               fsAPI,
		RSAPI:               r,
		KeyRing:             keyRing,
		ACLs:                r.ServerACLs,
		Queryer:             r.Queryer,
		EnableMetrics:       r.enableMetrics,
	}
	r.Inviter = &perform.Inviter{
		DB:      r.DB,
		Cfg:     &r.Cfg.DataFrame,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
	}
	r.Joiner = &perform.Joiner{
		Cfg:     &r.Cfg.DataFrame,
		DB:      r.DB,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
		Queryer: r.Queryer,
	}
	r.Peeker = &perform.Peeker{
		ServerName: r.ServerName,
		Cfg:        &r.Cfg.DataFrame,
		DB:         r.DB,
		FSAPI:      r.fsAPI,
		Inputer:    r.Inputer,
	}
	r.InboundPeeker = &perform.InboundPeeker{
		DB:      r.DB,
		Inputer: r.Inputer,
	}
	r.Unpeeker = &perform.Unpeeker{
		ServerName: r.ServerName,
		Cfg:        &r.Cfg.DataFrame,
		FSAPI:      r.fsAPI,
		Inputer:    r.Inputer,
	}
	r.Leaver = &perform.Leaver{
		Cfg:     &r.Cfg.DataFrame,
		DB:      r.DB,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
	}
	r.Publisher = &perform.Publisher{
		DB: r.DB,
	}
	r.Backfiller = &perform.Backfiller{
		IsLocalServerName: r.Cfg.Global.IsLocalServerName,
		DB:                r.DB,
		FSAPI:             r.fsAPI,
		Querier:           r.Queryer,
		KeyRing:           r.KeyRing,
		// Perspective servers are trusted to not lie about server keys, so we will also
		// prefer these servers when backfilling (assuming they are in the frame) rather
		// than trying random servers
		PreferServers: r.PerspectiveServerNames,
	}
	r.Forgetter = &perform.Forgetter{
		DB: r.DB,
	}
	r.Upgrader = &perform.Upgrader{
		Cfg:    &r.Cfg.DataFrame,
		URSAPI: r,
	}
	r.Admin = &perform.Admin{
		DB:      r.DB,
		Cfg:     &r.Cfg.DataFrame,
		Inputer: r.Inputer,
		Queryer: r.Queryer,
		Leaver:  r.Leaver,
	}
	r.Creator = &perform.Creator{
		DB:    r.DB,
		Cfg:   &r.Cfg.DataFrame,
		RSAPI: r,
	}

	if err := r.Inputer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start dataframe input API")
	}
}

func (r *DataframeInternalAPI) SetUserAPI(userAPI userapi.DataframeUserAPI) {
	r.Leaver.UserAPI = userAPI
	r.Inputer.UserAPI = userAPI
}

func (r *DataframeInternalAPI) SetAppserviceAPI(asAPI asAPI.AppServiceInternalAPI) {
	r.asAPI = asAPI
}

func (r *DataframeInternalAPI) DefaultFrameVersion() xtools.FrameVersion {
	return r.defaultFrameVersion
}

func (r *DataframeInternalAPI) IsKnownFrame(ctx context.Context, frameID spec.FrameID) (bool, error) {
	return r.Inviter.IsKnownFrame(ctx, frameID)
}

func (r *DataframeInternalAPI) StateQuerier() xtools.StateQuerier {
	return r.Inviter.StateQuerier()
}

func (r *DataframeInternalAPI) HandleInvite(
	ctx context.Context, inviteEvent *types.HeaderedEvent,
) error {
	outputEvents, err := r.Inviter.ProcessInviteMembership(ctx, inviteEvent)
	if err != nil {
		return err
	}
	return r.OutputProducer.ProduceFrameEvents(inviteEvent.FrameID(), outputEvents)
}

func (r *DataframeInternalAPI) PerformCreateFrame(
	ctx context.Context, userID spec.UserID, frameID spec.FrameID, createRequest *api.PerformCreateFrameRequest,
) (string, *xutil.JSONResponse) {
	return r.Creator.PerformCreateFrame(ctx, userID, frameID, createRequest)
}

func (r *DataframeInternalAPI) PerformInvite(
	ctx context.Context,
	req *api.PerformInviteRequest,
) error {
	return r.Inviter.PerformInvite(ctx, req)
}

func (r *DataframeInternalAPI) PerformLeave(
	ctx context.Context,
	req *api.PerformLeaveRequest,
	res *api.PerformLeaveResponse,
) error {
	outputEvents, err := r.Leaver.PerformLeave(ctx, req, res)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	if len(outputEvents) == 0 {
		return nil
	}
	return r.OutputProducer.ProduceFrameEvents(req.FrameID, outputEvents)
}

func (r *DataframeInternalAPI) PerformForget(
	ctx context.Context,
	req *api.PerformForgetRequest,
	resp *api.PerformForgetResponse,
) error {
	return r.Forgetter.PerformForget(ctx, req, resp)
}

// GetOrCreateUserFramePrivateKey gets the user frame key for the specified user. If no key exists yet, a new one is created.
func (r *DataframeInternalAPI) GetOrCreateUserFramePrivateKey(ctx context.Context, userID spec.UserID, frameID spec.FrameID) (ed25519.PrivateKey, error) {
	key, err := r.DB.SelectUserFramePrivateKey(ctx, userID, frameID)
	if err != nil {
		return nil, err
	}
	// no key found, create one
	if len(key) == 0 {
		_, key, err = ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}
		key, err = r.DB.InsertUserFramePrivatePublicKey(ctx, userID, frameID, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func (r *DataframeInternalAPI) StoreUserFramePublicKey(ctx context.Context, senderID spec.SenderID, userID spec.UserID, frameID spec.FrameID) error {
	pubKeyBytes, err := senderID.RawBytes()
	if err != nil {
		return err
	}
	_, err = r.DB.InsertUserFramePublicKey(ctx, userID, frameID, ed25519.PublicKey(pubKeyBytes))
	return err
}

func (r *DataframeInternalAPI) SigningIdentityFor(ctx context.Context, frameID spec.FrameID, senderID spec.UserID) (fclient.SigningIdentity, error) {
	frameVersion, ok := r.Cache.GetFrameVersion(frameID.String())
	if !ok {
		frameInfo, err := r.DB.FrameInfo(ctx, frameID.String())
		if err != nil {
			return fclient.SigningIdentity{}, err
		}
		if frameInfo != nil {
			frameVersion = frameInfo.FrameVersion
		}
	}
	if frameVersion == xtools.FrameVersionPseudoIDs {
		privKey, err := r.GetOrCreateUserFramePrivateKey(ctx, senderID, frameID)
		if err != nil {
			return fclient.SigningIdentity{}, err
		}
		return fclient.SigningIdentity{
			PrivateKey: privKey,
			KeyID:      "ed25519:1",
			ServerName: spec.ServerName(spec.SenderIDFromPseudoIDKey(privKey)),
		}, nil
	}
	identity, err := r.Cfg.Global.SigningIdentityFor(senderID.Domain())
	if err != nil {
		return fclient.SigningIdentity{}, err
	}
	return *identity, err
}

func (r *DataframeInternalAPI) AssignFrameNID(ctx context.Context, frameID spec.FrameID, frameVersion xtools.FrameVersion) (frameNID types.FrameNID, err error) {
	return r.DB.AssignFrameNID(ctx, frameID, frameVersion)
}
