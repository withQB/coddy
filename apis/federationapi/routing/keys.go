package routing

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	clienthttputil "github.com/withqb/coddy/apis/clientapi/httputil"
	federationAPI "github.com/withqb/coddy/apis/federationapi/api"
	"github.com/withqb/coddy/apis/userapi/api"
	"github.com/withqb/coddy/setup/config"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/fclient"
	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
	"golang.org/x/crypto/ed25519"
)

type queryKeysRequest struct {
	DeviceKeys map[string][]string `json:"device_keys"`
}

// QueryDeviceKeys returns device keys for users on this server.
func QueryDeviceKeys(
	httpReq *http.Request, request *fclient.FederationRequest, keyAPI api.FederationKeyAPI, thisServer spec.ServerName,
) xutil.JSONResponse {
	var qkr queryKeysRequest
	err := json.Unmarshal(request.Content(), &qkr)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// make sure we only query users on our domain
	for userID := range qkr.DeviceKeys {
		_, serverName, err := xtools.SplitID('@', userID)
		if err != nil {
			delete(qkr.DeviceKeys, userID)
			continue // ignore invalid users
		}
		if serverName != thisServer {
			delete(qkr.DeviceKeys, userID)
			continue
		}
	}

	var queryRes api.QueryKeysResponse
	keyAPI.QueryKeys(httpReq.Context(), &api.QueryKeysRequest{
		UserToDevices: qkr.DeviceKeys,
	}, &queryRes)
	if queryRes.Error != nil {
		xutil.GetLogger(httpReq.Context()).WithError(queryRes.Error).Error("failed to QueryKeys")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: struct {
			DeviceKeys      interface{} `json:"device_keys"`
			MasterKeys      interface{} `json:"master_keys"`
			SelfSigningKeys interface{} `json:"self_signing_keys"`
		}{
			queryRes.DeviceKeys,
			queryRes.MasterKeys,
			queryRes.SelfSigningKeys,
		},
	}
}

type claimOTKsRequest struct {
	OneTimeKeys map[string]map[string]string `json:"one_time_keys"`
}

// ClaimOneTimeKeys claims OTKs for users on this server.
func ClaimOneTimeKeys(
	httpReq *http.Request, request *fclient.FederationRequest, keyAPI api.FederationKeyAPI, thisServer spec.ServerName,
) xutil.JSONResponse {
	var cor claimOTKsRequest
	err := json.Unmarshal(request.Content(), &cor)
	if err != nil {
		return xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// make sure we only claim users on our domain
	for userID := range cor.OneTimeKeys {
		_, serverName, err := xtools.SplitID('@', userID)
		if err != nil {
			delete(cor.OneTimeKeys, userID)
			continue // ignore invalid users
		}
		if serverName != thisServer {
			delete(cor.OneTimeKeys, userID)
			continue
		}
	}

	var claimRes api.PerformClaimKeysResponse
	keyAPI.PerformClaimKeys(httpReq.Context(), &api.PerformClaimKeysRequest{
		OneTimeKeys: cor.OneTimeKeys,
	}, &claimRes)
	if claimRes.Error != nil {
		xutil.GetLogger(httpReq.Context()).WithError(claimRes.Error).Error("failed to PerformClaimKeys")
		return xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return xutil.JSONResponse{
		Code: 200,
		JSON: struct {
			OneTimeKeys interface{} `json:"one_time_keys"`
		}{claimRes.OneTimeKeys},
	}
}

// LocalKeys returns the local keys for the server.
func LocalKeys(cfg *config.FederationAPI, serverName spec.ServerName) xutil.JSONResponse {
	keys, err := localKeys(cfg, serverName)
	if err != nil {
		return xutil.MessageResponse(http.StatusNotFound, err.Error())
	}
	return xutil.JSONResponse{Code: http.StatusOK, JSON: keys}
}

func localKeys(cfg *config.FederationAPI, serverName spec.ServerName) (*xtools.ServerKeys, error) {
	var keys xtools.ServerKeys
	var identity *fclient.SigningIdentity
	var err error
	if virtualHost := cfg.Matrix.VirtualHostForHTTPHost(serverName); virtualHost == nil {
		if identity, err = cfg.Matrix.SigningIdentityFor(cfg.Matrix.ServerName); err != nil {
			return nil, err
		}
		publicKey := cfg.Matrix.PrivateKey.Public().(ed25519.PublicKey)
		keys.ServerName = cfg.Matrix.ServerName
		keys.ValidUntilTS = spec.AsTimestamp(time.Now().Add(cfg.Matrix.KeyValidityPeriod))
		keys.VerifyKeys = map[xtools.KeyID]xtools.VerifyKey{
			cfg.Matrix.KeyID: {
				Key: spec.Base64Bytes(publicKey),
			},
		}
		keys.OldVerifyKeys = map[xtools.KeyID]xtools.OldVerifyKey{}
		for _, oldVerifyKey := range cfg.Matrix.OldVerifyKeys {
			keys.OldVerifyKeys[oldVerifyKey.KeyID] = xtools.OldVerifyKey{
				VerifyKey: xtools.VerifyKey{
					Key: oldVerifyKey.PublicKey,
				},
				ExpiredTS: oldVerifyKey.ExpiredAt,
			}
		}
	} else {
		if identity, err = cfg.Matrix.SigningIdentityFor(virtualHost.ServerName); err != nil {
			return nil, err
		}
		publicKey := virtualHost.PrivateKey.Public().(ed25519.PublicKey)
		keys.ServerName = virtualHost.ServerName
		keys.ValidUntilTS = spec.AsTimestamp(time.Now().Add(virtualHost.KeyValidityPeriod))
		keys.VerifyKeys = map[xtools.KeyID]xtools.VerifyKey{
			virtualHost.KeyID: {
				Key: spec.Base64Bytes(publicKey),
			},
		}
		// TDO: Virtual hosts probably want to be able to specify old signing
		// keys too, just in case
	}

	toSign, err := json.Marshal(keys.ServerKeyFields)
	if err != nil {
		return nil, err
	}

	keys.Raw, err = xtools.SignJSON(
		string(identity.ServerName), identity.KeyID, identity.PrivateKey, toSign,
	)
	return &keys, err
}

func NotaryKeys(
	httpReq *http.Request, cfg *config.FederationAPI,
	fsAPI federationAPI.FederationInternalAPI,
	req *xtools.PublicKeyNotaryLookupRequest,
) xutil.JSONResponse {
	serverName := spec.ServerName(httpReq.Host) // TDO: this is not ideal
	if !cfg.Matrix.IsLocalServerName(serverName) {
		return xutil.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Server name not known"),
		}
	}

	if req == nil {
		req = &xtools.PublicKeyNotaryLookupRequest{}
		if reqErr := clienthttputil.UnmarshalJSONRequest(httpReq, &req); reqErr != nil {
			return *reqErr
		}
	}

	var response struct {
		ServerKeys []json.RawMessage `json:"server_keys"`
	}
	response.ServerKeys = []json.RawMessage{}

	for serverName, kidToCriteria := range req.ServerKeys {
		var keyList []xtools.ServerKeys
		if serverName == cfg.Matrix.ServerName {
			if k, err := localKeys(cfg, serverName); err == nil {
				keyList = append(keyList, *k)
			} else {
				return xutil.ErrorResponse(err)
			}
		} else {
			var resp federationAPI.QueryServerKeysResponse
			err := fsAPI.QueryServerKeys(httpReq.Context(), &federationAPI.QueryServerKeysRequest{
				ServerName:      serverName,
				KeyIDToCriteria: kidToCriteria,
			}, &resp)
			if err != nil {
				return xutil.ErrorResponse(err)
			}
			keyList = append(keyList, resp.ServerKeys...)
		}
		if len(keyList) == 0 {
			continue
		}

		for _, keys := range keyList {
			j, err := json.Marshal(keys)
			if err != nil {
				logrus.WithError(err).Errorf("failed to marshal %q response", serverName)
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			js, err := xtools.SignJSON(
				string(cfg.Matrix.ServerName), cfg.Matrix.KeyID, cfg.Matrix.PrivateKey, j,
			)
			if err != nil {
				logrus.WithError(err).Errorf("failed to sign %q response", serverName)
				return xutil.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}

			response.ServerKeys = append(response.ServerKeys, js)
		}
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
