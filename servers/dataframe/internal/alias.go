package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/withqb/coddy/internal/eventutil"
	"github.com/withqb/coddy/servers/dataframe/api"
	"github.com/withqb/coddy/servers/dataframe/internal/helpers"
	"github.com/withqb/coddy/servers/dataframe/types"
	asAPI "github.com/withqb/coddy/services/appservice/api"
	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// SetFrameAlias implements alias.DataframeInternalAPI
func (r *DataframeInternalAPI) SetFrameAlias(
	ctx context.Context,
	senderID spec.SenderID,
	frameID spec.FrameID,
	alias string,
) (aliasAlreadyUsed bool, err error) {
	// Check if the alias isn't already referring to a frame
	existingFrameID, err := r.DB.GetFrameIDForAlias(ctx, alias)
	if err != nil {
		return false, err
	}

	if len(existingFrameID) > 0 {
		// If the alias already exists, stop the process
		return true, nil
	}

	// Save the new alias
	if err := r.DB.SetFrameAlias(ctx, alias, frameID.String(), string(senderID)); err != nil {
		return false, err
	}

	return false, nil
}

// GetFrameIDForAlias implements alias.DataframeInternalAPI
func (r *DataframeInternalAPI) GetFrameIDForAlias(
	ctx context.Context,
	request *api.GetFrameIDForAliasRequest,
	response *api.GetFrameIDForAliasResponse,
) error {
	// Look up the frame ID in the database
	frameID, err := r.DB.GetFrameIDForAlias(ctx, request.Alias)
	if err == nil && frameID != "" {
		response.FrameID = frameID
		return nil
	}

	// Check appservice on err, but only if the appservice API is
	// wired in and no frame ID was found.
	if r.asAPI != nil && request.IncludeAppservices && frameID == "" {
		// No frame found locally, try our application services by making a call to
		// the appservice component
		aliasReq := &asAPI.FrameAliasExistsRequest{
			Alias: request.Alias,
		}
		aliasRes := &asAPI.FrameAliasExistsResponse{}
		if err = r.asAPI.FrameAliasExists(ctx, aliasReq, aliasRes); err != nil {
			return err
		}

		if aliasRes.AliasExists {
			frameID, err = r.DB.GetFrameIDForAlias(ctx, request.Alias)
			if err != nil {
				return err
			}
			response.FrameID = frameID
			return nil
		}
	}

	return err
}

// GetAliasesForFrameID implements alias.DataframeInternalAPI
func (r *DataframeInternalAPI) GetAliasesForFrameID(
	ctx context.Context,
	request *api.GetAliasesForFrameIDRequest,
	response *api.GetAliasesForFrameIDResponse,
) error {
	// Look up the aliases in the database for the given FrameID
	aliases, err := r.DB.GetAliasesForFrameID(ctx, request.FrameID)
	if err != nil {
		return err
	}

	response.Aliases = aliases
	return nil
}

// nolint:gocyclo
// RemoveFrameAlias implements alias.DataframeInternalAPI
// nolint: gocyclo
func (r *DataframeInternalAPI) RemoveFrameAlias(ctx context.Context, senderID spec.SenderID, alias string) (aliasFound bool, aliasRemoved bool, err error) {
	frameID, err := r.DB.GetFrameIDForAlias(ctx, alias)
	if err != nil {
		return false, false, fmt.Errorf("r.DB.GetFrameIDForAlias: %w", err)
	}
	if frameID == "" {
		return false, false, nil
	}

	validFrameID, err := spec.NewFrameID(frameID)
	if err != nil {
		return true, false, err
	}

	sender, err := r.QueryUserIDForSender(ctx, *validFrameID, senderID)
	if err != nil || sender == nil {
		return true, false, fmt.Errorf("r.QueryUserIDForSender: %w", err)
	}
	virtualHost := sender.Domain()

	creatorID, err := r.DB.GetCreatorIDForAlias(ctx, alias)
	if err != nil {
		return true, false, fmt.Errorf("r.DB.GetCreatorIDForAlias: %w", err)
	}

	if spec.SenderID(creatorID) != senderID {
		var plEvent *types.HeaderedEvent
		var pls *xtools.PowerLevelContent

		plEvent, err = r.DB.GetStateEvent(ctx, frameID, spec.MFramePowerLevels, "")
		if err != nil {
			return true, false, fmt.Errorf("r.DB.GetStateEvent: %w", err)
		}

		pls, err = plEvent.PowerLevels()
		if err != nil {
			return true, false, fmt.Errorf("plEvent.PowerLevels: %w", err)
		}

		if pls.UserLevel(senderID) < pls.EventLevel(spec.MFrameCanonicalAlias, true) {
			return true, false, nil
		}
	}

	ev, err := r.DB.GetStateEvent(ctx, frameID, spec.MFrameCanonicalAlias, "")
	if err != nil && err != sql.ErrNoRows {
		return true, false, err
	} else if ev != nil {
		stateAlias := gjson.GetBytes(ev.Content(), "alias").Str
		// the alias to remove is currently set as the canonical alias, remove it
		if stateAlias == alias {
			res, err := sjson.DeleteBytes(ev.Content(), "alias")
			if err != nil {
				return true, false, err
			}

			canonicalSenderID := ev.SenderID()
			canonicalSender, err := r.QueryUserIDForSender(ctx, *validFrameID, canonicalSenderID)
			if err != nil || canonicalSender == nil {
				return true, false, err
			}

			validFrameID, err := spec.NewFrameID(frameID)
			if err != nil {
				return true, false, err
			}
			identity, err := r.SigningIdentityFor(ctx, *validFrameID, *canonicalSender)
			if err != nil {
				return true, false, err
			}

			proto := &xtools.ProtoEvent{
				SenderID: string(canonicalSenderID),
				FrameID:   ev.FrameID(),
				Type:     ev.Type(),
				StateKey: ev.StateKey(),
				Content:  res,
			}

			eventsNeeded, err := xtools.StateNeededForProtoEvent(proto)
			if err != nil {
				return true, false, fmt.Errorf("xtools.StateNeededForEventBuilder: %w", err)
			}
			if len(eventsNeeded.Tuples()) == 0 {
				return true, false, errors.New("expecting state tuples for event builder, got none")
			}

			stateRes := &api.QueryLatestEventsAndStateResponse{}
			if err = helpers.QueryLatestEventsAndState(ctx, r.DB, r, &api.QueryLatestEventsAndStateRequest{FrameID: frameID, StateToFetch: eventsNeeded.Tuples()}, stateRes); err != nil {
				return true, false, err
			}

			newEvent, err := eventutil.BuildEvent(ctx, proto, &identity, time.Now(), &eventsNeeded, stateRes)
			if err != nil {
				return true, false, err
			}

			err = api.SendEvents(ctx, r, api.KindNew, []*types.HeaderedEvent{newEvent}, virtualHost, r.ServerName, r.ServerName, nil, false)
			if err != nil {
				return true, false, err
			}
		}
	}

	// Remove the alias from the database
	if err := r.DB.RemoveFrameAlias(ctx, alias); err != nil {
		return true, false, err
	}

	return true, true, nil
}
