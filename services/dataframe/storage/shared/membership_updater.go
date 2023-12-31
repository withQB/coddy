package shared

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/withqb/coddy/services/dataframe/storage/tables"
	"github.com/withqb/coddy/services/dataframe/types"
	"github.com/withqb/xtools"
)

type MembershipUpdater struct {
	transaction
	d             *Database
	frameNID       types.FrameNID
	targetUserNID types.EventStateKeyNID
	oldMembership tables.MembershipState
}

func NewMembershipUpdater(
	ctx context.Context, d *Database, txn *sql.Tx, frameID, targetUserID string,
	targetLocal bool, frameVersion xtools.FrameVersion,
) (*MembershipUpdater, error) {
	var frameNID types.FrameNID
	var targetUserNID types.EventStateKeyNID
	var err error
	err = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		frameNID, err = d.assignFrameNID(ctx, txn, frameID, frameVersion)
		if err != nil {
			return err
		}
		targetUserNID, err = d.assignStateKeyNID(ctx, txn, targetUserID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return d.membershipUpdaterTxn(ctx, txn, frameNID, targetUserNID, targetLocal)
}

func (d *Database) membershipUpdaterTxn(
	ctx context.Context,
	txn *sql.Tx,
	frameNID types.FrameNID,
	targetUserNID types.EventStateKeyNID,
	targetLocal bool,
) (*MembershipUpdater, error) {
	err := d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		if err := d.MembershipTable.InsertMembership(ctx, txn, frameNID, targetUserNID, targetLocal); err != nil {
			return fmt.Errorf("d.MembershipTable.InsertMembership: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("u.d.Writer.Do: %w", err)
	}

	membership, err := d.MembershipTable.SelectMembershipForUpdate(ctx, txn, frameNID, targetUserNID)
	if err != nil {
		return nil, err
	}

	return &MembershipUpdater{
		transaction{ctx, txn}, d, frameNID, targetUserNID, membership,
	}, nil
}

// IsInvite implements types.MembershipUpdater
func (u *MembershipUpdater) IsInvite() bool {
	return u.oldMembership == tables.MembershipStateInvite
}

// IsJoin implements types.MembershipUpdater
func (u *MembershipUpdater) IsJoin() bool {
	return u.oldMembership == tables.MembershipStateJoin
}

// IsLeave implements types.MembershipUpdater
func (u *MembershipUpdater) IsLeave() bool {
	return u.oldMembership == tables.MembershipStateLeaveOrBan
}

// IsKnock implements types.MembershipUpdater
func (u *MembershipUpdater) IsKnock() bool {
	return u.oldMembership == tables.MembershipStateKnock
}

func (u *MembershipUpdater) Delete() error {
	if _, err := u.d.InvitesTable.UpdateInviteRetired(u.ctx, u.txn, u.frameNID, u.targetUserNID); err != nil {
		return err
	}
	return u.d.MembershipTable.DeleteMembership(u.ctx, u.txn, u.frameNID, u.targetUserNID)
}

func (u *MembershipUpdater) Update(newMembership tables.MembershipState, event *types.Event) (bool, []string, error) {
	var inserted bool    // Did the query result in a membership change?
	var retired []string // Did we retire any updates in the process?
	return inserted, retired, u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		senderUserNID, err := u.d.assignStateKeyNID(u.ctx, u.txn, string(event.SenderID()))
		if err != nil {
			return fmt.Errorf("u.d.AssignStateKeyNID: %w", err)
		}
		inserted, err = u.d.MembershipTable.UpdateMembership(u.ctx, u.txn, u.frameNID, u.targetUserNID, senderUserNID, newMembership, event.EventNID, false)
		if err != nil {
			return fmt.Errorf("u.d.MembershipTable.UpdateMembership: %w", err)
		}
		if !inserted {
			return nil
		}
		switch {
		case u.oldMembership != tables.MembershipStateInvite && newMembership == tables.MembershipStateInvite:
			inserted, err = u.d.InvitesTable.InsertInviteEvent(
				u.ctx, u.txn, event.EventID(), u.frameNID, u.targetUserNID, senderUserNID, event.JSON(),
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTable.InsertInviteEvent: %w", err)
			}
		case u.oldMembership == tables.MembershipStateInvite && newMembership != tables.MembershipStateInvite:
			retired, err = u.d.InvitesTable.UpdateInviteRetired(
				u.ctx, u.txn, u.frameNID, u.targetUserNID,
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTables.UpdateInviteRetired: %w", err)
			}
		}
		return nil
	})
}
