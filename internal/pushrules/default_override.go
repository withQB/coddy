package pushrules

func defaultOverrideRules(userID string) []*Rule {
	return []*Rule{
		&mRuleMasterDefinition,
		&mRuleSuppressNoticesDefinition,
		mRuleInviteForMeDefinition(userID),
		&mRuleMemberEventDefinition,
		&mRuleContainsDisplayNameDefinition,
		&mRuleFrameNotifDefinition,
		&mRuleTombstoneDefinition,
		&mRuleReactionDefinition,
	}
}

const (
	MRuleMaster              = ".m.rule.master"
	MRuleSuppressNotices     = ".m.rule.suppress_notices"
	MRuleInviteForMe         = ".m.rule.invite_for_me"
	MRuleMemberEvent         = ".m.rule.member_event"
	MRuleContainsDisplayName = ".m.rule.contains_display_name"
	MRuleTombstone           = ".m.rule.tombstone"
	MRuleFrameNotif           = ".m.rule.framenotif"
	MRuleReaction            = ".m.rule.reaction"
	MRuleFrameACLs            = ".m.rule.frame.server_acl"
)

var (
	mRuleMasterDefinition = Rule{
		RuleID:  MRuleMaster,
		Default: true,
		Enabled: false,
		Actions: []*Action{{Kind: DontNotifyAction}},
	}
	mRuleSuppressNoticesDefinition = Rule{
		RuleID:  MRuleSuppressNotices,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "content.msgtype",
				Pattern: pointer("m.notice"),
			},
		},
		Actions: []*Action{{Kind: DontNotifyAction}},
	}
	mRuleMemberEventDefinition = Rule{
		RuleID:  MRuleMemberEvent,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.member"),
			},
		},
		Actions: []*Action{{Kind: DontNotifyAction}},
	}
	mRuleContainsDisplayNameDefinition = Rule{
		RuleID:     MRuleContainsDisplayName,
		Default:    true,
		Enabled:    true,
		Conditions: []*Condition{{Kind: ContainsDisplayNameCondition}},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "default",
			},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleTombstoneDefinition = Rule{
		RuleID:  MRuleTombstone,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.tombstone"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(""),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleACLsDefinition = Rule{
		RuleID:  MRuleFrameACLs,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.server_acl"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(""),
			},
		},
		Actions: []*Action{},
	}
	mRuleFrameNotifDefinition = Rule{
		RuleID:  MRuleFrameNotif,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "content.body",
				Pattern: pointer("@frame"),
			},
			{
				Kind: SenderNotificationPermissionCondition,
				Key:  "frame",
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: HighlightTweak,
			},
		},
	}
	mRuleReactionDefinition = Rule{
		RuleID:  MRuleReaction,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.reaction"),
			},
		},
		Actions: []*Action{
			{Kind: DontNotifyAction},
		},
	}
)

func mRuleInviteForMeDefinition(userID string) *Rule {
	return &Rule{
		RuleID:  MRuleInviteForMe,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.member"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "content.membership",
				Pattern: pointer("invite"),
			},
			{
				Kind:    EventMatchCondition,
				Key:     "state_key",
				Pattern: pointer(userID),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "default",
			},
		},
	}
}
