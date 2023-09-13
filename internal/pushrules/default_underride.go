package pushrules

const (
	MRuleCall                  = ".m.rule.call"
	MRuleEncryptedFrameOneToOne = ".m.rule.encrypted_frame_one_to_one"
	MRuleFrameOneToOne          = ".m.rule.frame_one_to_one"
	MRuleMessage               = ".m.rule.message"
	MRuleEncrypted             = ".m.rule.encrypted"
)

var defaultUnderrideRules = []*Rule{
	&mRuleCallDefinition,
	&mRuleFrameOneToOneDefinition,
	&mRuleEncryptedFrameOneToOneDefinition,
	&mRuleMessageDefinition,
	&mRuleEncryptedDefinition,
}

var (
	mRuleCallDefinition = Rule{
		RuleID:  MRuleCall,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.call.invite"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
			{
				Kind:  SetTweakAction,
				Tweak: SoundTweak,
				Value: "ring",
			},
		},
	}
	mRuleEncryptedFrameOneToOneDefinition = Rule{
		RuleID:  MRuleEncryptedFrameOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind: FrameMemberCountCondition,
				Is:   "2",
			},
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.encrypted"),
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
	mRuleFrameOneToOneDefinition = Rule{
		RuleID:  MRuleFrameOneToOne,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind: FrameMemberCountCondition,
				Is:   "2",
			},
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.message"),
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
	mRuleMessageDefinition = Rule{
		RuleID:  MRuleMessage,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.message"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
		},
	}
	mRuleEncryptedDefinition = Rule{
		RuleID:  MRuleEncrypted,
		Default: true,
		Enabled: true,
		Conditions: []*Condition{
			{
				Kind:    EventMatchCondition,
				Key:     "type",
				Pattern: pointer("m.frame.encrypted"),
			},
		},
		Actions: []*Action{
			{Kind: NotifyAction},
		},
	}
)
