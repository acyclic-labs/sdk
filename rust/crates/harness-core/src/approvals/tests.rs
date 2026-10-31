use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn approval_rules_deny_wins_in_either_order(allow_first in any::<bool>(), path in "[a-z]{1,32}") {
        let mut rules = [RuleVerdict::Allow, RuleVerdict::Deny].map(|verdict| ApprovalRule {
            tool: Some("write".into()), arguments: None, selectors: vec![], target: None, verdict,
        });
        if allow_first { rules.reverse(); }
        prop_assert_eq!(ApprovalRule::evaluate(&rules, RuleVerdict::Ask, "write", &serde_json::json!({"path":path})), RuleVerdict::Deny);
        prop_assert_eq!(ApprovalRule::evaluate(&rules, RuleVerdict::Ask, "read", &Value::Null), RuleVerdict::Ask);
    }
}

#[test]
fn approval_rules_bind_exact_arguments_and_policy_order() -> Result<()> {
    let rule = ApprovalRule {
        tool: Some("write".into()),
        arguments: Some(serde_json::json!({"path":"approved","text":"one"})),
        selectors: vec![],
        target: None,
        verdict: RuleVerdict::Allow,
    };
    assert!(rule.matches(
        "write",
        &serde_json::json!({"path":"approved","text":"one"})
    ));
    assert!(!rule.matches(
        "write",
        &serde_json::json!({"path":"approved","text":"two"})
    ));
    assert!(!rule.matches(
        "write",
        &serde_json::json!({"path":"elsewhere","text":"one"})
    ));
    assert!(!rule.matches("edit", &serde_json::json!({"path":"approved","text":"one"})));
    assert!(!rule.matches(
        "write",
        &serde_json::json!({"path":"approved","text":"one","extra":true})
    ));
    let mut policy = ApprovalPolicy {
        approvers: vec![
            Approver::Rules {
                principal: "rules".into(),
                rules: vec![rule],
                fallback: RuleVerdict::Ask,
            },
            Approver::Human {
                principal: "human".into(),
            },
        ],
    };
    let original = policy.bind()?.identity();
    policy.approvers.reverse();
    assert_ne!(original, policy.bind()?.identity());
    Ok(())
}

#[test]
fn approval_data_never_bypasses_the_interaction_gate() -> Result<()> {
    let policy = ApprovalPolicy {
        approvers: vec![Approver::Rules {
            principal: "configured".into(),
            rules: vec![],
            fallback: RuleVerdict::Allow,
        }],
    };
    let gate = policy.bind()?;
    let invocation = ToolInvocation {
        operation_id: crate::OperationId::new(),
        call_id: "call".into(),
        name: "write".into(),
        arguments: serde_json::json!({}),
    };
    let scope = RuntimeScope::new(
        crate::Capabilities::new(["tool:call:write"]),
        crate::conversation::Limits::default(),
    )?;
    let evaluated = futures::executor::block_on(gate.evaluate(&invocation, &scope, None)?)?;
    assert!(matches!(
        evaluated.0,
        ToolPolicyDecision::RequireApproval { .. }
    ));
    let mut invalid = policy;
    invalid.approvers.push(Approver::Human {
        principal: "configured".into(),
    });
    assert!(invalid.bind().is_err());
    invalid.approvers = vec![Approver::Human {
        principal: "parent:forged".into(),
    }];
    assert!(invalid.bind().is_err());
    Ok(())
}

#[test]
fn approval_rules_explicit_wildcards_govern_opaque_subjects() {
    let opaque = crate::interaction::ApprovalSubject::Opaque {};
    let selected = ApprovalRule {
        tool: Some("write".into()),
        arguments: None,
        selectors: vec![],
        target: None,
        verdict: RuleVerdict::Allow,
    };
    for fallback in [RuleVerdict::Allow, RuleVerdict::Deny, RuleVerdict::Ask] {
        let verdict =
            ApprovalRule::evaluate_subject(std::slice::from_ref(&selected), fallback, &opaque);
        let oracle = |actual| actual == fallback;
        assert!(oracle(verdict));
        // Negative control restores the unconditional ask for opaque requests.
        assert_eq!(oracle(RuleVerdict::Ask), fallback == RuleVerdict::Ask);
    }
    let wildcard_deny = ApprovalRule {
        tool: None,
        arguments: None,
        selectors: vec![],
        target: None,
        verdict: RuleVerdict::Deny,
    };
    assert_eq!(
        ApprovalRule::evaluate_subject(&[selected, wildcard_deny], RuleVerdict::Allow, &opaque),
        RuleVerdict::Deny
    );
    let selected_deny = ApprovalRule {
        tool: Some("write".into()),
        arguments: None,
        selectors: vec![],
        target: None,
        verdict: RuleVerdict::Deny,
    };
    assert_eq!(
        ApprovalRule::evaluate_subject(&[selected_deny], RuleVerdict::Allow, &opaque),
        RuleVerdict::Allow
    );
}

#[test]
fn approval_rules_match_the_resolved_device_and_owner() {
    let subject = |target: Option<(&str, &str)>| crate::interaction::ApprovalSubject::Tool {
        route: crate::interaction::ApprovalRoute::Policy,
        tool: "shell".into(),
        call_id: "call".into(),
        arguments: serde_json::json!({"command": "make", "target": "user:bo"}),
        target: target.map(|(device, owner)| crate::interaction::ApprovalTarget {
            device: device.into(),
            owner: owner.into(),
        }),
    };
    let deny_owner = ApprovalRule {
        tool: None,
        arguments: None,
        selectors: vec![],
        target: Some(super::TargetSelector {
            device: None,
            owner: Some("bo".into()),
        }),
        verdict: RuleVerdict::Deny,
    };
    let rules = std::slice::from_ref(&deny_owner);
    let verdict =
        |target| ApprovalRule::evaluate_subject(rules, RuleVerdict::Allow, &subject(target));
    // The resolved owner decides, whatever alias the arguments used.
    assert_eq!(verdict(Some(("desk", "bo"))), RuleVerdict::Deny);
    // Negative controls: another owner, or no resolved target, is not selected.
    assert_eq!(verdict(Some(("desk", "ana"))), RuleVerdict::Allow);
    assert_eq!(verdict(None), RuleVerdict::Allow);
    let device = ApprovalRule {
        target: Some(super::TargetSelector {
            device: Some("desk".into()),
            owner: Some("bo".into()),
        }),
        ..deny_owner.clone()
    };
    let rules = std::slice::from_ref(&device);
    let verdict =
        |target| ApprovalRule::evaluate_subject(rules, RuleVerdict::Allow, &subject(target));
    assert_eq!(verdict(Some(("desk", "bo"))), RuleVerdict::Deny);
    assert_eq!(verdict(Some(("laptop", "bo"))), RuleVerdict::Allow);
    // A target rule never matches a sealed action.
    assert_eq!(
        ApprovalRule::evaluate_subject(
            rules,
            RuleVerdict::Allow,
            &crate::interaction::ApprovalSubject::Opaque {}
        ),
        RuleVerdict::Allow
    );
}
