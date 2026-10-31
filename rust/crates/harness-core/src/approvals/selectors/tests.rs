use super::*;
use proptest::prelude::*;
use serde_json::json;

#[test]
fn approval_selectors_conjoin_command_path_and_volume() -> Result<()> {
    let rule: ApprovalRule = serde_json::from_value(json!({
        "tool":"deploy", "arguments":null, "verdict":"allow",
        "selectors":[
            {"kind":"pattern","pointer":"/command","pattern":"cargo test --package *"},
            {"kind":"path_within","pointer":"/path","root":"/src"},
            {"kind":"equal","pointer":"/volume","value":"work"}
        ]
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let policy = ApprovalPolicy {
        approvers: vec![Approver::Rules {
            principal: "rules".into(),
            rules: vec![rule.clone()],
            fallback: RuleVerdict::Deny,
        }],
    };
    policy.bind()?;
    let arguments =
        json!({"command":"cargo test --package core", "path":"src/lib.rs", "volume":"work"});
    assert!(rule.matches("deploy", &arguments));
    let mut broken = rule.clone();
    broken.selectors.clear();
    for (field, value) in [
        ("command", json!("echo cargo test --package core")),
        ("command", json!({"text":"cargo test --package core"})),
        ("path", json!("/src-private/key")),
        ("path", json!("/src/../private")),
        ("path", json!("src\\..\\private")),
        ("path", json!("/src//private")),
        ("volume", json!("other")),
        ("volume", Value::Null),
    ] {
        let mut changed = arguments.clone();
        changed[field] = value;
        assert!(!rule.matches("deploy", &changed), "{changed}");
        // Negative control: the previous tool/exact-arguments-only rule allows it.
        assert!(broken.matches("deploy", &changed));
    }
    let mut missing = arguments;
    missing.as_object_mut().unwrap().remove("volume");
    assert!(!rule.matches("deploy", &missing));
    Ok(())
}

#[test]
fn approval_selectors_reject_invalid_pointers_and_paths_and_do_not_match_opaque() {
    let rejected = |selector| {
        ApprovalPolicy {
            approvers: vec![Approver::Rules {
                principal: "rules".into(),
                rules: vec![ApprovalRule {
                    tool: None,
                    arguments: None,
                    selectors: vec![selector],
                    target: None,
                    verdict: RuleVerdict::Allow,
                }],
                fallback: RuleVerdict::Allow,
            }],
        }
        .bind()
        .is_err()
    };
    for pointer in ["command", "/bad~2", "/trailing~"] {
        assert!(rejected(ApprovalSelector::Equal {
            pointer: pointer.into(),
            value: Value::Null
        }));
    }
    for root in [
        "",
        "/src/../private",
        "/src/.",
        "/src//private",
        "src\\private",
    ] {
        assert!(rejected(ApprovalSelector::PathWithin {
            pointer: "/path".into(),
            root: root.into()
        }));
    }
    let escaped = ApprovalSelector::Equal {
        pointer: "/a~1b/~0".into(),
        value: json!("work"),
    };
    assert!(escaped.validate().is_ok());
    assert!(escaped.matches(&json!({"a/b":{"~":"work"}})));
    assert!(
        !ApprovalSelector::Equal {
            pointer: "/missing".into(),
            value: Value::Null
        }
        .matches(&json!({}))
    );
    let selected = ApprovalRule {
        tool: None,
        arguments: None,
        selectors: vec![escaped],
        target: None,
        verdict: RuleVerdict::Allow,
    };
    assert_eq!(
        ApprovalRule::evaluate_subject(
            &[selected],
            RuleVerdict::Ask,
            &crate::interaction::ApprovalSubject::Opaque {}
        ),
        RuleVerdict::Ask
    );
}

fn oracle(pattern: &[u8], text: &[u8]) -> bool {
    let mut prior = vec![false; text.len() + 1];
    prior[0] = true;
    for token in pattern {
        let mut next = vec![false; text.len() + 1];
        if *token == b'*' {
            next[0] = prior[0];
        }
        for (i, actual) in text.iter().enumerate() {
            next[i + 1] = if *token == b'*' {
                prior[i + 1] || next[i]
            } else {
                prior[i] && token == actual
            };
        }
        prior = next;
    }
    prior[text.len()]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn approval_command_patterns_match_the_full_string_oracle(pattern in "[abc*]{0,16}", text in "[abc]{0,16}") {
        prop_assert_eq!(pattern_matches(&pattern, &text), oracle(pattern.as_bytes(), text.as_bytes()));
    }
}

#[test]
fn approval_command_patterns_are_anchored_and_literal_except_for_stars() {
    assert!(pattern_matches("cargo test *", "cargo test core"));
    assert!(!pattern_matches("cargo test *", "echo cargo test core"));
    // A substring control would permit the prefixed command.
    assert!("echo cargo test core".contains("cargo test "));
    assert!(!pattern_matches("test?", "tests"));
    assert!(pattern_matches("a**b*c", "abbbc"));
    assert!(pattern_matches("Ã©*Î»", "Ã© text Î»"));
    assert!(!pattern_matches("a*a", "a"));
}
