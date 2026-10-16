use super::*;
#[path = "../../tests/common/dns_hardening.rs"]
mod hardening;
use proptest::{
    prelude::*,
    test_runner::{TestError, TestRunner},
};

#[test]
fn shrinking_dns_property_detects_removed_numeric_rejection() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 128,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    let result = runner.run(&hardening::numeric_name(), |name| {
        hardening::dns_property(&name, |name| dns_labels(name).is_some())
    });
    assert!(
        matches!(result, Err(TestError::Fail(_, _))),
        "removed numeric guard must produce a shrinking counterexample"
    );
}

#[test]
fn destination_validation_is_once_per_grant_not_once_per_rule() {
    let rules = vec![
        crate::contract::SocketHostRule {
            host: "different.example".into(),
            ports: vec![]
        };
        1024
    ];
    for name in ["target.example".to_owned(), "a".repeat(1024 * 1024)] {
        let destination = Destination {
            host: Host::Dns(&name),
            port: 443,
        };
        let mut calls = 0;
        assert!(!allows_rules(&rules, destination, |name| {
            calls += 1;
            dns(name)
        }));
        assert_eq!(calls, 1);
        // The broken arrangement re-enters the real validation/matching path
        // for each rule. The same work bound detects this original regression.
        let mut broken_calls = 0;
        assert!(!rules.iter().any(|rule| allows_rules(
            std::slice::from_ref(rule),
            destination,
            |name| {
                broken_calls += 1;
                dns(name)
            }
        )));
        assert!(broken_calls > 1);
    }
}
