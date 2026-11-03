use super::*;

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
