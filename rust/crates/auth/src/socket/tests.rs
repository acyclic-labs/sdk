use super::*;

#[test]
fn destination_validation_is_once_per_grant_not_once_per_rule() {
    let rules = vec![
        crate::contract::SocketHostRule {
            host: "different.example".into(),
            ports: vec![]
        };
        32
    ];
    for name in ["target.example".to_owned(), "a".repeat(254)] {
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
    }
}
