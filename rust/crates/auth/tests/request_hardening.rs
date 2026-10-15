mod common;
use acyclic_auth::{authority::*, contract::*, socket::*, valid_resource};
use common::*;
use proptest::prelude::*;
use prost::Message;
use std::net::{IpAddr, Ipv4Addr};

fn request<'a>(
    resource: &'a str,
    audience: &'a str,
    attributes: &'a RecordAttributes,
    usage: &'a Limits,
) -> Request<'a> {
    Request {
        target: Target::Resource {
            action: "fs/read",
            resource,
        },
        audience,
        holder: None,
        now_unix_seconds: 2,
        keys: &[],
        attributes,
        usage,
        cpu_millis: 0,
        memory_bytes: 0,
    }
}

fn invalid_name() -> impl Strategy<Value = String> {
    (
        "[a-z]{1,12}",
        prop::sample::select(vec![
            "../secret",
            "./secret",
            "a//b",
            "report?.txt",
            "%2f",
            "%41",
            "a%b",
            "λ",
        ]),
    )
        .prop_map(|(name, tail)| format!("fs:/{name}/{tail}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn noncanonical_resources_and_audiences_never_match(name in invalid_name(), child in any::<bool>()) {
        prop_assert!(!valid_resource(&name));
        let root = vec![root()];
        let blocks = if child { attenuate(&root, vec![], vec![6;32]).unwrap() } else { root };
        let attributes = RecordAttributes::default(); let usage = limits(None);
        let canonical = request("fs:/a", "workers:/app", &attributes, &usage);
        prop_assert_eq!(permits(&blocks, &canonical, PolicyDecision::Allow), Ok(()));
        prop_assert_eq!(permits(&blocks, &canonical, PolicyDecision::Deny), Err(Deny::OwnerPolicy));
        for policy in [PolicyDecision::Allow, PolicyDecision::Deny] {
            prop_assert_eq!(permits(&blocks, &request(&name, "workers:/app", &attributes, &usage), policy), Err(Deny::NonCanonicalResource));
            prop_assert_eq!(permits(&blocks, &request("fs:/a", &name, &attributes, &usage), policy), Err(Deny::NonCanonicalAudience));
        }
    }

    #[test]
    fn arbitrary_noncanonical_request_facts_are_typed_denials(resource in any::<String>(), audience in any::<String>()) {
        let blocks = vec![root()]; let attributes = RecordAttributes::default(); let usage = limits(None);
        let result = permits(&blocks, &request(&resource, &audience, &attributes, &usage), PolicyDecision::Allow);
        if !valid_resource(&resource) { prop_assert_eq!(result, Err(Deny::NonCanonicalResource)); }
        else if !valid_resource(&audience) { prop_assert_eq!(result, Err(Deny::NonCanonicalAudience)); }
    }

    #[test]
    fn numeric_dns_names_are_rejected_by_socket_and_egress_admission(
        values in prop::collection::vec(any::<u32>(), 1..5), notation in 0..3u8, trailing in any::<bool>(),
    ) {
        let labels = values.iter().map(|value| match notation { 0 => value.to_string(), 1 => format!("0{value:o}"), _ => format!("0x{value:x}") }).collect::<Vec<_>>();
        let name = labels.join(".");
        let name = if trailing { format!("{name}.") } else { name };
        prop_assert!(!valid_dns_name(&name));
        // A plain canonical IP is an explicit IP rule; every other numeric
        // spelling must fail rather than falling through to the DNS branch.
        prop_assert_eq!(valid_rule(&name), name.parse::<IpAddr>().is_ok());
        for host in [format!("*.{name}"), format!("prefix.{name}")] {
            prop_assert!(!valid_rule(&host));
            prop_assert!(!valid_dns_name(host.strip_prefix("*.").unwrap_or(&host)));
            prop_assert!(SocketHostRule::try_from(SocketHostRuleProto { host, ports: vec![] }).is_err());
        }
    }

    #[test]
    fn canonical_dns_and_explicit_ips_round_trip(label in "[a-z][a-z0-9]{0,15}", ip in any::<u32>(), upper in any::<bool>(), trailing in any::<bool>()) {
        let name = format!("{label}.example.com");
        let name = if upper { name.to_ascii_uppercase() } else { name };
        let name = if trailing { format!("{name}.") } else { name };
        prop_assert!(valid_dns_name(&name));
        for host in [name, Ipv4Addr::from(ip).to_string(), Ipv4Addr::from(ip).to_ipv6_mapped().to_string()] {
            let value = SocketHostRule::try_from(SocketHostRuleProto { host, ports: vec![] }).unwrap();
            let bytes = SocketHostRuleProto::from(value.clone()).encode_to_vec();
            prop_assert_eq!(SocketHostRule::try_from(SocketHostRuleProto::decode(bytes.as_slice()).unwrap()), Ok(value));
        }
    }

    #[test]
    fn ipv4_mapped_hosts_require_explicit_ip_facts(ip in any::<u32>()) {
        let mapped = Ipv4Addr::from(ip).to_ipv6_mapped();
        let name = mapped.to_string();
        let grant = WebsocketConnectGrant { rules: vec![SocketHostRule { host: name.clone(), ports: vec![] }] };
        prop_assert!(!valid_dns_name(&name));
        prop_assert!(!allows(&grant, Destination { host: Host::Dns(&name), port: 443 }));
        prop_assert!(allows(&grant, Destination { host: Host::Ip(IpAddr::V6(mapped)), port: 443 }));
        prop_assert!(!allows(&grant, Destination { host: Host::Ip(IpAddr::V4(Ipv4Addr::from(ip))), port: 443 }));
    }
}

#[test]
fn numeric_regression_spellings_never_enter_dns_grants() {
    for name in [
        "127.1",
        "0x7f.1",
        "2130706433",
        "0x7f000001",
        "0177.1",
        "0x",
        "example.0X",
        "::ffff:127.0.0.1",
        "[::ffff:127.0.0.1]",
    ] {
        assert!(!valid_dns_name(name), "{name}");
        assert!(!valid_rule(&format!("*.{name}")), "{name}");
    }
    assert!(valid_dns_name("0x1234.example"));
    assert!(valid_dns_name("127.example"));
}
