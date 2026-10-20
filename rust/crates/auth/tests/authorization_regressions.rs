//! Properties for record admission, segmented authority and typed socket grants.
mod common;
#[path = "common/limits.rs"]
mod limits;
use acyclic_auth::{authority::*, contract::*, socket::*, *};
use common::*;
use limits::limits;
use proptest::prelude::*;
use prost::Message;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn request<'a>(
    target: Target<'a>,
    attributes: &'a RecordAttributes,
    usage: &'a Limits,
) -> Request<'a> {
    Request {
        target,
        audience: "workers:/app",
        holder: None,
        now_unix_seconds: 2,
        keys: &[],
        attributes,
        usage,
        cpu_millis: 0,
        memory_bytes: 0,
    }
}
fn socket_grants(host: String, ports: Vec<SocketPortRange>) -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::WebsocketConnect(WebsocketConnectGrant {
                rules: vec![SocketHostRule { host, ports }],
            }),
        }],
    }
}
/// Whether two addresses agree on their first `bits` bits.
fn same_prefix(a: &[u8], b: &[u8], bits: usize) -> bool {
    (0..bits).all(|bit| {
        let mask = 128 >> (bit % 8);
        a.get(bit / 8).map(|byte| byte & mask) == b.get(bit / 8).map(|byte| byte & mask)
    })
}
fn socket_root(grants: GrantSet) -> Vec<Block> {
    let mut root = root();
    // The claims fixture certifies exactly one key, which receives the grants.
    if let Some(certificate) = root
        .certificates
        .as_mut()
        .and_then(|chain| chain.keys.first_mut())
    {
        certificate.key.grants = grants;
    }
    vec![root]
}
fn record_bytes(attributes: Vec<Attribute>) -> Vec<u8> {
    RecordAttributesProto {
        attributes: attributes.into_iter().map(AttributeProto::from).collect(),
    }
    .encode_to_vec()
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn duplicate_record_keys_are_never_admitted(
        name in any::<String>(), first in prop::collection::vec(any::<u8>(),0..32), second in prop::collection::vec(any::<u8>(),0..32),
        before in prop::collection::vec(("[a-z]{1,8}",prop::collection::vec(any::<u8>(),0..16)),0..8),
    ) {
        let mut attrs=before.into_iter().map(|(name,value)|Attribute {name,value}).collect::<Vec<_>>();
        attrs.push(Attribute {name:name.clone(),value:first});attrs.insert(0,Attribute {name,value:second});
        prop_assert!(RecordAttributes::decode(&record_bytes(attrs.clone())).is_err());
        prop_assert!(RecordAttributes::try_new(attrs.clone()).is_err());
        let mut principal=root().certificates.unwrap().keys.remove(0).key.principal;
        principal.attributes=attrs;prop_assert!(Principal::try_from(PrincipalProto::from(principal)).is_err());
    }
    // A record cannot satisfy an issuer pin and a victim filter by presenting
    // conflicting copies: admission fails before either consumer can see it.
    #[test]
    fn conflicting_authorship_is_rejected_at_decode(victim in "victim[a-z]{0,12}", reverse in any::<bool>()) {
        let mut attrs=vec![Attribute {name:"author".into(),value:b"issuer".to_vec()},Attribute {name:"author".into(),value:victim.into_bytes()}];
        if reverse {attrs.reverse();}
        prop_assert!(RecordAttributes::decode(&record_bytes(attrs)).is_err());
    }
    #[test]
    fn record_admission_round_trip_iff_names_are_unique(attrs in prop::collection::vec((prop_oneof![Just(String::new()),"[a-c]{1,2}",".{0,16}"],prop::collection::vec(any::<u8>(),0..16)),0..16)) {
        let attrs=attrs.into_iter().map(|(name,value)|Attribute {name,value}).collect::<Vec<_>>();
        let unique=attrs.iter().enumerate().all(|(i,a)|attrs.iter().take(i).all(|b|a.name!=b.name));
        let bytes=record_bytes(attrs.clone());let admitted=RecordAttributes::decode(&bytes);
        prop_assert_eq!(admitted.is_ok(),unique);
        if let Ok(admitted)=admitted {
            prop_assert_eq!(admitted.as_slice(),attrs.as_slice());
            prop_assert_eq!(RecordAttributesProto::from(admitted).encode_to_vec(),bytes);
        }
    }
    // Literal encoded components may contain encoded slashes, but matching
    // always compares whole '/'-delimited component lists, including the root.
    #[test]
    fn resource_prefix_iff_segment_list_prefix(
        prefix in prop::collection::vec(prop::collection::vec(any::<u8>(),1..12),0..4),
        resource in prop::collection::vec(prop::collection::vec(any::<u8>(),1..12),0..6),
        trailing in any::<bool>(),
    ) {
        let encode=|parts:&Vec<Vec<u8>>|format!("fs:/{}",parts.iter().map(|p|encode_component(p)).collect::<Vec<_>>().join("/"));
        let mut prefix_name=encode(&prefix);if trailing && !prefix.is_empty() {prefix_name.push('/');}
        let resource_name=encode(&resource);
        let mut root=root();root.certificates.as_mut().unwrap().keys.first_mut().unwrap().key.grants=grants(&prefix_name,true,"fs/read");
        let attrs=RecordAttributes::default();let usage=limits(None);
        let r=request(Target::Resource {action:"fs/read",resource:&resource_name},&attrs,&usage);
        prop_assert_eq!(permits(&[root.clone()],&r,PolicyDecision::Allow).is_ok(),resource.starts_with(&prefix));
        let mut descendant=prefix;descendant.extend(resource);
        let name=encode(&descendant);
        let inside=request(Target::Resource {action:"fs/read",resource:&name},&attrs,&usage);
        prop_assert!(permits(&[root],&inside,PolicyDecision::Allow).is_ok());
    }
    #[test]
    fn a_bucket_prefix_never_grants_a_sibling(suffix in "[a-z]{1,10}") {
        let mut root=root();root.certificates.as_mut().unwrap().keys.first_mut().unwrap().key.grants=grants("objects:/logs",true,"objects/get");
        let blocks=vec![root];let attrs=RecordAttributes::default();let usage=limits(None);
        for resource in ["objects:/logs","objects:/logs/key"] {
            prop_assert!(permits(&blocks,&request(Target::Resource {action:"objects/get",resource},&attrs,&usage),PolicyDecision::Allow).is_ok(), "bucket and descendants must be permitted");
        }
        let resource=format!("objects:/logs-{suffix}");
        prop_assert!(!permits(&blocks,&request(Target::Resource {action:"objects/get",resource:&resource},&attrs,&usage),PolicyDecision::Allow).is_ok(), "sibling bucket must be denied");
    }
    #[test]
    fn dns_socket_intersections_never_widen(label in "[a-z]{1,12}", other in "[a-z]{1,12}", port in 1..=65535u16, lower in 1..=65535u16, width in 0..100u16) {
        let host=format!("{label}.example.com");let sibling=format!("{other}.other.test");
        let upper=lower.saturating_add(width);
        let parent=socket_root(socket_grants("*.example.com".into(),vec![SocketPortRange {first:1,last:65535}]));
        let child=attenuate(&parent,vec![Caveat {kind:CaveatKind::Grants(socket_grants(host.clone(),vec![SocketPortRange {first:u32::from(lower),last:u32::from(upper)}]))}],vec![6;32]).unwrap();
        let mut certified=parent.clone();
        let certificates=certified.first_mut().unwrap().certificates.as_mut().unwrap();
        let mut next=certificates.keys.first().unwrap().clone();next.key.key_id="child".into();next.key.parent_key_id=Some("key".into());
        next.key.grants=socket_grants(host.clone(),vec![SocketPortRange {first:u32::from(lower),last:u32::from(upper)}]);
        certificates.keys.push(next);
        let attrs=RecordAttributes::default();let usage=limits(None);
        let r=request(Target::Websocket(Destination {host:Host::Dns(&host),port}),&attrs,&usage);
        prop_assert_eq!(permits(&certified,&r,PolicyDecision::Allow).is_ok(),port>=lower && port<=upper);
        prop_assert!(permits(&parent,&r,PolicyDecision::Allow).is_ok());
        prop_assert_eq!(permits(&child,&r,PolicyDecision::Allow).is_ok(),port>=lower && port<=upper);
        let inside=request(Target::Websocket(Destination {host:Host::Dns(&host),port:lower}),&attrs,&usage);
        prop_assert!(permits(&child,&inside,PolicyDecision::Allow).is_ok());
        prop_assert!(!permits(&child,&r,PolicyDecision::Deny).is_ok());
        let denied=request(Target::Websocket(Destination {host:Host::Dns(&sibling),port}),&attrs,&usage);
        prop_assert!(!permits(&parent,&denied,PolicyDecision::Allow).is_ok());
        prop_assert!(!permits(&child,&denied,PolicyDecision::Allow).is_ok());
        let resource=request(Target::Resource {action:"fs/read",resource:"fs:/v"},&attrs,&usage);
        prop_assert!(!permits(&parent,&resource,PolicyDecision::Allow).is_ok());
    }
    #[test]
    fn socket_default_port_and_dns_boundaries(port in 1..=65535u16, label in "[a-z]{1,12}") {
        let parent=socket_root(socket_grants("example.com".into(),vec![]));
        let attrs=RecordAttributes::default();let usage=limits(None);
        let r=request(Target::Websocket(Destination {host:Host::Dns("EXAMPLE.COM."),port}),&attrs,&usage);
        prop_assert_eq!(permits(&parent,&r,PolicyDecision::Allow).is_ok(),port==443);
        let wildcard=socket_root(socket_grants("*.example.com".into(),vec![]));
        for host in ["example.com".to_owned(),format!("{label}example.com"),"example.com.evil".to_owned()] {
            let r=request(Target::Websocket(Destination {host:Host::Dns(&host),port:443}),&attrs,&usage);
            prop_assert!(!permits(&wildcard,&r,PolicyDecision::Allow).is_ok());
        }
        let resource_parent=vec![root()];
        prop_assert!(!permits(&resource_parent,&r,PolicyDecision::Allow).is_ok());
    }
    #[test]
    fn ip_socket_intersections_never_widen(ip in any::<u32>(), prefix in 0..=32u8, ipv6 in any::<u128>(), prefix6 in 0..=128u8, candidate in any::<u32>(), candidate6 in any::<u128>()) {
        let attrs=RecordAttributes::default();let usage=limits(None);
        for (ip,prefix,all) in [(IpAddr::V4(Ipv4Addr::from(ip)),prefix,"0.0.0.0/0"),(IpAddr::V6(Ipv6Addr::from(ipv6)),prefix6,"::/0")] {
            let parent=socket_root(socket_grants(all.into(),vec![]));
            let child=attenuate(&parent,vec![Caveat {kind:CaveatKind::Grants(socket_grants(format!("{ip}/{prefix}"),vec![]))}],vec![6;32]).unwrap();
            let r=request(Target::Websocket(Destination {host:Host::Ip(ip),port:443}),&attrs,&usage);
            prop_assert!(permits(&parent,&r,PolicyDecision::Allow).is_ok());
            prop_assert!(permits(&child,&r,PolicyDecision::Allow).is_ok());
            let exact=attenuate(&child,vec![Caveat {kind:CaveatKind::Grants(socket_grants(ip.to_string(),vec![]))}],vec![7;32]).unwrap();
            prop_assert!(permits(&exact,&r,PolicyDecision::Allow).is_ok());
            prop_assert!(!permits(&exact,&r,PolicyDecision::Deny).is_ok());
            let (other,expected)=match ip {
                IpAddr::V4(ip) => {
                    let a=ip.octets();let b=candidate.to_be_bytes();
                    (IpAddr::V4(Ipv4Addr::from(candidate)), same_prefix(&a, &b, usize::from(prefix)))
                },
                IpAddr::V6(ip) => {
                    let a=ip.octets();let b=candidate6.to_be_bytes();
                    (IpAddr::V6(Ipv6Addr::from(candidate6)), same_prefix(&a, &b, usize::from(prefix)))
                },
            };
            let r=request(Target::Websocket(Destination {host:Host::Ip(other),port:443}),&attrs,&usage);
            prop_assert!(permits(&parent,&r,PolicyDecision::Allow).is_ok());
            prop_assert_eq!(permits(&child,&r,PolicyDecision::Allow).is_ok(),expected);
            prop_assert_eq!(permits(&exact,&r,PolicyDecision::Allow).is_ok(),other==ip);
            let wrong_family=match ip { IpAddr::V4(_)=>IpAddr::V6(Ipv6Addr::LOCALHOST),IpAddr::V6(_)=>IpAddr::V4(Ipv4Addr::LOCALHOST) };
            let r=request(Target::Websocket(Destination {host:Host::Ip(wrong_family),port:443}),&attrs,&usage);
            prop_assert!(!permits(&parent,&r,PolicyDecision::Allow).is_ok());
        }
    }
    #[test]
    fn socket_rule_parser_round_trip(host in prop_oneof![".{0,128}", "[a-z]{1,16}\\.example\\.com", Just("*.example.com".to_owned()), Just("192.0.2.0/24".to_owned()), Just("2001:db8::/32".to_owned())], first in 0..=65536u32, last in 0..=65536u32) {
        let wire=SocketHostRuleProto {host,ports:vec![SocketPortRangeProto {first,last}]};
        if let Ok(value)=SocketHostRule::try_from(wire) {
            let bytes=SocketHostRuleProto::from(value.clone()).encode_to_vec();
            prop_assert_eq!(SocketHostRule::try_from(SocketHostRuleProto::decode(bytes.as_slice()).unwrap()),Ok(value));
        }
    }
    #[test]
    fn child_certificate_identity_never_adds_claims(keep_group in any::<bool>(), keep_role in any::<bool>(), keep_attribute in any::<bool>(), lineage in prop::collection::vec("[a-z]{1,8}",0..4)) {
        let mut chain=root().certificates.unwrap();
        let parent=&mut chain.keys.first_mut().unwrap().key.principal;
        parent.lineage=vec!["attested".into()];parent.attributes=vec![Attribute {name:"tenant".into(),value:b"tenant-a".to_vec()}];
        let mut child=chain.keys.first().unwrap().clone();child.key.key_id="child".into();child.key.parent_key_id=Some("key".into());
        if !keep_group {child.key.principal.groups.clear();}
        if !keep_role {child.key.principal.roles.clear();}
        if !keep_attribute {child.key.principal.attributes.clear();}
        child.key.principal.lineage.extend(lineage);chain.keys.push(child);
        prop_assert!(CertificateChain::try_from(CertificateChainProto::from(chain.clone())).is_ok());
        for change in 0..6 {
            let mut widened=chain.clone();let p=&mut widened.keys.last_mut().unwrap().key.principal;
            match change {
                0 => p.id="another-issuer".into(),
                1 => p.kind=PrincipalKind::Service,
                2 => p.groups.push("unattested-group".into()),
                3 => p.roles.push("unattested-role".into()),
                4 => p.attributes.push(Attribute {name:"new-claim".into(),value:vec![1]}),
                _ => *p.lineage.first_mut().unwrap()="forged-ancestor".into(),
            }
            prop_assert!(CertificateChain::try_from(CertificateChainProto::from(widened)).is_err());
        }
    }
    #[test]
    fn audit_decisions_round_trip_and_unknown_values_fail(value in 1..=5i32, unknown in 6..=i32::MAX) {
        let decision=AuditDecision::try_from(value).unwrap();
        prop_assert_eq!(i32::from(decision),value);
        prop_assert!(AuditDecision::try_from(unknown).is_err());
        let audit=AuditRecord {at_unix_seconds:2,chain_ids:vec![vec![1;32]],subject:root().certificates.unwrap().keys.remove(0).key.principal,lineage:vec![],holder:Holder {account:None},action:"fs/read".into(),resource:"fs:/v".into(),filter_hash:vec![2;32],decision,reason:"policy".into()};
        let wire=AuditRecordProto::from(audit.clone());
        prop_assert_eq!(AuditRecord::try_from(AuditRecordProto::decode(wire.encode_to_vec().as_slice()).unwrap()),Ok(audit));
        let mut invalid=wire.clone();invalid.decision=unknown;
        prop_assert!(AuditRecord::try_from(invalid).is_err());
        let mut unspecified=wire;unspecified.decision=0;
        prop_assert!(AuditRecord::try_from(unspecified).is_err());
    }

}
