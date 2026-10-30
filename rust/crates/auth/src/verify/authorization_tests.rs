//! Differential proof over real signed authority and current owner state.
use super::*;
use crate::{authority::*, testing as signed};
use acyclic_test_support::differential::observe;

#[test]
fn current_authorization_reuses_admission_and_preserves_every_request_dependency() {
    let mut trust = signed::Snapshot::default();
    let capability = verify(&signed::root(|_| {}), &trust, signed::NOW)
        .unwrap()
        .attenuate(
            vec![
                Caveat {
                    kind: CaveatKind::NotBeforeUnixSeconds(signed::NOW - 1),
                },
                Caveat {
                    kind: CaveatKind::ExpiresAtUnixSeconds(signed::NOW + 3),
                },
                Caveat {
                    kind: CaveatKind::Audience(ResourcePattern {
                        name: "workers:/test".into(),
                        prefix: false,
                    }),
                },
                Caveat {
                    kind: CaveatKind::Holder(Holder {
                        account: Some("account".into()),
                    }),
                },
                Caveat {
                    kind: CaveatKind::Grants(signed::claims::grants("fs:/a", false, "fs/read")),
                },
                Caveat {
                    kind: CaveatKind::Limits(signed::limits(Some(10))),
                },
                Caveat {
                    kind: CaveatKind::Pin(Pin {
                        name: "version".into(),
                        value: PinValue::Literal(b"v1".to_vec()),
                    }),
                },
                Caveat {
                    kind: CaveatKind::Filter(Filter {
                        keys: vec![KeyRange {
                            start: b"a".to_vec(),
                            end: b"z".to_vec(),
                        }],
                        attributes: vec![],
                    }),
                },
            ],
            &[8; 32],
        )
        .unwrap();
    let attributes = RecordAttributes::try_new(vec![Attribute {
        name: "version".into(),
        value: b"v1".to_vec(),
    }])
    .unwrap();
    let empty = RecordAttributes::default();
    let usage = signed::limits(None);
    let excess = signed::limits(Some(11));
    let keys = vec![b"b".to_vec()];
    let foreign_keys = vec![b"zz".to_vec()];
    let request = || Request {
        keys: &keys,
        ..signed::request("fs:/a", &attributes, &usage)
    };
    let former = |request: &Request<'_>, policy, scope, trust: &signed::Snapshot| {
        capability.check(trust, request.now_unix_seconds)?;
        permits(&capability.blocks, request, policy, scope).map_err(VerifyError::Denied)
    };
    for case in [
        "valid",
        "resource",
        "action",
        "audience",
        "holder",
        "pin",
        "keys",
        "usage",
        "scope",
        "policy",
        "canonical-resource",
        "canonical-audience",
        "not-before",
        "expiry",
        "certificate-time",
    ] {
        let mut request = request();
        let mut policy = PolicyDecision::Allow;
        let mut scope = Scope::Exact;
        match case {
            "valid" => (),
            "resource" => {
                request.target = Target::Resource {
                    action: "fs/read",
                    resource: "fs:/b",
                }
            }
            "action" => {
                request.target = Target::Resource {
                    action: "fs/write",
                    resource: "fs:/a",
                }
            }
            "audience" => request.audience = "workers:/other",
            "holder" => request.holder = Some("other-account"),
            "pin" => request.attributes = &empty,
            "keys" => request.keys = &foreign_keys,
            "usage" => request.usage = &excess,
            "scope" => scope = Scope::Follow,
            "policy" => policy = PolicyDecision::Deny,
            "canonical-resource" => {
                request.target = Target::Resource {
                    action: "fs/read",
                    resource: "fs:/a/../a",
                }
            }
            "canonical-audience" => request.audience = "workers:/test/../test",
            "not-before" => request.now_unix_seconds = signed::NOW - 2,
            "expiry" => request.now_unix_seconds = signed::NOW + 3,
            "certificate-time" => request.now_unix_seconds = 0,
            _ => unreachable!(),
        }
        let actual = capability.authorize(&trust, &request, policy, scope);
        assert_eq!(actual, former(&request, policy, scope, &trust), "{case}");
        assert_eq!(actual.is_ok(), case == "valid", "{case}");
    }
    for count in [1, 100, 1000] {
        let ((), work) = observe(
            || (),
            |()| {
                for _ in 0..count {
                    capability
                        .authorize(&trust, &request(), PolicyDecision::Allow, Scope::Exact)
                        .unwrap();
                }
            },
        );
        let work: Vec<StructuralAdmission> = work;
        assert!(work.is_empty());
        // Restoring the former production route must restore one full
        // structural admission per operation. Both routes still check trust.
        let ((), old) = observe(
            || (),
            |()| {
                for _ in 0..count {
                    former(&request(), PolicyDecision::Allow, Scope::Exact, &trust).unwrap();
                }
            },
        );
        let old: Vec<StructuralAdmission> = old;
        assert_eq!(old.len(), count);
    }
    // Retaining the immutable admitted chain must never retain Allow across
    // changes in its original owner, issuer epoch or any revocation dependency.
    for change in [
        "block",
        "key",
        "epoch",
        "environment",
        "issuer",
        "unavailable",
    ] {
        match change {
            "block" => trust.revoked_blocks.push(capability.block_ids()[0]),
            "key" => trust.revoked_keys.push("key".into()),
            "epoch" => trust.minimum_epoch = 2,
            "environment" => trust.environments.clear(),
            "issuer" => trust.issuers.clear(),
            "unavailable" => trust.available = false,
            _ => unreachable!(),
        }
        assert!(
            capability
                .authorize(&trust, &request(), PolicyDecision::Allow, Scope::Exact)
                .is_err(),
            "{change}"
        );
        // Omitting the current-owner check accepts the same otherwise valid
        // request: this is the selective stale-trust negative control.
        assert_eq!(
            permits_current(
                &capability.blocks,
                &request(),
                PolicyDecision::Allow,
                Scope::Exact
            ),
            Ok(())
        );
        trust = signed::Snapshot::default();
    }
}
