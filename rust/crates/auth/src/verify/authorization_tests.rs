//! Differential proof over real signed authority and current owner state.
use super::*;
use crate::{authority::*, testing as signed};
use acyclic_test_support::differential::observe;

#[test]
fn signed_cross_account_requirements_fence_cached_and_compact_authority() {
    #[derive(Default)]
    struct Domains {
        selected: signed::Snapshot,
        caller: signed::Snapshot,
    }
    impl Trust for Domains {
        fn environment_keys(&self, environment: &str) -> Result<Keys<'_>, VerifyError> {
            match environment {
                "caller-environment" => self.caller.environment_keys("test"),
                _ => self.selected.environment_keys(environment),
            }
        }
        fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError> {
            match account {
                "caller-account" => self.caller.account("account"),
                _ => self.selected.account(account),
            }
        }
        fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError> {
            match account {
                "caller-account" => self.caller.revoked("account", check),
                _ => self.selected.revoked(account, check),
            }
        }
    }
    let selected = verify(&signed::root(|_| {}), &Domains::default(), signed::NOW).unwrap();
    let caller = verify(
        &signed::root(|block| {
            let certificates = block.certificates.as_mut().unwrap();
            certificates.birth.birth.environment = "caller-environment".into();
            certificates.birth.birth.account_id = "caller-account".into();
            for certificate in &mut certificates.keys {
                certificate.key.account_id = "caller-account".into();
                certificate.key.parent_revocation_ids.push(vec![12; 32]);
                certificate.key.trust_requirements = selected.requirements().unwrap();
            }
        }),
        &Domains::default(),
        signed::NOW,
    )
    .unwrap()
    .attenuate(
        vec![Caveat {
            kind: CaveatKind::ExpiresAtUnixSeconds(signed::NOW + 3),
        }],
        &[9; 32],
    )
    .unwrap();
    let requirements = caller.requirements().unwrap();
    assert_eq!(
        requirements,
        caller.trust_evidence().unwrap().requirements().unwrap()
    );
    let caller_evidence = caller.trust_evidence().unwrap();
    let requirements = selected.requirements_with(&caller_evidence).unwrap();
    let encoded = signed::root(|block| {
        block.certificates.as_mut().unwrap().keys[0]
            .key
            .trust_requirements = requirements.clone();
    });
    let mut cache = CertificateVerifier::default();
    let joined = cache
        .verify(&encoded, &Domains::default(), signed::NOW)
        .unwrap();
    assert!(selected.contributed_to(&joined).unwrap());
    assert!(caller.contributed_to(&joined).unwrap());
    assert!(caller_evidence.contributed_to(&joined));
    assert!(!caller.preserved_by(&joined).unwrap());
    // Each altered result is still a real, currently authentic signed
    // credential. Its issuer's validity alone cannot prove the contribution.
    for omitted in ["caller", "parent", "inherited", "deadline"] {
        let altered = signed::root(|block| {
            let mut requirements = requirements.clone();
            match omitted {
                "caller" => {
                    requirements.remove(1);
                }
                "parent" => {
                    requirements[1]
                        .blocks
                        .retain(|id| id.as_slice() != [12; 32]);
                }
                "inherited" => {
                    requirements.pop();
                }
                "deadline" => {
                    requirements[1].validity.expires_at_unix_seconds += 1;
                }
                _ => unreachable!(),
            }
            block.certificates.as_mut().unwrap().keys[0]
                .key
                .trust_requirements = requirements;
        });
        let altered = verify(&altered, &Domains::default(), signed::NOW).unwrap();
        assert!(selected.contributed_to(&altered).unwrap(), "{omitted}");
        assert!(!caller.contributed_to(&altered).unwrap(), "{omitted}");
        assert!(!caller_evidence.contributed_to(&altered), "{omitted}");
    }
    let compact = joined.trust_evidence().unwrap();
    assert_eq!(
        joined.effective().expires_at_unix_seconds(),
        signed::NOW + 3,
        "the original session alarm must include the dependency's expiry"
    );
    assert_eq!(
        compact.owned_bytes().unwrap(),
        joined.trust_evidence_storage().unwrap().owned_bytes
    );
    compact.check(&Domains::default(), signed::NOW).unwrap();
    let reopened = joined.seal().unwrap().encode();
    verify(&reopened, &Domains::default(), signed::NOW).unwrap();
    for change in [
        "block",
        "parent",
        "key",
        "epoch",
        "environment",
        "issuer",
        "unavailable",
        "expiry",
    ] {
        let mut domains = Domains::default();
        let mut now = signed::NOW;
        match change {
            "block" => domains.caller.revoked_blocks.push(caller.block_ids()[0]),
            "parent" => domains.caller.revoked_blocks.push([12; 32]),
            "key" => domains.caller.revoked_keys.push("key".into()),
            "epoch" => domains.caller.minimum_epoch = 2,
            "environment" => domains.caller.environments.clear(),
            "issuer" => domains.caller.issuers.clear(),
            "unavailable" => domains.caller.available = false,
            "expiry" => now += 3,
            _ => unreachable!(),
        }
        assert!(compact.check(&domains, now).is_err(), "{change}");
        assert!(cache.verify(&encoded, &domains, now).is_err(), "{change}");
        assert!(verify(&reopened, &domains, now).is_err(), "{change}");
        // A valid selected issuer alone cannot detect loss of the original
        // caller. Removing the signed dependency must expose this negative control.
        verify(&signed::root(|_| {}), &domains, now).unwrap();
    }
}

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
