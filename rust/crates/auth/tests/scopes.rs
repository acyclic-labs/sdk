//! Deferred admission cannot replace exact record checks or current authority.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{
    authority::{PolicyDecision, Scope, Target},
    contract::{Attribute, Caveat, CaveatKind, Pin, PinValue, RecordAttributes},
    verify::{VerifyError, verify},
};

#[test]
fn discovery_defers_only_record_facts_and_never_current_authority() {
    for (scope, action, resource) in [
        (Scope::Follow, "streams/follow", "streams:/events"),
        (Scope::Children, "streams/list", "streams:/events"),
        (Scope::ResourceFollow, "fs/list", "fs:/volume"),
    ] {
        let trust = signed::Snapshot::default();
        let token = signed::root(|block| {
            block.certificates.as_mut().unwrap().keys[0].key.grants =
                signed::claims::grants(resource, true, action);
            block.caveats.push(Caveat {
                kind: CaveatKind::Pin(Pin {
                    name: "record".into(),
                    value: PinValue::Literal(b"ok".to_vec()),
                }),
            });
        });
        let capability = verify(&token, &trust, signed::NOW).unwrap();
        let empty = RecordAttributes::default();
        let usage = signed::limits(None);
        let mut request = signed::request(resource, &empty, &usage);
        request.target = Target::Resource { action, resource };
        assert_eq!(
            capability.authorize(&trust, &request, PolicyDecision::Allow, scope),
            Ok(())
        );
        assert!(
            capability
                .authorize(&trust, &request, PolicyDecision::Allow, Scope::Exact)
                .is_err()
        );
        assert!(
            capability
                .authorize(&trust, &request, PolicyDecision::Deny, scope)
                .is_err()
        );
        let pinned = RecordAttributes::try_new(vec![Attribute {
            name: "record".into(),
            value: b"ok".to_vec(),
        }])
        .unwrap();
        request.attributes = &pinned;
        assert_eq!(
            capability.authorize(&trust, &request, PolicyDecision::Allow, Scope::Exact),
            Ok(())
        );
        for other in [Scope::Follow, Scope::Children, Scope::ResourceFollow] {
            if other != scope {
                assert!(
                    capability
                        .authorize(&trust, &request, PolicyDecision::Allow, other)
                        .is_err()
                );
            }
        }
        request.audience = "workers:/other";
        // The token is explicitly pinned to its recipient, below.
        let audience_bound = capability
            .attenuate(
                vec![Caveat {
                    kind: CaveatKind::Audience(acyclic_auth::contract::ResourcePattern {
                        name: "workers:/test".into(),
                        prefix: false,
                    }),
                }],
                &[9; 32],
            )
            .unwrap();
        assert!(
            audience_bound
                .authorize(&trust, &request, PolicyDecision::Allow, scope)
                .is_err()
        );
        request.audience = "workers:/test";
        request.now_unix_seconds = 1000;
        assert_eq!(
            capability.authorize(&trust, &request, PolicyDecision::Allow, scope),
            Err(VerifyError::Time)
        );
        request.now_unix_seconds = signed::NOW;
        let mut revoked = signed::Snapshot::default();
        revoked.revoked_keys.push("key".into());
        assert!(
            capability
                .authorize(&revoked, &request, PolicyDecision::Allow, scope)
                .is_err()
        );
        let unavailable = signed::Snapshot {
            available: false,
            ..Default::default()
        };
        assert!(
            capability
                .authorize(&unavailable, &request, PolicyDecision::Allow, scope)
                .is_err()
        );
    }
}
