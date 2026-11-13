use super::*;
use acyclic_fs::{Digest, GenerationId, ObjectId, ObjectKind, kernel::MetadataField};

#[test]
fn clock_rollback_invalidates_reused_reads_and_publication_authority() -> Result<(), InvalidPolicy>
{
    let caller = principal();
    let mut denied = rule("temporary-denial", Effect::Deny, &[Action::Read], "**");
    denied.conditions.expires_at = Some(100);
    let policy = policy(vec![
        rule("allow", Effect::Allow, &[Action::Read], "**"),
        denied,
    ]);
    let cap = capability();
    let selected = resource(&caller.lineage, "file", 10);
    let admitted = Context {
        now: 100,
        approvals: 0,
    };
    let earlier = Context {
        now: 99,
        ..admitted
    };
    let digest = Digest::from_bytes([7; 32]);
    let revision = MetadataField::Value(ObjectId {
        storage: None,
        kind: ObjectKind::Metadata,
        digest,
    });
    let view = PathTrie::compile(&policy, &caller, selected, admitted, &cap)?
        .with_policy_revision(GenerationId::new(digest), revision);
    assert!(view.decide(Action::Read, "file", admitted.now).allowed);
    assert_eq!(
        decide(&policy, &caller, Action::Read, selected, earlier, &cap).denial,
        Some(Denial::Rule)
    );
    let read_blocked = !view.decide(Action::Read, "file", earlier.now).allowed;
    let reopened = PathTrie::compile(&policy, &caller, selected, earlier, &cap)?;
    assert_eq!(
        reopened.decide(Action::Read, "file", earlier.now).denial,
        Some(Denial::Rule)
    );
    let fence = view.publication_fence().expect("authenticated policy cut");
    assert!(fence.valid_at(admitted.now));
    let publication_blocked = !fence.valid_at(earlier.now);
    assert!(
        read_blocked && publication_blocked,
        "clock rollback must refuse reused authority: read={read_blocked} publication={publication_blocked}"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A data pin cannot change during publication, and cannot replace the
    // independent persisted-policy fence used to admit the operation.
    #[test]
    fn immutable_data_binding_preserves_the_current_policy_fence(
        selected in any::<[u8; 32]>(), other in any::<[u8; 32]>(),
        policy_id in any::<[u8; 32]>(),
    ) {
        let caller = principal();
        let policy = policy(vec![rule("allow", Effect::Allow, &Action::ALL, "**")]);
        let head = GenerationId::new(Digest::from_bytes(policy_id));
        let revision = MetadataField::Value(ObjectId { storage: None,
            kind: ObjectKind::Metadata, digest: Digest::from_bytes(policy_id),
        });
        let build = |data| PathTrie::compile(&policy, &caller, resource(&caller.lineage, "", 1),
            Context { now: 0, approvals: 0 }, &capability()).unwrap()
                .with_policy_revision(head, revision)
                .with_data_generation(GenerationId::new(Digest::from_bytes(data)));
        let view = build(selected);
        prop_assert_eq!(view.data_generation(), Some(GenerationId::new(Digest::from_bytes(selected))));
        prop_assert_eq!(view.same_binding(&build(other)), selected == other);
        prop_assert!(view.matches_policy_revision(head, revision));
        let mut changed = policy_id;
        changed[0] ^= 1;
        prop_assert!(!view.matches_policy_revision(head, MetadataField::Value(ObjectId { storage: None,
            kind: ObjectKind::Metadata, digest: Digest::from_bytes(changed),
        })), "a data pin must not replace the policy fence");
    }
}
