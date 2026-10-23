use super::*;
use acyclic_fs::{Digest, GenerationId, ObjectId, ObjectKind, kernel::MetadataField};

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
        let revision = MetadataField::Value(ObjectId {
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
        prop_assert!(!view.matches_policy_revision(head, MetadataField::Value(ObjectId {
            kind: ObjectKind::Metadata, digest: Digest::from_bytes(changed),
        })), "a data pin must not replace the policy fence");
    }
}
