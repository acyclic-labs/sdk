use super::*;
use crate::conversation::{VolumeClass, VolumeOwner};
use crate::resources::ProviderRef;
use proptest::prelude::*;

fn matrix_matches(
    bytes: [u8; 16],
    label: String,
    accepts: impl Fn(&VolumeRef, Option<crate::AgentId>) -> bool,
) -> bool {
    let agent = crate::AgentId::from_bytes(bytes);
    let mut other = bytes;
    other[0] ^= 1;
    let other = crate::AgentId::from_bytes(other);
    let provider =
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None).unwrap();
    for class in [
        VolumeClass::Project,
        VolumeClass::AgentPrivate,
        VolumeClass::SessionShared,
    ] {
        for owner in [
            VolumeOwner::Project(label.clone()),
            VolumeOwner::Agent(agent),
            VolumeOwner::Session(label.clone()),
        ] {
            let volume = VolumeRef::new(
                provider.clone(),
                "content".into(),
                class,
                owner.clone(),
                &mut None,
            )
            .unwrap();
            for signed_agent in [None, Some(agent), Some(other)] {
                let expected = class == VolumeClass::AgentPrivate
                    && matches!(owner, VolumeOwner::Agent(_))
                    && signed_agent == Some(agent);
                if accepts(&volume, signed_agent) != expected {
                    return false;
                }
            }
        }
    }
    true
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Only the matching agent's private volume is a private-content binding,
    // independent of labels, grants, or any project/session owner namespace.
    #[test]
    fn private_content_owner_matrix(bytes in any::<[u8; 16]>(), label in "[a-z]{1,32}") {
        prop_assert!(matrix_matches(bytes, label.clone(), private_owner_matches));
        // Broken real policy: check only the volume class, omitting the owner.
        prop_assert!(!matrix_matches(bytes, label, |volume, _| volume.class() == VolumeClass::AgentPrivate));
    }
}
