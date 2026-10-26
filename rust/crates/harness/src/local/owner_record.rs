use super::{AgentId, TaskId, VolumeRef};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Owner {
    /// Accepted only by the authenticated private-file migration reader.
    #[serde(default, skip_serializing)]
    pub key: Option<[u8; 32]>,
    #[serde(default)]
    pub key_ref: Option<acyclic_native_runtime::signing_key::SigningKeyReference>,
    pub agent: AgentId,
    pub task: TaskId,
    pub project: VolumeRef,
    pub(super) workspace: Option<PathBuf>,
    pub(super) workspace_identity: Option<RootWitness>,
    pub(super) workspace_incarnation: Option<[u8; 16]>,
}

/// Evidence retained independently of the checkout token. A token locates state;
/// it does not prove that a different physical root is the original checkout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum RootWitness {
    Unstable,
    StableObjectId([u8; 16]),
    FileHandle { object: [u8; 16], handle: [u8; 32] },
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(key) = &mut self.key {
            acyclic_native_runtime::owner_record::clear_secret(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conversation::{VolumeClass, VolumeOwner},
        resources::ProviderRef,
    };
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // The on-disk codec preserves arbitrary credentials, identities and
        // Unicode workspace paths; these are synthetic keys, never user state.
        #[test]
        fn owner_record_round_trips(
            key in any::<[u8; 32]>(), agent in any::<[u8; 16]>(),
            task in any::<[u8; 16]>(), identity in proptest::option::of(proptest::prop_oneof![
                Just(RootWitness::Unstable),
                any::<[u8; 16]>().prop_map(RootWitness::StableObjectId),
                (any::<[u8; 16]>(), any::<[u8; 32]>()).prop_map(|(object, handle)| RootWitness::FileHandle { object, handle }),
            ]),
            incarnation in any::<Option<[u8; 16]>>(),
            workspace in proptest::option::of(".{0,128}"),
        ) {
            let owner = Owner {
                key: None, key_ref: Some(serde_json::from_value(serde_json::json!(key)).unwrap()), agent: AgentId::from_bytes(agent), task: TaskId::from_bytes(task),
                project: VolumeRef::new(
                    ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
                    "volume".into(), VolumeClass::Project, VolumeOwner::Project("project".into()), &mut None,
                ).unwrap(),
                workspace: workspace.map(PathBuf::from), workspace_identity: identity,
                workspace_incarnation: incarnation,
            };
            let bytes = serde_json::to_vec(&owner).unwrap();
            let decoded: Owner = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        }
    }
}
