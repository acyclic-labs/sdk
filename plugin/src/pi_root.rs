//! Immutable physical-root selection retained in the original Pi admission.

use super::{AdapterState, MAXIMUM_ADAPTER_ROOTS, PiTaskBinding, PluginTaskBinding};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

const MAXIMUM_ROOT_PATH_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiRootIntent {
    ScratchOnly {},
    Project {
        context_id: [u8; 16],
        root_id: [u8; 16],
        /// Exact native path strings, not ambient cwd or mutable route aliases.
        paths: BTreeMap<String, String>,
    },
}

pub(crate) fn root_identity_key_matches(key: &str, identity: &[u8; 16]) -> bool {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = key.as_bytes();
    bytes.len() == 32 && identity.iter().enumerate().all(|(index, byte)| {
        bytes[2 * index] == HEX[usize::from(byte >> 4)]
            && bytes[2 * index + 1] == HEX[usize::from(byte & 15)]
    })
}

fn root_selected_identity(paths: &BTreeMap<String, String>, identity: &[u8; 16]) -> bool {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = [0u8; 32];
    for (index, byte) in identity.iter().enumerate() {
        key[2 * index] = HEX[usize::from(byte >> 4)];
        key[2 * index + 1] = HEX[usize::from(byte & 15)];
    }
    // Every assigned byte comes from the ASCII alphabet above.
    std::str::from_utf8(&key).is_ok_and(|key| paths.contains_key(key))
}

fn root_validation_error(reason: &'static str) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason,
    }
}

fn root_validation_step(work: Option<&acyclic_fs::NativeMaterializationWork>, items: u64)
    -> acyclic_harness::Result<()> {
    if let Some(work) = work {
        work.admit(acyclic_fs::WorkCounters { items_examined: items,
            ..acyclic_fs::WorkCounters::UNCHARGED })
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
    }
    Ok(())
}

impl PiRootIntent {
    pub(crate) fn validate(&self, binding: &PiTaskBinding) -> Result<(), String> {
        self.validate_inner(binding, None).map_err(|error| match error {
            acyclic_harness::Error::NativeBoundary { reason, .. } => reason.to_owned(),
            error => super::display(error),
        })
    }

    pub(crate) fn validate_with_native_work(
        &self, binding: &PiTaskBinding, work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        self.validate_inner(binding, Some(work))
    }

    fn validate_inner(&self, binding: &PiTaskBinding,
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<()> {
        root_validation_step(work, 1)?;
        match self {
            Self::ScratchOnly {} if binding.project_volumes.is_empty() => Ok(()),
            Self::Project {
                context_id,
                root_id,
                paths,
            } if *context_id != [0; 16]
                && *root_id != [0; 16]
                && !paths.is_empty()
                && paths.len() <= MAXIMUM_ADAPTER_ROOTS
                && paths.keys().eq(binding.project_volumes.keys())
                && root_selected_identity(paths, root_id) =>
            {
                for (key, path) in paths {
                    root_validation_step(work, key.len() as u64 + path.len() as u64 + 1)?;
                    let mut identity = [0; 16];
                    if key.len() != 32
                        || hex::decode_to_slice(key, &mut identity).is_err()
                        || identity == [0; 16]
                        || !root_identity_key_matches(key, &identity)
                        || path.len() > MAXIMUM_ROOT_PATH_BYTES
                        || path.as_bytes().contains(&0)
                        || !Path::new(path).is_absolute()
                        || Path::new(path)
                            .components()
                            .any(|part| matches!(part, Component::ParentDir))
                    {
                        return Err(root_validation_error(
                            "Pi root intent has an invalid identity or native path"
                        ));
                    }
                }
                Ok(())
            }
            _ => Err(root_validation_error("Pi root intent differs from its selected project roots")),
        }
    }

    /// Captures references only.
    pub(crate) fn from_state(state: &AdapterState) -> Result<Self, String> {
        super::validate_state_version(state)?;
        super::validate_state_bounds(state)?;
        let PluginTaskBinding::Pi(binding) = &state.task_binding else {
            return Err("Pi root intent requires a Pi task binding".to_owned());
        };
        let intent = if state.roots.is_empty()
            && state.root_context_id == [0; 16]
            && state.root_id == [0; 16]
        {
            Self::ScratchOnly {}
        } else {
            let paths = state
                .roots
                .iter()
                .map(|(key, root)| {
                    root.path
                        .to_str()
                        .map(|path| (key.clone(), path.to_owned()))
                        .ok_or_else(|| {
                            "Pi root path has no supported wire representation".to_owned()
                        })
                })
                .collect::<Result<_, _>>()?;
            Self::Project {
                context_id: state.root_context_id,
                root_id: state.root_id,
                paths,
            }
        };
        intent.validate(binding)?;
        Ok(intent)
    }

    pub(crate) fn schema() -> Value {
        serde_json::to_value(Self::schema_descriptor())
            .expect("the static Pi root schema contains only JSON-compatible values")
    }

    pub(crate) fn schema_descriptor() -> PiRootIntentSchema {
        PiRootIntentSchema
    }
}

pub(crate) struct PiRootIntentSchema;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootIntentObject<P, R> {
    #[serde(rename = "type")]
    kind: &'static str,
    additional_properties: bool,
    required: R,
    properties: P,
}

#[derive(Serialize)]
struct RootIntentConstant {
    #[serde(rename = "const")]
    value: &'static str,
}

#[derive(Serialize)]
struct ScratchIntentProperties {
    kind: RootIntentConstant,
}

#[derive(Serialize)]
struct ProjectIntentProperties {
    kind: RootIntentConstant,
    context_id: RootIntentIdentity,
    root_id: RootIntentIdentity,
    paths: RootIntentPaths,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootIntentIdentity {
    #[serde(rename = "type")]
    kind: &'static str,
    min_items: usize,
    max_items: usize,
    items: RootIntentByte,
}

#[derive(Serialize)]
struct RootIntentByte {
    #[serde(rename = "type")]
    kind: &'static str,
    minimum: u8,
    maximum: u8,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootIntentPaths {
    #[serde(rename = "type")]
    kind: &'static str,
    min_properties: usize,
    max_properties: usize,
    property_names: RootIntentPropertyName,
    additional_properties: RootIntentPath,
}

#[derive(Serialize)]
struct RootIntentPropertyName {
    pattern: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootIntentPath {
    #[serde(rename = "type")]
    kind: &'static str,
    min_length: usize,
    max_length: usize,
}

impl Serialize for PiRootIntentSchema {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let identity = || RootIntentIdentity {
            kind: "array", min_items: 16, max_items: 16,
            items: RootIntentByte { kind: "integer", minimum: 0, maximum: 255 },
        };
        let variants = (
            RootIntentObject {
                kind: "object", additional_properties: false, required: ["kind"],
                properties: ScratchIntentProperties {
                    kind: RootIntentConstant { value: "scratch_only" },
                },
            },
            RootIntentObject {
                kind: "object", additional_properties: false,
                required: ["kind", "context_id", "root_id", "paths"],
                properties: ProjectIntentProperties {
                    kind: RootIntentConstant { value: "project" },
                    context_id: identity(), root_id: identity(),
                    paths: RootIntentPaths {
                        kind: "object", min_properties: 1, max_properties: MAXIMUM_ADAPTER_ROOTS,
                        property_names: RootIntentPropertyName { pattern: "^[0-9a-f]{32}$" },
                        additional_properties: RootIntentPath {
                            kind: "string", min_length: 1, max_length: MAXIMUM_ROOT_PATH_BYTES,
                        },
                    },
                },
            },
        );
        let mut schema = serializer.serialize_map(Some(1))?;
        schema.serialize_entry("oneOf", &variants)?;
        schema.end()
    }
}

#[cfg(test)]
mod identity_key_tests {
    use super::*;

    #[test]
    fn root_identity_stack_comparison_preserves_canonical_hex_selection() {
        for offset in 0..16 {
            for byte in 0..=255u8 {
                let mut identity = [1u8; 16];
                identity[offset] = byte;
                let canonical = hex::encode(identity);
                assert!(root_identity_key_matches(&canonical, &identity));
                let paths = BTreeMap::from([(canonical.clone(), "original-root".to_owned())]);
                assert!(root_selected_identity(&paths, &identity));
                let mut substituted = identity;
                substituted[offset] ^= 1;
                assert!(!root_selected_identity(&paths, &substituted));
                if canonical.bytes().any(|byte| byte.is_ascii_lowercase()) {
                    assert!(!root_identity_key_matches(&canonical.to_uppercase(), &identity));
                }
                assert!(!root_identity_key_matches(&canonical[..31], &identity));
            }
        }
    }
}
