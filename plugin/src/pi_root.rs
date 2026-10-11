//! Immutable physical-root selection retained in the original Pi admission.

use super::{AdapterState, MAXIMUM_ADAPTER_ROOTS, PiTaskBinding, PluginTaskBinding};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

// Covers supported native paths, including UTF-8 encoding of Windows extended
// paths. The complete request is also subject to the owning runtime's limit.
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

impl PiRootIntent {
    pub(crate) fn validate(&self, binding: &PiTaskBinding) -> Result<(), String> {
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
                && paths.contains_key(&hex::encode(root_id)) =>
            {
                for (key, path) in paths {
                    let mut identity = [0; 16];
                    if key.len() != 32
                        || hex::decode_to_slice(key, &mut identity).is_err()
                        || identity == [0; 16]
                        || hex::encode(identity) != *key
                        || path.len() > MAXIMUM_ROOT_PATH_BYTES
                        || path.as_bytes().contains(&0)
                        || !Path::new(path).is_absolute()
                        || Path::new(path)
                            .components()
                            .any(|part| matches!(part, Component::ParentDir))
                    {
                        return Err(
                            "Pi root intent has an invalid identity or native path".to_owned()
                        );
                    }
                }
                Ok(())
            }
            _ => Err("Pi root intent differs from its selected project roots".to_owned()),
        }
    }

    /// Captures references only. Authenticating them requires comparing this
    /// value with the original immutable admission before any native access.
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
