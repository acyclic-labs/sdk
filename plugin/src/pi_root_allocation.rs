//! Strict successful result of the first-party root allocation effect.

use super::{PiScratchCreation, PiTaskBinding, RootCreation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootAllocation {
    pub(crate) roots: BTreeMap<String, RootCreation>,
    pub(crate) scratch: PiScratchCreation,
}

impl PiRootAllocation {
    /// Checks result references against the immutable original selection.
    pub(crate) fn validate(&self, binding: &PiTaskBinding) -> acyclic_harness::Result<()> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(String::as_str),
            true,
        )?;
        if !self.roots.keys().eq(binding.project_volumes.keys()) {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi allocation differs from its original root selection",
            });
        }
        for creation in self.roots.values() {
            creation
                .validate()
                .map_err(acyclic_harness::Error::Invalid)?;
        }
        self.scratch.validate(&binding.scratch.volume)
    }

    /// Builds the identical closed schema directly through the original SDK observed encoder; no intermediate JSON tree or cloned root map exists.
    pub(crate) fn schema(
        binding: &PiTaskBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        binding.validate(
            None,
            binding.project_volumes.keys().map(String::as_str),
            true,
        )?;
        super::pi_root_provider::guarded_pi_value(
            &RootAllocationSchema(binding),
            super::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            work,
        )
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectSchema<R, P> {
    #[serde(rename = "type")]
    kind: &'static str,
    additional_properties: bool,
    required: R,
    properties: P,
}

fn object_schema<R, P>(required: R, properties: P) -> ObjectSchema<R, P> {
    ObjectSchema {
        kind: "object",
        additional_properties: false,
        required,
        properties,
    }
}

#[derive(Serialize)]
struct ConstantSchema<'a> {
    #[serde(rename = "const")]
    value: &'a acyclic_harness::resources::ProviderRef,
}

/// Borrowed first-party result schema, shared by ordinary and native encoders.
struct RootAllocationSchema<'a>(&'a PiTaskBinding);

const ID_SCHEMA: &str = r#"{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255},"not":{"const":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}}"#;
const KEY_SCHEMA: &str = r#"{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}"#;
const ROOT_SCHEMA: &str = r#"{
    "type":"object","additionalProperties":false,
    "required":["repository_workspace_id","source_identity","source_epoch","native_root_identity"],
    "properties":{
        "repository_workspace_id":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255},"not":{"const":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}},
        "source_identity":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255},"not":{"const":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}},
        "source_epoch":{"type":"integer","minimum":0,"maximum":18446744073709551615},
        "native_root_identity":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255},"not":{"const":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}}
    }
}"#;

fn raw_schema(source: &'static str) -> &'static RawValue {
    serde_json::from_str(source).expect("static Pi allocation schema")
}

#[derive(Serialize)]
struct GenerationProperties<'a> {
    kind: &'static RawValue,
    provider: ConstantSchema<'a>,
    key: &'static RawValue,
    version: &'static RawValue,
}

#[derive(Serialize)]
struct ScratchProperties<'a> {
    workspace_id: &'static RawValue,
    generation: ObjectSchema<[&'static str; 4], GenerationProperties<'a>>,
}

struct SelectedRootKeys<'a>(&'a PiTaskBinding);

impl Serialize for SelectedRootKeys<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(Some(self.0.project_volumes.len()))?;
        for key in self.0.project_volumes.keys() {
            sequence.serialize_element(key)?;
        }
        sequence.end()
    }
}

struct SelectedRootSchemas<'a>(&'a PiTaskBinding);

impl Serialize for SelectedRootSchemas<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut properties = serializer.serialize_map(Some(self.0.project_volumes.len()))?;
        let root = raw_schema(ROOT_SCHEMA);
        for key in self.0.project_volumes.keys() {
            properties.serialize_entry(key, root)?;
        }
        properties.end()
    }
}

#[derive(Serialize)]
struct AllocationProperties<'a> {
    roots: ObjectSchema<SelectedRootKeys<'a>, SelectedRootSchemas<'a>>,
    scratch: ObjectSchema<[&'static str; 2], ScratchProperties<'a>>,
}

impl Serialize for RootAllocationSchema<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        object_schema(
            ["roots", "scratch"],
            AllocationProperties {
                roots: object_schema(SelectedRootKeys(self.0), SelectedRootSchemas(self.0)),
                scratch: object_schema(
                    ["workspace_id", "generation"],
                    ScratchProperties {
                        workspace_id: raw_schema(ID_SCHEMA),
                        generation: object_schema(
                            ["kind", "provider", "key", "version"],
                            GenerationProperties {
                                kind: raw_schema(r#"{"const":"generation"}"#),
                                provider: ConstantSchema {
                                    value: self.0.scratch.volume.provider(),
                                },
                                key: raw_schema(KEY_SCHEMA),
                                version: raw_schema(r#"{"type":"null"}"#),
                            },
                        ),
                    },
                ),
            },
        )
        .serialize(serializer)
    }
}
