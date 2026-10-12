//! Strict successful result of the first-party root allocation effect.

use super::{PiScratchCreation, PiTaskBinding, RootCreation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootAllocation {
    pub(crate) roots: BTreeMap<String, RootCreation>,
    pub(crate) scratch: PiScratchCreation,
}

impl PiRootAllocation {
    /// Checks result references against the immutable original selection.
    /// No path is opened and no retained route is changed by this check.
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
            creation.validate().map_err(acyclic_harness::Error::Invalid)?;
        }
        self.scratch.validate(&binding.scratch.volume)
    }

    /// Pins selected root keys and the exact original provider in the staged
    /// schema. Resource fields describe only the generation returned by this
    /// effect, rather than a second general-purpose resource wire contract.
    pub(crate) fn schema(binding: &PiTaskBinding) -> acyclic_harness::Result<Value> {
        binding.validate(
            None, binding.project_volumes.keys().map(String::as_str), true,
        )?;
        serde_json::to_value(RootAllocationSchema(binding)).map_err(|source| {
            acyclic_harness::Error::NativeDecode {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                source,
                custody: None,
            }
        })
    }

    /// Builds the identical closed schema directly through the original SDK
    /// observed encoder; no intermediate JSON tree or cloned root map exists.
    pub(crate) fn schema_with_native_work(
        binding: &PiTaskBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(|cause| {
            acyclic_harness::Error::NativeWork { cause, custody: None }
        })?;
        binding.validate(
            None, binding.project_volumes.keys().map(String::as_str), true,
        )?;
        super::pi_root_provider::guarded_pi_value(
            &RootAllocationSchema(binding), super::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work,
        )
    }
}

/// Borrowed first-party result schema, shared by ordinary and native encoders.
struct RootAllocationSchema<'a>(&'a PiTaskBinding);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectSchema<R, P> {
    #[serde(rename = "type")]
    kind: &'static str,
    additional_properties: bool,
    required: R,
    properties: P,
}

fn object<R, P>(required: R, properties: P) -> ObjectSchema<R, P> {
    ObjectSchema { kind: "object", additional_properties: false, required, properties }
}

#[derive(Clone, Copy, Serialize)]
struct Constant<T> {
    #[serde(rename = "const")]
    value: T,
}

#[derive(Clone, Copy, Serialize)]
struct IntegerSchema {
    #[serde(rename = "type")]
    kind: &'static str,
    minimum: u64,
    maximum: u64,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct ByteArraySchema {
    #[serde(rename = "type")]
    kind: &'static str,
    min_items: usize,
    max_items: usize,
    items: IntegerSchema,
    #[serde(skip_serializing_if = "Option::is_none")]
    not: Option<Constant<[u8; 16]>>,
}

fn bytes_schema(length: usize, nonzero: bool) -> ByteArraySchema {
    ByteArraySchema {
        kind: "array", min_items: length, max_items: length,
        items: IntegerSchema { kind: "integer", minimum: 0, maximum: 255 },
        not: nonzero.then_some(Constant { value: [0; 16] }),
    }
}

#[derive(Serialize)]
struct RootProperties {
    repository_workspace_id: ByteArraySchema,
    source_identity: ByteArraySchema,
    source_epoch: IntegerSchema,
    native_root_identity: ByteArraySchema,
}

fn root_schema() -> ObjectSchema<[&'static str; 4], RootProperties> {
    object(
        ["repository_workspace_id", "source_identity", "source_epoch", "native_root_identity"],
        RootProperties {
            repository_workspace_id: bytes_schema(16, true),
            source_identity: bytes_schema(16, true),
            source_epoch: IntegerSchema { kind: "integer", minimum: 0, maximum: u64::MAX },
            native_root_identity: bytes_schema(16, true),
        },
    )
}

#[derive(Serialize)]
struct ScalarSchema {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Serialize)]
struct GenerationProperties<'a> {
    kind: Constant<&'static str>,
    provider: Constant<&'a acyclic_harness::resources::ProviderRef>,
    key: ByteArraySchema,
    version: ScalarSchema,
}

#[derive(Serialize)]
struct ScratchProperties<'a> {
    workspace_id: ByteArraySchema,
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
        for key in self.0.project_volumes.keys() {
            properties.serialize_entry(key, &root_schema())?;
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
        object(["roots", "scratch"], AllocationProperties {
            roots: object(SelectedRootKeys(self.0), SelectedRootSchemas(self.0)),
            scratch: object(["workspace_id", "generation"], ScratchProperties {
                workspace_id: bytes_schema(16, true),
                generation: object(["kind", "provider", "key", "version"], GenerationProperties {
                    kind: Constant { value: "generation" },
                    provider: Constant { value: self.0.scratch.volume.provider() },
                    key: bytes_schema(32, false),
                    version: ScalarSchema { kind: "null" },
                }),
            }),
        }).serialize(serializer)
    }
}
