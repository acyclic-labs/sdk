//! Pinned public contracts over the original registered capability executors.
use crate::{
    Error, NativeCustody, NativeWork, Result,
    model::NativeToolProfile,
    profile::ToolRevision,
    tool::{Tool, ToolDefinition},
};
use serde_json::Value;
use std::sync::Arc;

mod adapter;
mod arguments;
mod registration;
pub use registration::register_profiles;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Exact source declarations in the profile's registration order. Internal
/// capability metadata is added only when binding; it never reaches a model.
pub const fn declarations(profile: NativeToolProfile) -> &'static str {
    match profile {
        NativeToolProfile::Anthropic => include_str!("anthropic.json"),
        NativeToolProfile::Openai => include_str!("openai.json"),
        NativeToolProfile::Google => include_str!("google.json"),
    }
}

/// A profile's source contracts decoded once during original registry assembly.
/// Declarations move into their selected bindings; there is no second registry.
pub struct NativeDeclarations {
    // Owned source values retire before their original reservation.
    source: Value,
    profile: NativeToolProfile,
    io: Option<crate::filesystem::PreparationIo>,
}

impl NativeDeclarations {
    /// Decodes the immutable public schema list on the caller's original ledger.
    pub fn new(profile: NativeToolProfile, work: NativeWork<'_>) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<crate::filesystem::PreparationIo> = None;
        let _ = work;
        let bytes = declarations(profile).as_bytes();
        let source: Value = match io.as_ref() {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<Value>,
                bytes,
                crate::wire::ErrorCode::Invalid,
            )?)?,
            None => {
                serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        Ok(Self {
            source,
            profile,
            io,
        })
    }

    /// Binds one admitted revision to the original immutable registration.
    /// Does not select tools, install a registry or confer a call grant.
    pub fn bind_variant(
        &mut self,
        revision: &ToolRevision,
        original: Arc<Tool>,
    ) -> Result<(Tool, NativeCustody)> {
        let profile = self.profile;
        let capability = arguments::Capability::parse(&revision.capability)?;
        let core_capability = match capability {
            arguments::Capability::Glob | arguments::Capability::Search => "shell",
            _ => revision.capability.as_str(),
        };
        if original.capability_name() != core_capability {
            return Err(Error::Invalid(
            "native binding requires its original capability; recovery: resolve the admitted profile capability in the original registry".into(),
        ));
        }
        let io = self.io.as_ref();
        let work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<ToolDefinition>() as u64)?;
        }
        let entries = self
            .source
            .as_array_mut()
            .ok_or_else(|| Error::Invalid("native declaration list is invalid".into()))?;
        let index = entries
            .iter()
            .position(|entry| entry["name"] == revision.variant)
            .ok_or_else(|| {
                Error::Invalid("native variant is absent from its pinned public contract".into())
            })?;
        let mut entry = entries.swap_remove(index);
        let name = take_string(&mut entry, "name")?;
        let description = take_string(&mut entry, "description")?;
        let parameters = entry
            .get_mut("parametersJsonSchema")
            .map(Value::take)
            .ok_or_else(|| Error::Invalid("native input contract is absent".into()))?;
        // One funded schema copy adds the canonical policy identity at the same
        // boundary as schema ownership. The shared renderer omits this metadata.
        #[derive(serde::Serialize)]
        struct Input<'a> {
            #[serde(flatten)]
            parameters: &'a Value,
            #[serde(rename = "x-harness-capability")]
            capability: &'a str,
        }
        let input = Input {
            parameters: &parameters,
            capability: &revision.capability,
        };
        let input_schema = match io {
            Some(io) => io.value_json(&input, io.remaining()?.bytes_encoded)?,
            None => {
                serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        let output_schema = crate::model::clone_value(&original.definition.output_schema, io)?;
        let projection_schema =
            crate::model::clone_value(&original.definition.projection_schema, io)?;
        let definition = ToolDefinition {
            name,
            description,
            revision: crate::executor::stock_string_copy(&revision.revision, io)?,
            input_schema,
            output_schema,
            projection_schema,
        };
        drop(parameters);
        drop(entry);
        let mut custody = crate::tool::edit::allocation(
            work,
            std::mem::size_of::<adapter::Adapter>() + 2 * std::mem::size_of::<usize>(),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            use crate::filesystem::UploadRecord as _;
            let retained = (std::mem::size_of::<ToolDefinition>() as u64)
                .checked_add(definition.logical_owned_bytes()?)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native definition ownership overflow",
                })?;
            let mut held = io.take_native_custody(retained)?;
            custody
                .as_mut()
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native adapter custody is absent",
                })?
                .absorb(&mut held)?;
        }
        let hook_names = original.hook_names;
        let adapter = Arc::new(adapter::Adapter {
            profile,
            capability,
            original,
        });
        Ok((
            Tool {
                definition,
                hook_names,
                executor: adapter.clone(),
                projection: adapter,
                skill_source: None,
            },
            custody,
        ))
    }
}

fn take_string(value: &mut Value, key: &str) -> Result<String> {
    match value.get_mut(key).map(Value::take) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(Error::Invalid("native declaration string is absent".into())),
    }
}
