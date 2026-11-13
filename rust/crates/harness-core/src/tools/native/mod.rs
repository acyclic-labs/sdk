//! Pinned public tool contracts for each model family's native tool surface.
//!
//! The family profile layer (`profile/native/*.md`) pins each declaration's
//! capability, variant name and revision; these files pin the exact public
//! descriptions and input schemas. Command declarations use the shared `shell`
//! name while preserving each family's argument adapter.
use crate::{
    Error, Result,
    model::NativeToolProfile,
    tool::{Tool, ToolDefinition, ToolRegistry},
};
use serde_json::Value;
use std::sync::Arc;

mod adapter;
#[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
mod allocation;
mod arguments;

/// The same canonical adapter used for model declarations, with the original
/// capability retaining execution, grants, hooks, replay and result contracts.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn alias(
    mut definition: ToolDefinition,
    original: Arc<Tool>,
    template: Value,
    mut custody: acyclic_fs::NativeCustody,
) -> Result<Tool> {
    let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.tool-alias.v2");
    for revision in [&definition.revision, &original.definition.revision] {
        identity.update(&(revision.len() as u64).to_be_bytes());
        identity.update(revision.as_bytes());
    }
    let io = match &custody {
        Some(custody) => crate::filesystem::PreparationIo::from_native_work(custody.work())?,
        None => {
            crate::filesystem::PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?
        }
    };
    identity.update(&io.canonical_digest(&template)?);
    let original_revision_bytes = definition.revision.capacity() as u64;
    if let Some(custody) = &mut custody {
        custody.grow(64)?;
    }
    definition.revision = identity.finalize().to_hex().to_string();
    if let Some(custody) = &mut custody {
        custody.release(original_revision_bytes)?;
    }
    reserve_shared::<arguments::Arguments>(&mut custody)?;
    let arguments = Arc::new(arguments::Arguments::new(template, Vec::new())?);
    adapted(
        definition,
        Arc::clone(&original),
        original,
        arguments,
        custody,
    )
}

fn adapted(
    mut definition: ToolDefinition,
    capability: Arc<Tool>,
    original: Arc<Tool>,
    arguments: Arc<arguments::Arguments>,
    mut custody: acyclic_fs::NativeCustody,
) -> Result<Tool> {
    definition.output_schema = copy_schema(&original.definition.output_schema, &mut custody)?;
    definition.projection_schema =
        copy_schema(&original.definition.projection_schema, &mut custody)?;
    reserve_shared::<adapter::Adapter>(&mut custody)?;
    let adapter = Arc::new(adapter::Adapter {
        arguments,
        capability,
        original,
        _custody: custody,
    });
    Ok(Tool {
        hook_names: adapter.original.hook_names,
        definition,
        executor: adapter.clone(),
        projection: adapter,
    })
}

fn reserve_shared<T>(custody: &mut acyclic_fs::NativeCustody) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(custody) = custody {
        let bytes = acyclic_fs::NativeMaterializationWork::shared_allocation_bytes::<T>().map_err(
            |_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "shared native tool layout overflow",
            },
        )?;
        custody.grow(bytes)?;
    }
    #[cfg(target_arch = "wasm32")]
    let _ = custody;
    Ok(())
}

fn copy_schema(value: &Value, custody: &mut acyclic_fs::NativeCustody) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(custody) = custody {
        let (value, mut allocation) = crate::tool::clone_value(value, None, custody.work())?;
        custody.absorb(&mut allocation)?;
        return Ok(value);
    }
    #[cfg(target_arch = "wasm32")]
    let _ = custody;
    Ok(value.clone())
}
/// Exercises production native declarations with hostile argument values.
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub use allocation::check as check_native_arguments;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod arguments_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Exact source declarations in the profile's registration order: the
/// seed's `sys/native/<family>/tools.json`.
pub const fn declarations(profile: NativeToolProfile) -> &'static str {
    match profile {
        NativeToolProfile::Anthropic => crate::seed::ANTHROPIC_TOOLS,
        NativeToolProfile::Openai => crate::seed::OPENAI_TOOLS,
        NativeToolProfile::Google => crate::seed::GOOGLE_TOOLS,
    }
}

/// Decodes one profile's public declarations into model-visible definitions,
/// in the profile layer's frozen order. The revision is the profile layer's
/// pinned revision; output and projection schemas belong to the capability
/// executor that binds each declaration, so they are left empty here.
pub fn definitions(profile: NativeToolProfile) -> Result<Vec<ToolDefinition>> {
    decoded(profile)?
        .map(|entry| entry.map(|(_, definition)| definition))
        .collect()
}

/// Decode the mapping and public declaration together. Binding consumes this
/// iterator directly, so it neither reparses the mapping nor collects a second
/// vector of definitions before constructing the executable declarations.
fn decoded(
    profile: NativeToolProfile,
) -> Result<impl Iterator<Item = Result<(crate::profile::ToolRevision, ToolDefinition)>>> {
    let source: Vec<Value> = serde_json::from_str(declarations(profile))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let revisions = profile.tools()?;
    if revisions.len() != source.len() {
        return Err(Error::Invalid(
            "native profile layer and public declarations differ".into(),
        ));
    }
    Ok(revisions
        .into_iter()
        .zip(source)
        .map(|(revision, mut entry)| {
            let name = take_string(&mut entry, "name")?;
            if name != revision.variant {
                return Err(Error::Invalid(
                    "native declaration order differs from its profile layer".into(),
                ));
            }
            let definition = ToolDefinition {
                name,
                revision: revision.revision.clone(),
                description: take_string(&mut entry, "description")?,
                input_schema: entry
                    .get_mut("parametersJsonSchema")
                    .map(Value::take)
                    .ok_or_else(|| Error::Invalid("native input contract is absent".into()))?,
                output_schema: Value::Bool(true),
                projection_schema: Value::Bool(true),
            };
            Ok((revision, definition))
        }))
}

/// One mapped declaration: its capability, the variant that runs it when
/// not the capability's own tool, its definition and its template.
type Mapped = (
    String,
    Option<Arc<Tool>>,
    ToolDefinition,
    Arc<arguments::Arguments>,
);

/// One profile's mapped declarations, decoded and checked once.
pub struct NativeTools(Vec<Mapped>);

impl NativeTools {
    /// The declarations `profile`'s layer maps, in the layer's order.
    /// Unmapped declarations are left out.
    ///
    /// # Errors
    /// [`Error::Invalid`] when the layer, its declarations or a template is malformed.
    pub fn new(profile: NativeToolProfile) -> Result<Self> {
        Self::from_entries(decoded(profile)?)
    }

    /// Binds admitted declarations to their canonical capability mappings.
    /// Names and revisions must match each mapping. Descriptions and input
    /// schemas may change; result contracts, grants and approval remain the
    /// original executor's when [`Self::bind`] selects the capability.
    /// No capability is created by a declaration.
    ///
    /// # Errors
    /// [`Error::Invalid`] for mismatched identities or malformed templates;
    /// the original variant refusal for an unavailable implementation.
    pub fn from_definitions(
        definitions: impl IntoIterator<Item = (crate::profile::ToolRevision, ToolDefinition)>,
    ) -> Result<Self> {
        Self::from_entries(definitions.into_iter().map(|(revision, definition)| {
            if revision.variant != definition.name || revision.revision != definition.revision {
                return Err(Error::Invalid(
                    "native declaration identity differs from its mapping".into(),
                ));
            }
            Ok((revision, definition))
        }))
    }

    fn from_entries(
        entries: impl IntoIterator<Item = Result<(crate::profile::ToolRevision, ToolDefinition)>>,
    ) -> Result<Self> {
        let mut mapped = Vec::new();
        for entry in entries {
            let (revision, definition) = entry?;
            let Some(template) = revision.arguments else {
                continue;
            };
            let implementation = revision
                .implementation
                .as_deref()
                .map(crate::tools::variants::tool)
                .transpose()?
                .map(Arc::new);
            let arguments = arguments::Arguments::new(template, revision.refuse)?;
            mapped.push((
                revision.capability,
                implementation,
                definition,
                Arc::new(arguments),
            ));
        }
        Ok(Self(mapped))
    }

    /// Offers each capability `tools` selects as these declarations. Each runs
    /// the capability's selected revision through its template; its grant,
    /// policy and approval are the capability's. A manifest pins `tools`
    /// first, so the declarations run the revisions it pins.
    ///
    /// # Errors
    /// A registry refusal of a declaration.
    pub fn bind(&self, tools: &mut ToolRegistry) -> Result<()> {
        // Resolve every original selection before registering a declaration can
        // replace it, including one offered under the capability's own name.
        let capabilities = self
            .0
            .iter()
            .map(|(name, ..)| {
                tools.get_shared(name, None, None).map(|tool| {
                    tool.map(|tool| Arc::clone(tool.executor.capability().unwrap_or(tool)))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut presented = std::collections::BTreeMap::<&str, Vec<String>>::new();
        for ((name, implementation, definition, arguments), capability) in
            self.0.iter().zip(capabilities)
        {
            let Some(capability) = capability else {
                continue;
            };
            let original = implementation
                .clone()
                .unwrap_or_else(|| Arc::clone(&capability));
            let already_bound = tools
                .get_shared(&definition.name, Some(&definition.revision), None)?
                .is_some_and(|registered| {
                    let Some(binding) = registered.executor.canonical_binding() else {
                        return false;
                    };
                    let (template, refused) = arguments.binding();
                    Arc::ptr_eq(binding.capability, &capability)
                        && Arc::ptr_eq(binding.implementation, &original)
                        && binding.arguments == template
                        && binding.refused == refused
                        && registered.definition.name == definition.name
                        && registered.definition.revision == definition.revision
                        && registered.definition.description == definition.description
                        && registered.definition.input_schema == definition.input_schema
                        && registered.definition.output_schema == original.definition.output_schema
                        && registered.definition.projection_schema
                            == original.definition.projection_schema
                });
            let native = definition.name.clone();
            let revision = definition.revision.clone();
            if !already_bound {
                tools.register(
                    (
                        adapted(
                            definition.clone(),
                            capability,
                            original,
                            Arc::clone(arguments),
                            None,
                        )?,
                        None,
                    ),
                    &mut None,
                    None,
                )?;
            }
            tools.select_model_version(&native, &revision)?;
            presented.entry(name).or_default().push(native);
        }
        presented
            .into_iter()
            .try_for_each(|(capability, natives)| tools.present_natively(capability, natives))
    }
}

/// Binds `profile`'s mapped declarations into `tools`; see [`NativeTools::bind`].
///
/// # Errors
/// As [`NativeTools::new`] and [`NativeTools::bind`].
pub fn bind(tools: &mut ToolRegistry, profile: NativeToolProfile) -> Result<()> {
    NativeTools::new(profile)?.bind(tools)
}

fn take_string(value: &mut Value, key: &str) -> Result<String> {
    match value.get_mut(key).map(Value::take) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(Error::Invalid("native declaration string is absent".into())),
    }
}
