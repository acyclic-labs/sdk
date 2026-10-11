//! Declared, bounded filesystem discovery feeding the original ordered context pipeline.

use super::{ContextInput, ContextPlacement, ContextSource, SourceStage};
use crate::{
    Error, Result,
    conversation::{
        ContentResidencyVerifier, FileRef, PrivateDirectoryEntryKind, VolumeRef,
        validate_content_path,
    },
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole},
    resources::GenerationRef,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// A host-declared directory boundary. A declaration conveys no authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextRoot {
    /// Exact volume identity, independently of consumer role.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: VolumeRef,
    /// Exact owner-issued subtree capability boundary; empty denotes the volume root.
    pub directory: String,
}

impl ContextRoot {
    fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    fn validate_with_work(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.volume.validate_with_work(work)?;
        if !self.directory.is_empty() {
            crate::conversation::validate_content_path_with_work(&self.directory, work)?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            let capability = self.volume.directory_read_capability_with_native_work(&self.directory, native)?;
            drop(capability);
            return Ok(());
        }
        self.volume.directory_read_capability(&self.directory)?;
        Ok(())
    }
}

/// Repository instruction scope, evaluated from the declared root to the active directory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct InstructionScope {
    /// Declared repository boundary.
    pub root: ContextRoot,
    /// Directory relative to that boundary; empty selects only root instructions.
    pub active_directory: String,
}

/// Replaceable discovery policy. Empty lists disable either discovery independently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextDiscoveryPolicy {
    /// Instruction filenames, evaluated in this order at each ancestor scope.
    pub instruction_names: Vec<String>,
    /// Skill entry filename. `None` disables skill discovery.
    pub skill_name: Option<String>,
}

/// Refresh timing selected by the embedding admitted workflow.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextReloadPolicy {
    /// Reuse the captured revision until the caller explicitly invokes capture.
    #[default]
    Explicit,
    /// Capture a replacement at the next admitted request boundary.
    NextRequest,
}

/// Ordinary declaration values used by both explicit reload and live bindings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextDiscovery {
    /// Ordered repository scopes.
    pub instructions: Vec<InstructionScope>,
    /// Ordered independent skill roots (builtins and editable skills can be separate).
    pub skills: Vec<ContextRoot>,
    /// Replaceable filenames and enablement policy.
    pub policy: ContextDiscoveryPolicy,
    /// Explicit work and byte limits.
    pub limits: ContextDiscoveryLimits,
}

impl ContextDiscovery {
    /// Captures a complete replacement. Failure leaves the caller's prior revision intact.
    pub async fn capture(
        &self,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<DiscoveredContext> {
        DiscoveredContext::capture(
            reader,
            &self.instructions,
            &self.skills,
            &self.policy,
            self.limits,
        )
        .await
    }

    /// Selects an immutable revision before model admission. This does not install
    /// a watcher or mutate the captured context used by an in-flight request.
    /// The caller admits and records refresh through its existing workflow journal.
    pub async fn for_request(
        &self,
        reader: &dyn ContentResidencyVerifier,
        current: &DiscoveredContext,
        reload: ContextReloadPolicy,
    ) -> Result<DiscoveredContext> {
        match reload {
            ContextReloadPolicy::Explicit => {
                current.validate()?;
                Ok(current.clone())
            }
            ContextReloadPolicy::NextRequest => self.capture(reader).await,
        }
    }
}

impl Default for ContextDiscoveryPolicy {
    fn default() -> Self {
        Self {
            instruction_names: vec!["AGENTS.md".into()],
            skill_name: Some("SKILL.md".into()),
        }
    }
}

/// Explicit finite discovery bounds, independent of the retained workspace count.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextDiscoveryLimits {
    /// Combined directory entries inspected, including nonmatching names.
    pub entries: u32,
    /// Combined directories visited, including instruction ancestor scopes.
    pub directories: u32,
    /// Maximum frontmatter prefix bytes read per skill.
    pub header_bytes: u32,
    /// Maximum complete instruction bytes per file.
    pub instruction_bytes: u64,
}

impl ContextDiscoveryLimits {
    fn validate(self) -> Result<()> {
        if self.entries == 0
            || self.directories == 0
            || self.header_bytes == 0
            || self.instruction_bytes == 0
        {
            return Err(Error::Invalid(
                "discovery requires finite positive bounds".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable path selected by discovery; resolving it never follows a newer head.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct PinnedContextPath {
    /// Declared volume and read boundary.
    pub root: ContextRoot,
    /// Exact retained filesystem generation.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
    )]
    pub generation: GenerationRef,
    /// Volume-relative path.
    pub path: String,
}

impl PinnedContextPath {
    fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    fn validate_with_work(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.root.validate_with_work(work)?;
        self.generation.validate_with_work(work)?;
        crate::conversation::validate_content_path_with_work(&self.path, work)?;
        if self.generation.as_resource().provider() != self.root.volume.provider()
            || (!self.root.directory.is_empty()
                && !self
                    .path
                    .strip_prefix(&self.root.directory)
                    .is_some_and(|tail| tail.starts_with('/')))
            || crate::conversation::is_internal_path(&self.path)
        {
            return Err(discovery_error(work, crate::wire::ErrorCode::Invalid,
                "pinned context path is outside its declared root"));
        }
        Ok(())
    }

    /// Loads a body through the ordinary authorized pinned read, checking provider output.
    pub async fn read(&self, reader: &dyn ContentResidencyVerifier) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.validate()?;
        let (file, bytes) = reader
            .read_private_path(
                &self.root.volume,
                &self.root.directory,
                &self.path,
                Some(&self.generation),
            )
            .await?;
        if file.volume() != &self.root.volume || file.path() != self.path {
            return Err(Error::Storage(
                "pinned path reader returned another file".into(),
            ));
        }
        file.descriptor().verify(&bytes)?;
        Ok((file, bytes))
    }
}

/// Frontmatter only; the body is not fetched or injected by discovery.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct SkillMetadata {
    /// Stable frontmatter name.
    pub name: String,
    /// Frontmatter description.
    pub description: String,
    /// Additional declarative frontmatter fields; no execution authority is inferred.
    #[cfg_attr(feature = "wasm", tsify(type = "Record<string, WasmModelJsonValue>"))]
    pub fields: BTreeMap<String, serde_json::Value>,
    /// Exact lazy body location and provenance.
    pub source: PinnedContextPath,
}

impl SkillMetadata {
    /// Validates restored metadata without performing reads.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    fn validate_with_work(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.source.validate_with_work(work)?;
        if self.name.is_empty()
            || self.name.starts_with('-')
            || self.name.ends_with('-')
            || self.name.contains("--")
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || self.description.trim().is_empty()
            || self
                .description
                .chars()
                .any(|ch| ch.is_control() && !ch.is_whitespace())
            || self.fields.contains_key("name")
            || self.fields.contains_key("description")
        {
            return Err(discovery_error(work, crate::wire::ErrorCode::Invalid, "skill metadata is invalid"));
        }
        Ok(())
    }
}

fn discovery_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return super::native_context_boundary(code, reason);
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn discovery_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn bound_skill_values(
    skills: &[SkillMetadata], maximum_bytes: u64, io: &mut dyn crate::store::history::HistoryIo,
) -> Result<()> {
    fn byte_bound(bytes: usize, maximum: u64) -> Result<()> {
        if bytes as u64 > maximum {
            return Err(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
                "JSON exceeds declared byte bound"));
        }
        Ok(())
    }
    fn provider_bound(provider: &crate::resources::ProviderRef, maximum: u64) -> Result<()> {
        byte_bound(provider.namespace().len(), maximum)?;
        byte_bound(provider.family().len(), maximum)?;
        byte_bound(provider.version().len(), maximum)
    }
    fn value_bound(
        value: &serde_json::Value, depth: u32, maximum: u64,
        io: &mut dyn crate::store::history::HistoryIo,
    ) -> Result<()> {
        io.check_cancellation()?;
        if depth > 256 {
            return Err(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
                "JSON exceeds declared structural bound"));
        }
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        match value {
            serde_json::Value::String(text) => byte_bound(text.len(), maximum)?,
            serde_json::Value::Array(values) => {
                byte_bound(values.len(), maximum)?;
                for value in values { value_bound(value, depth + 1, maximum, io)?; }
            }
            serde_json::Value::Object(values) => {
                byte_bound(values.len(), maximum)?;
                for (key, value) in values {
                    io.check_cancellation()?;
                    io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                    byte_bound(key.len(), maximum)?;
                    value_bound(value, depth + 1, maximum, io)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    // These are implied byte bounds, not a new cap on discovery or retained workspaces.
    byte_bound(skills.len(), maximum_bytes)?;
    for skill in skills {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        byte_bound(skill.name.len(), maximum_bytes)?;
        byte_bound(skill.description.len(), maximum_bytes)?;
        byte_bound(skill.fields.len(), maximum_bytes)?;
        byte_bound(skill.source.root.directory.len(), maximum_bytes)?;
        byte_bound(skill.source.path.len(), maximum_bytes)?;
        let volume = &skill.source.root.volume;
        byte_bound(volume.id().len(), maximum_bytes)?;
        provider_bound(volume.provider(), maximum_bytes)?;
        if let crate::conversation::VolumeOwner::Project(name) = volume.owner() {
            byte_bound(name.len(), maximum_bytes)?;
        }
        let generation = skill.source.generation.as_resource();
        provider_bound(generation.provider(), maximum_bytes)?;
        byte_bound(generation.key().len(), maximum_bytes)?;
        if let Some(version) = generation.version() { byte_bound(version.len(), maximum_bytes)?; }
        for (key, value) in &skill.fields {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            byte_bound(key.len(), maximum_bytes)?;
            // skills array -> skill object -> fields object -> actual field value.
            value_bound(value, 3, maximum_bytes, io)?;
        }
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn skill_text_native(
    skills: &[SkillMetadata], size: u64, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    struct Writer<'a, 'b> {
        bytes: &'a mut Vec<u8>,
        maximum: usize,
        work: &'b acyclic_fs::NativeMaterializationWork,
        failure: Option<Error>,
    }
    impl Writer<'_, '_> {
        fn fail(&mut self, error: Error) -> std::io::Error {
            self.failure = Some(error);
            // Serde's original io-error conversion allocates its one boxed ErrorImpl.
            if let Err(cause) = self.work.spent(acyclic_fs::WorkCounters {
                allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
            }) {
                self.failure = Some(discovery_work_error(cause));
            }
            std::io::ErrorKind::Other.into()
        }
    }
    impl std::io::Write for Writer<'_, '_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let observed = self.work.spent(acyclic_fs::WorkCounters {
                bytes_encoded: bytes.len() as u64, ..acyclic_fs::WorkCounters::default()
            });
            if let Err(cause) = observed { return Err(self.fail(discovery_work_error(cause))); }
            if let Err(cause) = self.work.remaining() { return Err(self.fail(discovery_work_error(cause))); }
            if self.bytes.len().checked_add(bytes.len()).is_none_or(|end| end > self.maximum) {
                return Err(self.fail(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
                    "discovered skill serialization exceeded its counted size")));
            }
            if let Err(cause) = self.work.admit(acyclic_fs::WorkCounters {
                bytes_copied: bytes.len() as u64, ..acyclic_fs::WorkCounters::default()
            }) {
                return Err(self.fail(discovery_work_error(cause)));
            }
            self.bytes.extend_from_slice(bytes);
            if let Err(cause) = self.work.spent(acyclic_fs::WorkCounters {
                bytes_copied: bytes.len() as u64, ..acyclic_fs::WorkCounters::default()
            }) {
                return Err(self.fail(discovery_work_error(cause)));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let maximum = usize::try_from(size).map_err(|_| super::native_context_boundary(
        crate::wire::ErrorCode::Invalid, "discovered skill serialization capacity overflows"))?;
    let mut output = (Vec::new(), work.retain_owned(0).map_err(discovery_work_error)?);
    let mut backing = super::reserve_context_vec_native(&mut output.0, maximum, work)?;
    output.1.absorb(&mut backing).map_err(discovery_work_error)?;
    let labels = ["SkillMetadata", "name", "description", "fields", "source",
        "PinnedContextPath", "root", "generation", "path", "ContextRoot", "volume", "directory",
        "VolumeRef", "ProviderRef", "namespace", "family", "version", "owner", "class",
        "GenerationRef", "ResourceRef", "kind", "provider", "key",
        "key must be a string", "number out of range"];
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        size, labels.iter().map(|label| label.len() as u64 + 4).sum())?;
    let diagnostic = work.retain_owned(diagnostic_bytes).map_err(discovery_work_error)?;
    work.admit(acyclic_fs::WorkCounters { bytes_encoded: size, bytes_copied: size,
        allocation_operations: 1, ..acyclic_fs::WorkCounters::default() }).map_err(discovery_work_error)?;
    let mut writer = Writer { bytes: &mut output.0, maximum, work, failure: None };
    let serialized = serde_json::to_writer(&mut writer, skills);
    let failure = writer.failure.take();
    drop(writer);
    if let Err(source) = serialized {
        if let Some(failure) = failure {
            drop(source);
            drop(diagnostic);
            return Err(failure);
        }
        if let Err(cause) = work.spent(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
        }) {
            drop(source);
            return Err(Error::NativeWork { cause, custody: Some(diagnostic) });
        }
        return Err(Error::NativeEncode { code: crate::wire::ErrorCode::Invalid,
            source, custody: Some(diagnostic) });
    }
    drop(diagnostic);
    if output.0.len() != maximum {
        return Err(super::native_context_boundary(crate::wire::ErrorCode::Invalid,
            "discovered skill serialization changed its counted size"));
    }
    work.remaining().map_err(discovery_work_error)?;
    let text = String::from_utf8(output.0).map_err(|_| super::native_context_boundary(
        crate::wire::ErrorCode::Invalid, "discovered skill serialization is not UTF-8"))?;
    work.remaining().map_err(discovery_work_error)?;
    Ok((text, output.1))
}

#[derive(Deserialize)]
struct Frontmatter {
    name: String,
    description: String,
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,
}

/// Parses a bounded prefix. Missing delimiters, invalid UTF-8/YAML and oversized
/// frontmatter are explicit errors; the trailing body need not be valid UTF-8 yet.
pub fn parse_skill_metadata(prefix: &[u8], source: PinnedContextPath) -> Result<SkillMetadata> {
    source.validate()?;
    let mut offset = 0;
    let mut header_start = None;
    let mut header_end = None;
    for line in prefix.split_inclusive(|byte| *byte == b'\n') {
        let delimiter = line.strip_suffix(b"\n").unwrap_or(line);
        let delimiter = delimiter.strip_suffix(b"\r").unwrap_or(delimiter);
        if offset == 0 {
            if delimiter != b"---" || !line.ends_with(b"\n") {
                return Err(Error::Invalid(
                    "skill requires opening frontmatter delimiter".into(),
                ));
            }
            header_start = Some(line.len());
        } else if delimiter == b"---" && line.ends_with(b"\n") {
            header_end = Some(offset);
            break;
        }
        offset += line.len();
    }
    let (Some(start), Some(end)) = (header_start, header_end) else {
        return Err(Error::Invalid(
            "skill frontmatter is missing or exceeds prefix bound".into(),
        ));
    };
    let header = prefix
        .get(start..end)
        .ok_or_else(|| Error::Invalid("invalid skill header".into()))?;
    // Capture has already enforced the caller's prefix budget. Do not add
    // unrelated ceilings on the contents of that admitted input. One document
    // and rejection of expansion features are frontmatter format restrictions.
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: usize::MAX, max_nodes: usize::MAX, max_depth: usize::MAX,
            flow_nesting_limit: usize::MAX, max_documents: 1,
            max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
            max_total_scalar_bytes: usize::MAX,
        },
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        reject_unsupported_tags: true,
        emit_comments: false,
        with_snippet: false,
    };
    let metadata: Frontmatter = serde_saphyr::from_slice_with_options(header, options)
        .map_err(|error| Error::Invalid(format!("invalid skill frontmatter: {error}")))?;
    let metadata = SkillMetadata {
        name: metadata.name,
        description: metadata.description,
        fields: metadata.fields,
        source,
    };
    metadata.validate()?;
    Ok(metadata)
}

/// Immutable discovery revision. Serialize this value through the existing admitted
/// caller journal for restart; it contains no credentials or mutable provider handles.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct DiscoveredContext {
    /// Root-to-leaf instructions in declared root/filename order.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire[]"))]
    pub instructions: Vec<FileRef>,
    /// Skills in declared root and lexical directory order; duplicate names reject discovery.
    pub skills: Vec<SkillMetadata>,
}

struct DiscoveryWork {
    entries: u32,
    directories: u32,
    limits: ContextDiscoveryLimits,
}

impl DiscoveryWork {
    async fn list(
        &mut self,
        reader: &dyn ContentResidencyVerifier,
        root: &ContextRoot,
        path: &str,
        generation: Option<&GenerationRef>,
    ) -> Result<(
        GenerationRef,
        Vec<crate::conversation::PrivateDirectoryEntry>,
    )> {
        self.directories = self
            .directories
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("discovery work overflow".into()))?;
        if self.directories > self.limits.directories {
            return Err(Error::Invalid("discovery directory bound exceeded".into()));
        }
        let mut pinned = generation.cloned();
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let remaining = self
                .limits
                .entries
                .checked_sub(self.entries)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| Error::Invalid("discovery entry bound exceeded".into()))?;
            let page = reader
                .list_private_directory(
                    &root.volume,
                    &root.directory,
                    path,
                    pinned.as_ref(),
                    cursor.as_deref(),
                    remaining.min(128),
                )
                .await?;
            page.validate()?;
            if page.generation.as_resource().provider() != root.volume.provider()
                || pinned
                    .as_ref()
                    .is_some_and(|generation| generation != &page.generation)
                || page.entries.len() > remaining.min(128) as usize
                || page.entries.first().is_some_and(|entry| {
                    cursor
                        .as_ref()
                        .is_some_and(|cursor: &String| entry.name.as_bytes() <= cursor.as_bytes())
                })
                || page.has_more && page.entries.is_empty()
            {
                return Err(Error::Storage(
                    "discovery page violated pinned range".into(),
                ));
            }
            pinned = Some(page.generation);
            self.entries += u32::try_from(page.entries.len())
                .map_err(|_| Error::Invalid("entry count overflow".into()))?;
            cursor = page.entries.last().map(|entry| entry.name.clone());
            entries.extend(page.entries);
            if !page.has_more {
                break;
            }
        }
        Ok((
            pinned.ok_or_else(|| Error::Storage("missing discovery generation".into()))?,
            entries,
        ))
    }
}

fn child(directory: &str, name: &str) -> String {
    if directory.is_empty() {
        name.into()
    } else {
        format!("{directory}/{name}")
    }
}

impl DiscoveredContext {
    /// Checks a serialized snapshot without reading providers or granting authority.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    fn validate_with_work(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        for file in &self.instructions {
            file.validate_with_work(work)?;
        }
        let name_slots = if work.is_some() {
            self.skills.len().checked_mul(std::mem::size_of::<&String>())
                .and_then(|bytes| u64::try_from(bytes).ok()).ok_or_else(||
                    discovery_error(work, crate::wire::ErrorCode::Invalid, "discovered context count overflows"))?
        } else { 0 };
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(name_slots)?;
            io.retain_owned(name_slots)?;
        }
        let mut names = std::collections::BTreeSet::new();
        for skill in &self.skills {
            skill.validate_with_work(work)?;
            if !names.insert(&skill.name) {
                return Err(discovery_error(work, crate::wire::ErrorCode::Conflict,
                    "discovered skill names are repeated"));
            }
        }
        drop(names);
        if let Some(io) = work.as_deref_mut() { io.release_owned(name_slots)?; }
        Ok(())
    }

    /// Projects this immutable snapshot into ordinary typed model messages.
    /// Body resolution remains the provider's bounded full/reference contract.
    pub fn messages(&self) -> Result<Vec<ModelMessage>> {
        self.messages_with_io(None, None)
    }

    fn messages_with_io(
        &self, io: Option<&super::ContextPreparationIo>, skill_bytes: Option<u64>,
    ) -> Result<Vec<ModelMessage>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let mut observer = crate::store::history::native_work_io(work).map_err(discovery_work_error)?;
            self.validate_with_work(&mut Some(&mut observer))?;
            let count = self.instructions.len().checked_add(usize::from(!self.skills.is_empty()))
                .ok_or_else(|| super::native_context_boundary(crate::wire::ErrorCode::Invalid,
                    "discovered context count overflows"))?;
            let mut messages = (Vec::new(), work.retain_owned(0).map_err(discovery_work_error)?);
            let mut backing = super::reserve_context_vec_native(&mut messages.0, count, work)?;
            messages.1.absorb(&mut backing).map_err(discovery_work_error)?;
            for file in &self.instructions {
                let mut file = super::clone_file_native(file, work)?;
                messages.1.absorb(&mut file.1).map_err(discovery_work_error)?;
                messages.0.push(ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Part(ModelContentPart::File {
                        file: file.0, policy: FileProjectionPolicy::BoundedFull,
                    }),
                });
            }
            if !self.skills.is_empty() {
                let size = skill_bytes.ok_or_else(|| super::native_context_boundary(
                    crate::wire::ErrorCode::Invalid, "discovered skill serialization was not bounded"))?;
                let mut text = skill_text_native(&self.skills, size, work)?;
                messages.1.absorb(&mut text.1).map_err(discovery_work_error)?;
                messages.0.push(ModelMessage { role: ModelRole::System, content: ModelContent::Text(text.0) });
            }
            io.absorb_native_custody(messages.1)?;
            return Ok(messages.0);
        }
        let _ = (io, skill_bytes);
        self.validate()?;
        let mut messages: Vec<_> = self
            .instructions
            .iter()
            .map(|file| ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Part(ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::BoundedFull,
                }),
            })
            .collect();
        if !self.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(
                    serde_json::to_string(&self.skills)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                ),
            });
        }
        Ok(messages)
    }

    fn load_messages_bounded_with_io(
        &self,
        limits: crate::conversation::Limits,
        io: Option<&super::ContextPreparationIo>,
    ) -> Result<Vec<ModelMessage>> {
        super::validate_context_limits_with_io(limits, io)?;
        let count = self.instructions.len().checked_add(usize::from(!self.skills.is_empty()))
            .ok_or_else(|| super::context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "discovered context count overflows"))?;
        if count > limits.context_messages {
            return Err(super::context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "discovered context exceeds message bound"));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let skill_bytes = if let Some(native) = io.and_then(super::ContextPreparationIo::original_native_work) {
            let mut observer = crate::store::history::native_work_io(native).map_err(discovery_work_error)?;
            bound_skill_values(&self.skills, limits.render_bytes, &mut observer)?;
            Some(crate::contract::count_trusted_json_bytes_with_work(
                &self.skills, limits.render_bytes, &mut Some(&mut observer))?)
        } else {
            crate::contract::validate_json_byte_bound(&self.skills, limits.render_bytes)?;
            None
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let skill_bytes = {
            crate::contract::validate_json_byte_bound(&self.skills, limits.render_bytes)?;
            None
        };
        for file in &self.instructions {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = io.and_then(super::ContextPreparationIo::original_native_work) {
                limits.validate_file_with_native_work(file, native)?;
            } else {
                limits.validate_file(file)?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            limits.validate_file(file)?;
        }
        let messages = self.messages_with_io(io, skill_bytes)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = io.and_then(super::ContextPreparationIo::original_native_work) {
            super::validate_source_messages_native(&messages, limits, native)?;
        } else {
            super::validate_source_messages(&messages, limits)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        super::validate_source_messages(&messages, limits)?;
        Ok(messages)
    }

    /// Explicit bounded capture. Construction is pure; only calling capture performs
    /// authorized reads. Call from an admitted caller workflow for reload/live refresh.
    pub async fn capture(
        reader: &dyn ContentResidencyVerifier,
        instructions: &[InstructionScope],
        skills: &[ContextRoot],
        policy: &ContextDiscoveryPolicy,
        limits: ContextDiscoveryLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if instructions.len().saturating_add(skills.len()) > limits.directories as usize
            || policy.instruction_names.len() > limits.entries as usize
        {
            return Err(Error::Invalid(
                "discovery declarations exceed work bounds".into(),
            ));
        }
        for name in policy
            .instruction_names
            .iter()
            .chain(policy.skill_name.iter())
        {
            validate_content_path(name)?;
            if name.contains('/') {
                return Err(Error::Invalid(
                    "discovery entry must be one filename".into(),
                ));
            }
        }
        let mut work = DiscoveryWork {
            entries: 0,
            directories: 0,
            limits,
        };
        let mut result = Self::default();
        for scope in instructions {
            scope.root.validate()?;
            if !scope.active_directory.is_empty() {
                validate_content_path(&scope.active_directory)?;
            }
            if policy.instruction_names.is_empty() {
                continue;
            }
            let mut directories = vec![scope.root.directory.clone()];
            for segment in scope
                .active_directory
                .split('/')
                .filter(|segment| !segment.is_empty())
            {
                let previous = directories
                    .last()
                    .ok_or_else(|| Error::Invalid("missing scope".into()))?;
                directories.push(child(previous, segment));
            }
            let mut generation = None;
            for directory in directories {
                let (pinned, entries) = work
                    .list(reader, &scope.root, &directory, generation.as_ref())
                    .await?;
                generation = Some(pinned.clone());
                for name in &policy.instruction_names {
                    if let Some(entry) = entries.iter().find(|entry| &entry.name == name) {
                        if entry.kind != PrivateDirectoryEntryKind::File {
                            return Err(Error::Invalid("instruction entry is not a file".into()));
                        }
                        let source = PinnedContextPath {
                            root: scope.root.clone(),
                            generation: pinned.clone(),
                            path: child(&directory, name),
                        };
                        let prefix = reader
                            .read_private_prefix(
                                &scope.root.volume,
                                &scope.root.directory,
                                &source.path,
                                &pinned,
                                limits.instruction_bytes.checked_add(1).ok_or_else(|| {
                                    Error::Invalid("instruction bound overflow".into())
                                })?,
                            )
                            .await?;
                        if prefix.len() as u64 > limits.instruction_bytes {
                            return Err(Error::Invalid("instruction exceeds byte bound".into()));
                        }
                        let (file, bytes) = source.read(reader).await?;
                        if bytes.len() as u64 > limits.instruction_bytes
                            || std::str::from_utf8(&bytes).is_err()
                            || bytes.contains(&0)
                        {
                            return Err(Error::Invalid(
                                "instruction is oversized or invalid text".into(),
                            ));
                        }
                        result.instructions.push(file);
                    }
                }
            }
        }
        if let Some(name) = &policy.skill_name {
            result.skills = discover_skills(reader, skills, name, &mut work).await?;
        }
        result.validate()?;
        Ok(result)
    }

    /// Composes this immutable revision through the existing source/stage path.
    /// Its complete provenance is part of the ordinary executor binding digest.
    pub fn stage(self, placement: ContextPlacement) -> Result<SourceStage> {
        self.validate()?;
        let revision = crate::contract::canonical_json_digest(&self)?;
        let revision = blake3::Hash::from_bytes(revision).to_hex().to_string();
        Ok(SourceStage::new(
            "declared-context",
            revision,
            Arc::new(self),
            placement,
        ))
    }
}

impl ContextSource for DiscoveredContext {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: crate::conversation::Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move { self.load_messages_bounded_with_io(limits, None) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_native<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: crate::conversation::Limits,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ModelMessage>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::ContextPreparationIo::from_native_work(work)?;
            let messages = self.load_messages_bounded_with_io(limits, Some(&io))?;
            let retained = io.owned_bytes()?;
            Ok((messages, io.into_native_custody(retained)?))
        })
    }
}

async fn discover_skills(
    reader: &dyn ContentResidencyVerifier,
    skills: &[ContextRoot],
    name: &str,
    work: &mut DiscoveryWork,
) -> Result<Vec<SkillMetadata>> {
    let mut result: Vec<SkillMetadata> = Vec::new();
    for root in skills {
        root.validate()?;
        let (generation, entries) = work.list(reader, root, &root.directory, None).await?;
        for entry in entries {
            if entry.kind != PrivateDirectoryEntryKind::Directory {
                continue;
            }
            let directory = child(&root.directory, &entry.name);
            let (_, files) = work
                .list(reader, root, &directory, Some(&generation))
                .await?;
            let Some(entry) = files.iter().find(|entry| entry.name == name) else {
                continue;
            };
            if entry.kind != PrivateDirectoryEntryKind::File {
                return Err(Error::Invalid("skill entry is not a file".into()));
            }
            let source = PinnedContextPath {
                root: root.clone(),
                generation: generation.clone(),
                path: child(&directory, name),
            };
            let prefix = reader
                .read_private_prefix(
                    &root.volume,
                    &root.directory,
                    &source.path,
                    &generation,
                    u64::from(work.limits.header_bytes),
                )
                .await?;
            if prefix.len() > work.limits.header_bytes as usize {
                return Err(Error::Storage("prefix reader exceeded bound".into()));
            }
            let metadata = parse_skill_metadata(&prefix, source)?;
            if result.iter().any(|skill| skill.name == metadata.name) {
                return Err(Error::Conflict(format!(
                    "skill name {} is declared twice",
                    metadata.name
                )));
            }
            result.push(metadata);
        }
    }
    Ok(result)
}
