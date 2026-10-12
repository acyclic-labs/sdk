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
    fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.volume.validate(work)?;
        if !self.directory.is_empty() {
            crate::conversation::validate_content_path(&self.directory, work)?;
        }
        let capability = self.volume.directory_read_capability(&self.directory, work)?;
        let retained = capability.capacity() as u64;
        drop(capability);
        if let Some(io) = work.as_deref_mut() { io.release_owned(retained)?; }
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

    /// Selects an immutable revision before model admission.
    pub async fn for_request(
        &self,
        reader: &dyn ContentResidencyVerifier,
        current: &DiscoveredContext,
        reload: ContextReloadPolicy,
    ) -> Result<DiscoveredContext> {
        match reload {
            ContextReloadPolicy::Explicit => {
                current.validate(&mut None)?;
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
    fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.root.validate(work)?;
        self.generation.validate(work)?;
        validate_content_path(&self.path, work)?;
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
        self.validate(&mut None)?;
        let (file, bytes, _hold) = reader
            .read_private_path(
                &self.root.volume,
                &self.root.directory,
                &self.path,
                Some(&self.generation),
                crate::conversation::MAX_EXACT_JS_INTEGER, None,
            )?
            .await?;
        if file.volume() != &self.root.volume || file.path() != self.path {
            return Err(Error::Storage(
                "pinned path reader returned another file".into(),
            ));
        }
        file.descriptor().verify(&bytes, &mut None)?;
        Ok((file, bytes))
    }
}

fn discovery_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return crate::Error::NativeBoundary { code, reason };
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

/// Immutable discovery revision.
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
                    None,
                )?
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
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        for file in &self.instructions {
            file.validate(work)?;
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
            skill.validate(work)?;
            if !names.insert(&skill.name) {
                return Err(discovery_error(work, crate::wire::ErrorCode::Conflict,
                    "discovered skill names are repeated"));
            }
        }
        drop(names);
        if let Some(io) = work.as_deref_mut() { io.release_owned(name_slots)?; }
        Ok(())
    }

    /// Projects this immutable snapshot into bounded model messages.
    pub(crate) fn messages(
        &self,
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ModelMessage>> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        let count = self.instructions.len().checked_add(usize::from(!self.skills.is_empty()))
            .ok_or_else(|| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "discovered context count overflows"))?;
        if count > limits.context_messages {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "discovered context exceeds message bound"));
        }
        let skill_bytes = crate::executor::with_history_io(io, |work| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(observer) = work.as_deref_mut()
                && observer.original_native_work().is_some()
            {
                bound_skill_values(&self.skills, limits.render_bytes, observer)?;
            }
            crate::contract::count_trusted_json_bytes_with_work(&self.skills, limits.render_bytes, work)
        })?;
        crate::executor::with_history_io(io, |work| {
            for file in &self.instructions { limits.validate_file(file, work)?; }
            self.validate(work)
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let mut messages = (Vec::new(), work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(discovery_work_error)?);
            let mut backing = super::reserve_context_vec_native(&mut messages.0, count, work)?;
            messages.1.absorb(&mut backing).map_err(discovery_work_error)?;
            for file in &self.instructions {
                messages.0.push(ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Part(ModelContentPart::File {
                        file: crate::model::clone_file(file, Some(io))?,
                        policy: FileProjectionPolicy::BoundedFull,
                    }),
                });
            }
            if !self.skills.is_empty() {
                let mut text = skill_text_native(&self.skills, skill_bytes, work)?;
                messages.1.absorb(&mut text.1).map_err(discovery_work_error)?;
                messages.0.push(ModelMessage { role: ModelRole::System, content: ModelContent::Text(text.0) });
            }
            io.absorb_native_custody(messages.1)?;
            super::validate_source_messages(&messages.0, limits, Some(io))?;
            return Ok(messages.0);
        }
        let _ = (io, skill_bytes);
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
        super::validate_source_messages(&messages, limits, io)?;
        Ok(messages)
    }

    /// Explicit bounded capture.
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
            validate_content_path(name, &mut None)?;
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
            scope.root.validate(&mut None)?;
            if !scope.active_directory.is_empty() {
                validate_content_path(&scope.active_directory, &mut None)?;
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
                                None,
                            )?
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
        result.validate(&mut None)?;
        Ok(result)
    }

    /// Composes this immutable revision through the existing source/stage path.
    pub fn stage(self, placement: ContextPlacement) -> Result<SourceStage> {
        self.validate(&mut None)?;
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
    fn load<'a>(&'a self, _: &'a ContextInput, limits: crate::conversation::Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move { self.messages(limits, io) })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }
}

async fn discover_skills(
    reader: &dyn ContentResidencyVerifier,
    skills: &[ContextRoot],
    name: &str,
    work: &mut DiscoveryWork,
) -> Result<Vec<SkillMetadata>> {
    let mut result: Vec<SkillMetadata> = Vec::new();
    for root in skills {
        root.validate(&mut None)?;
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
                    None,
                )?
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

mod skills;
pub use skills::{SkillMetadata, parse_skill_metadata};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use skills::{bound_skill_values, skill_text_native};
