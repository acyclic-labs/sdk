//! Declared, bounded filesystem discovery feeding the ordinary context pipeline.

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
        self.volume.validate()?;
        if !self.directory.is_empty() {
            validate_content_path(&self.directory)?;
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
            || self.header_bytes > 65_536
            || self.instruction_bytes > crate::model::MAX_MODEL_REQUEST_BYTES
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
        self.root.validate()?;
        self.generation.validate()?;
        validate_content_path(&self.path)?;
        if self.generation.as_resource().provider() != self.root.volume.provider()
            || (!self.root.directory.is_empty()
                && !self
                    .path
                    .strip_prefix(&self.root.directory)
                    .is_some_and(|tail| tail.starts_with('/')))
            || crate::conversation::is_internal_path(&self.path)
        {
            return Err(Error::Invalid(
                "pinned context path is outside its declared root".into(),
            ));
        }
        Ok(())
    }

    /// Loads a body through the ordinary authorized pinned read, checking provider output.
    pub async fn read(&self, reader: &dyn ContentResidencyVerifier) -> Result<(FileRef, Vec<u8>)> {
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
        self.source.validate()?;
        if self.name.is_empty()
            || self.name.len() > 64
            || self.name.starts_with('-')
            || self.name.ends_with('-')
            || self.name.contains("--")
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || self.description.trim().is_empty()
            || self.description.len() > 1_024
            || self
                .description
                .chars()
                .any(|ch| ch.is_control() && !ch.is_whitespace())
            || self.fields.contains_key("name")
            || self.fields.contains_key("description")
            || crate::contract::canonical_json_bytes(&self.fields)?.len() > 65_536
        {
            return Err(Error::Invalid("skill metadata is invalid".into()));
        }
        Ok(())
    }
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
    if prefix.len() > 65_536 {
        return Err(Error::Invalid("skill prefix exceeds protocol bound".into()));
    }
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
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: 2_048, max_nodes: 1_024, max_depth: 16,
            flow_nesting_limit: 16, max_documents: 1,
            max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
            max_total_scalar_bytes: 65_536,
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
        if crate::contract::canonical_json_bytes(self)?.len() as u64
            > crate::model::MAX_MODEL_REQUEST_BYTES
        {
            return Err(Error::Invalid(
                "discovered context exceeds protocol bound".into(),
            ));
        }
        for file in &self.instructions {
            file.validate()?;
        }
        let mut names = std::collections::BTreeSet::new();
        for skill in &self.skills {
            skill.validate()?;
            if !names.insert(&skill.name) {
                return Err(Error::Conflict(
                    "discovered skill names are repeated".into(),
                ));
            }
        }
        Ok(())
    }

    /// Projects this immutable snapshot into ordinary typed model messages.
    /// Body resolution remains the provider's bounded full/reference contract.
    pub fn messages(&self) -> Result<Vec<ModelMessage>> {
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
    fn load<'a>(&'a self, _: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move { self.messages() })
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
