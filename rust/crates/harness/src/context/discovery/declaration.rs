use super::*;

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
    pub(super) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate(work)?;
        if !self.directory.is_empty() {
            crate::conversation::validate_content_path(&self.directory, work)?;
        }
        let capability = self
            .volume
            .directory_read_capability(&self.directory, work)?;
        let retained = capability.capacity() as u64;
        drop(capability);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(retained)?;
        }
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
    /// Captures the declared revision under its directory, entry and byte limits.
    pub async fn capture(
        &self,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<DiscoveredContext> {
        let (instructions, skills, policy, limits) = (
            self.instructions.as_slice(),
            self.skills.as_slice(),
            &self.policy,
            self.limits,
        );
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
        let mut result = DiscoveredContext::default();
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
    pub(super) fn validate(self) -> Result<()> {
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
    pub(super) fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
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
            return Err(discovery_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "pinned context path is outside its declared root",
            ));
        }
        Ok(())
    }

    /// Loads a body through the ordinary authorized pinned read, checking provider output.
    pub async fn read(
        &self,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.validate(&mut None)?;
        let (file, bytes, _hold) = reader
            .read_private_path(
                &self.root.volume,
                &self.root.directory,
                &self.path,
                Some(&self.generation),
                crate::conversation::MAX_EXACT_JS_INTEGER,
                None,
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
