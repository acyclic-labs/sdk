use super::*;

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

pub(super) struct DiscoveryWork {
    pub(super) entries: u32,
    pub(super) directories: u32,
    pub(super) limits: ContextDiscoveryLimits,
}

impl DiscoveryWork {
    pub(super) async fn list(
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

pub(super) fn child(directory: &str, name: &str) -> String {
    if directory.is_empty() {
        name.into()
    } else {
        format!("{directory}/{name}")
    }
}

impl DiscoveredContext {
    /// Checks a serialized snapshot without reading providers or granting authority.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for file in &self.instructions {
            file.validate(work)?;
        }
        let name_slots = if work.is_some() {
            self.skills
                .len()
                .checked_mul(std::mem::size_of::<&String>())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| {
                    discovery_error(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "discovered context count overflows",
                    )
                })?
        } else {
            0
        };
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(name_slots)?;
            io.retain_owned(name_slots)?;
        }
        let mut names = std::collections::BTreeSet::new();
        for skill in &self.skills {
            skill.validate(work)?;
            if !names.insert(&skill.name) {
                return Err(discovery_error(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "discovered skill names are repeated",
                ));
            }
        }
        drop(names);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(name_slots)?;
        }
        Ok(())
    }

    /// Projects this immutable snapshot into bounded model messages.
    pub(crate) fn messages(
        &self,
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Vec<ModelMessage>> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        let count = self
            .instructions
            .len()
            .checked_add(usize::from(!self.skills.is_empty()))
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "discovered context count overflows",
                )
            })?;
        if count > limits.context_messages {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "discovered context exceeds message bound",
            ));
        }
        let skill_bytes = crate::executor::with_history_io(io, |work| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(observer) = work.as_deref_mut()
                && observer.original_native_work().is_some()
            {
                bound_skill_values(&self.skills, limits.render_bytes, observer)?;
            }
            crate::contract::count_trusted_json_bytes_with_work(
                &self.skills,
                limits.render_bytes,
                work,
            )
        })?;
        crate::executor::with_history_io(io, |work| {
            for file in &self.instructions {
                limits.validate_file(file, work)?;
            }
            self.validate(work)
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let mut messages = (
                Vec::new(),
                work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(discovery_work_error)?,
            );
            let mut backing = super::reserve_context_vec_native(&mut messages.0, count, work)?;
            messages
                .1
                .absorb(&mut backing)
                .map_err(discovery_work_error)?;
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
                messages
                    .1
                    .absorb(&mut text.1)
                    .map_err(discovery_work_error)?;
                messages.0.push(ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(text.0),
                });
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
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: crate::conversation::Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(
            io,
            || async move { self.messages(limits, io) },
        )
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        true
    }
}
