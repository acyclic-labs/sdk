use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    ParentProjectController<'_, A, O>
{
    /// Freezes a completed exchange from the authoritative execution journal before retaining its shared segment.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact journal exchange and direct-parent binding"
    )]
    pub async fn stage_completed_model_prefix(
        &self,
        private: &VolumeRef,
        executor: &crate::executor::StockExecutor,
        journal: &dyn crate::executor::ExecutionJournal,
        input: &crate::executor::TurnInput,
        step: u32,
        call_id: &str,
        parent: Option<(&FileRef, &crate::model::PreparedModelRequest)>,
    ) -> Result<(FileRef, crate::model::PreparedModelRequest)> {
        let prepared = executor
            .completed_tool_prefix(journal, input, step, Some(call_id))
            .await?;
        let segment = crate::model::ModelPrefix::select(&prepared, parent)?;
        let reference = self.stage_model_prefix(private, &segment).await?;
        Ok((reference, prepared))
    }

    /// Retains one immutable model segment in the parent's private volume.
    pub async fn stage_model_prefix(
        &self,
        private: &VolumeRef,
        prefix: &crate::model::ModelPrefix,
    ) -> Result<FileRef> {
        self.require("fork:publish", VolumeOperation::Read)?;
        if private.provider() != &self.host.provider
            || private.class() != VolumeClass::AgentPrivate
            || private.owner()
                != &VolumeOwner::Agent(
                    self.scope.agent().ok_or_else(|| {
                        Error::Unauthorized("model prefix parent is unbound".into())
                    })?,
                )
        {
            return Err(Error::Unauthorized(
                "model prefix is not parent owned".into(),
            ));
        }
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            private,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let bytes = crate::contract::canonical_json_bytes(&prefix)?;
        let digest = blake3::hash(&bytes).to_hex();
        self.host
            .put_internal_content(
                private,
                &grant,
                &format!(".system/inherited-conversation/model-prefix/{digest}.json"),
                &bytes,
                crate::model::ModelPrefix::MEDIA_TYPE,
                "model-prefix.json",
                u64::MAX,
                &IdempotencyKey::new(format!("model-prefix:{digest}"))?,
                InternalContentClass::ModelPrefix,
            )
            .await
    }

    /// Projects inherited reader capabilities from the controller's original parent cut.
    #[allow(
        clippy::too_many_arguments,
        reason = "original history, readers and finite bounds"
    )]
    pub async fn inherited_reference_grants(
        &self,
        parent: &Reducer,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_messages: usize,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        archive: Option<&crate::fork::PreparedForkHistory>,
    ) -> Result<(Vec<ReferenceGrant>, Option<MergeAllocation>)> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = self
            .native_custody
            .as_ref()
            .map(|custody| {
                self.require_with_native_work(
                    capability::FORK_PUBLISH,
                    VolumeOperation::Read,
                    custody.work(),
                )
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<PreparationIo> = None;
        if io.is_none() {
            self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        }
        if let Some(archive) = archive {
            crate::executor::with_history_io(io.as_ref(), |work| {
                archive.verify(&self.verifier, work)
            })?;
            if archive.parent() != parent.authority()
                || archive.parent_revision() != parent.revision()
            {
                return Err(fork_preparer_io::failure(
                    io.as_ref(),
                    crate::wire::ErrorCode::Conflict,
                    "inherited archive is not the original parent cut",
                ));
            }
        }
        let history = self.selected_inherited_history(
            parent,
            through_sequence,
            maximum_messages,
            archive.map(crate::fork::PreparedForkHistory::conversation),
            io.as_ref(),
        )?;
        let (grants, manifests) = Self::collect_inherited_reference_grants(
            history,
            child_agent,
            attached_agents,
            through_sequence,
            maximum_bytes,
            maximum_references,
            resolver,
            io.as_ref(),
        )
        .await?;
        drop(manifests);
        #[cfg(not(target_arch = "wasm32"))]
        let custody = io
            .map(|io| {
                let mut bytes = (grants.capacity() as u64)
                    .checked_mul(std::mem::size_of::<ReferenceGrant>() as u64)
                    .ok_or_else(|| io.invalid("inherited grant output ownership overflows"))?;
                for grant in &grants {
                    let manifest = grant
                        .attachment_manifest
                        .as_ref()
                        .map(FileRef::logical_owned_bytes)
                        .transpose()?
                        .unwrap_or(0);
                    bytes = bytes
                        .checked_add(grant.file.logical_owned_bytes()?)
                        .and_then(|sum| sum.checked_add(manifest))
                        .ok_or_else(|| io.invalid("inherited grant output ownership overflows"))?;
                }
                io.into_native_custody(bytes)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok((grants, custody))
    }

    pub(in crate::filesystem) fn selected_inherited_history<'p>(
        &self,
        parent: &'p Reducer,
        through_sequence: u64,
        maximum_messages: usize,
        archive: Option<&'p crate::conversation::ConversationState>,
        io: Option<&PreparationIo>,
    ) -> Result<&'p crate::conversation::ConversationState> {
        let rejected = |code: crate::wire::ErrorCode, reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return Error::NativeBoundary { code, reason };
            }
            match code {
                crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
                crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
                _ => Error::Invalid(reason.into()),
            }
        };
        if parent.authority() != self.verifier.audience()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
        {
            return Err(rejected(
                crate::wire::ErrorCode::Unauthorized,
                "inherited history is outside the actual parent",
            ));
        }
        let parent_history = parent.conversation().ok_or_else(|| {
            rejected(
                crate::wire::ErrorCode::Invalid,
                "parent is not a conversation",
            )
        })?;
        let history = archive.unwrap_or(parent_history);
        if history.agent != parent_history.agent
            || through_sequence > parent_history.logical_revision()
            || (archive.is_some()
                && (history.resident_after_sequence() != 0
                    || history.logical_revision() != through_sequence))
        {
            return Err(rejected(
                crate::wire::ErrorCode::Conflict,
                "inherited archive differs from its original parent cut",
            ));
        }
        let count = usize::try_from(through_sequence).map_err(|_| {
            fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited prefix is too large",
            )
        })?;
        if count > maximum_messages || count > history.messages.len() {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited prefix exceeds the selected history or limit",
            ));
        }
        Ok(history)
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one canonical bounded reference projection"
    )]
    pub(in crate::filesystem) async fn collect_inherited_reference_grants(
        history: &crate::conversation::ConversationState,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        io: Option<&PreparationIo>,
    ) -> Result<(Vec<ReferenceGrant>, Vec<FileRef>)> {
        let count = usize::try_from(through_sequence).map_err(|_| {
            fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited prefix is too large",
            )
        })?;
        let mut direct = BTreeMap::<String, FileRef>::new();
        let mut member = BTreeMap::<(String, String), (FileRef, FileRef)>::new();
        let mut manifests = BTreeMap::<String, FileRef>::new();
        let mut manifest_bytes_total = 0_u64;
        for message in history.messages.iter().take(count) {
            fork_preparer_io::item(io, 0)?;
            fork_preparer_io::item(io, std::mem::size_of::<(String, FileRef)>() as u64)?;
            direct.insert(
                fork_preparer_io::reference_key(&message.content, io)?,
                crate::executor::with_history_io(io, |work| message.content.copy(work))?,
            );
            for reference in message.extensions.values() {
                fork_preparer_io::item(io, std::mem::size_of::<(String, FileRef)>() as u64)?;
                direct.insert(
                    fork_preparer_io::reference_key(reference, io)?,
                    crate::executor::with_history_io(io, |work| reference.copy(work))?,
                );
            }
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for item in items {
                        fork_preparer_io::item(
                            io,
                            std::mem::size_of::<(String, FileRef)>() as u64,
                        )?;
                        direct.insert(
                            fork_preparer_io::reference_key(&item.file, io)?,
                            crate::executor::with_history_io(io, |work| item.file.copy(work))?,
                        );
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    manifest_bytes_total = manifest_bytes_total
                        .checked_add(manifest.descriptor().byte_length())
                        .ok_or_else(|| {
                            fork_preparer_io::failure(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "inherited manifests exceed byte limit",
                            )
                        })?;
                    if manifest_bytes_total > maximum_bytes {
                        return Err(fork_preparer_io::failure(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "inherited attachment manifest exceeds limit",
                        ));
                    }
                    let (items, _items_hold) =
                        crate::conversation::load_manifest(resolver, manifest, *item_count, io)
                            .await?;
                    if items.len() != *item_count as usize {
                        return Err(fork_preparer_io::failure(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "inherited attachment manifest count mismatch",
                        ));
                    }
                    let key = fork_preparer_io::reference_key(manifest, io)?;
                    fork_preparer_io::item(
                        io,
                        (2 * std::mem::size_of::<(String, FileRef)>()) as u64,
                    )?;
                    direct.insert(
                        fork_preparer_io::clone_text(&key, io)?,
                        crate::executor::with_history_io(io, |work| manifest.copy(work))?,
                    );
                    manifests.insert(
                        fork_preparer_io::clone_text(&key, io)?,
                        crate::executor::with_history_io(io, |work| manifest.copy(work))?,
                    );
                    for item in items {
                        fork_preparer_io::item(
                            io,
                            std::mem::size_of::<((String, String), (FileRef, FileRef))>() as u64,
                        )?;
                        member.insert(
                            (
                                fork_preparer_io::clone_text(&key, io)?,
                                fork_preparer_io::reference_key(&item.file, io)?,
                            ),
                            (
                                crate::executor::with_history_io(io, |work| manifest.copy(work))?,
                                item.file,
                            ),
                        );
                    }
                }
            }
            if direct.len().saturating_add(member.len()) > maximum_references {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "inherited reference count exceeds limit",
                ));
            }
        }
        if let Some(io) = io {
            for file in direct.values().chain(member.values().map(|(_, file)| file)) {
                resolver.verify(file, maximum_bytes, Some(io))?.await?;
            }
            io.retain_owned(((attached_agents.len() + 1) * std::mem::size_of::<AgentId>()) as u64)?;
        }
        let mut readers = BTreeSet::from([child_agent]);
        readers.extend(attached_agents.iter().copied());
        if readers.len() != attached_agents.len().saturating_add(1) {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited readers are duplicated",
            ));
        }
        let total_grants = direct
            .len()
            .saturating_add(member.len())
            .saturating_mul(readers.len());
        if total_grants > maximum_references {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited read grants exceed limit",
            ));
        }
        let mut reference_grants = fork_preparer_io::vector(total_grants, io)?;
        for reader in readers {
            for file in direct.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: crate::executor::with_history_io(io, |work| file.copy(work))?,
                    reader,
                    attachment_manifest: None,
                });
            }
            for (manifest, file) in member.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: crate::executor::with_history_io(io, |work| file.copy(work))?,
                    reader,
                    attachment_manifest: Some(crate::executor::with_history_io(io, |work| {
                        manifest.copy(work)
                    })?),
                });
            }
        }
        let mut attachment_manifests = fork_preparer_io::vector(manifests.len(), io)?;
        attachment_manifests.extend(manifests.into_values());
        Ok((reference_grants, attachment_manifests))
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "same inherited capture with exact original summary continuation keys"
    )]
    /// Materializes the bounded inherited prefix and its original continuation.
    pub async fn materialize_inherited_conversation(
        &self,
        parent: &Reducer,
        child_private: &VolumeRef,
        initial_generation: &GenerationRef,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_messages: usize,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        idempotency_key: &IdempotencyKey,
        continuation_keys: &[IdempotencyKey],
        archived_history: Option<&crate::conversation::ConversationState>,
        io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))] native_creation_guard: Option<
            &std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>,
        >,
    ) -> Result<InheritedContextCapture> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work).is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native inherited publication requires its original issued creation guard",
            })?.verify_creation_with_native_work(work)?.await?;
        }
        if !native && let Some(preparation) = preparation {
            preparation.verify_creation().await?;
        }
        if let Some(io) = io {
            let project = io.capability(&self.project, "volume:read:")?;
            if !self.scope.capabilities().contains(&project)
                || !self.scope.capabilities().contains(capability::FORK_PUBLISH)
            {
                return Err(fork_preparer_io::failure(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "parent fork preparation is not granted",
                ));
            }
        } else {
            self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        }
        if parent.authority() != self.verifier.audience()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || child_private.owner() != &VolumeOwner::Agent(child_agent)
        {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "inherited context is not parent-controlled and child-owned",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            child_private.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            child_private.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        child_private.validate(&mut None)?;
        let history = self.selected_inherited_history(
            parent,
            through_sequence,
            maximum_messages,
            archived_history,
            io,
        )?;
        let prefix = if let Some(io) = io {
            if attached_agents.len() as u64 > crate::fork::MAX_FORK_AGENTS as u64 {
                return Err(io.invalid("too many attached agents for inherited context"));
            }
            let mut readers = BTreeSet::new();
            for reader in attached_agents {
                fork_preparer_io::item(Some(io), std::mem::size_of::<AgentId>() as u64)?;
                if !readers.insert(*reader) {
                    return Err(io.invalid("inherited attached agents are invalid"));
                }
            }
            let message_digest =
                history.history_prefix_digest(through_sequence, &mut Some(&mut io.history_io()))?;
            let inherited_parent = crate::core::Authority {
                kind: self.verifier.audience().kind,
                id: fork_preparer_io::clone_text(&self.verifier.audience().id, Some(io))?,
            };
            let copied = acyclic_fs::WorkCounters {
                bytes_copied: std::mem::size_of_val(attached_agents) as u64,
                ..Default::default()
            };
            io.preflight(copied)?;
            let mut attached = fork_preparer_io::vector(attached_agents.len(), Some(io))?;
            attached.extend_from_slice(attached_agents);
            io.charge(copied)?;
            InheritedConversationPrefix {
                format_version: 3,
                parent: inherited_parent,
                parent_revision: parent.revision(),
                parent_agent: history
                    .agent
                    .ok_or_else(|| io.invalid("inherited prefix parent is unbound"))?,
                through_sequence,
                attached_agents: attached,
                message_digest,
            }
        } else {
            InheritedConversationPrefix::select(
                self.verifier.audience().clone(),
                parent.revision(),
                through_sequence,
                attached_agents,
                history,
                &mut None,
            )?
        };
        let (reference_grants, attachment_manifests) = Self::collect_inherited_reference_grants(
            history,
            child_agent,
            attached_agents,
            through_sequence,
            maximum_bytes,
            maximum_references,
            resolver,
            io,
        )
        .await?;
        let bytes = match io {
            Some(io) => io.encode(&prefix, maximum_bytes, true)?,
            None => crate::contract::canonical_json_bytes(&prefix)?.into(),
        };
        if bytes.len() as u64 > maximum_bytes {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "inherited context exceeds byte limit",
            ));
        }
        let path = ".system/inherited-conversation/prefix.json";
        let descriptor = crate::executor::with_history_io(io, |work| {
            FileDescriptor::from_bytes(
                &bytes,
                "application/vnd.acyclic.harness.inherited-conversation+json",
                work,
            )
        })?;
        let workspace =
            fork_preparer_io::content_workspace(&self.host.provider, child_private, io)?;
        let committed = if let Some(prior) =
            fork_preparer_io::operation_generation(self.host, &workspace, idempotency_key, io)
                .await?
        {
            let original = fork_preparer_io::generation_ref(&self.host.provider, prior.id(), io)?;
            let head = fork_preparer_io::resolve(self.host, &workspace, io)
                .await?
                .generation;
            if head != original {
                let mut continued = false;
                for key in continuation_keys {
                    if let Some(generation) =
                        fork_preparer_io::operation_generation(self.host, &workspace, key, io)
                            .await?
                        && fork_preparer_io::generation_ref(
                            &self.host.provider,
                            generation.id(),
                            io,
                        )? == head
                    {
                        continued = true;
                        break;
                    }
                }
                if !continued {
                    return Err(fork_preparer_io::failure(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "child private volume changed after inherited-context staging",
                    ));
                }
            }
            prior
        } else {
            let initial = fork_preparer_io::resolve(self.host, &workspace, io)
                .await?
                .generation;
            if &initial != initial_generation {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "child private volume is not fresh",
                ));
            }
            let copied = bytes.clone();
            let mutations = [
                WorkspaceMutation::CreateDirectory {
                    path: fork_preparer_io::clone_text("/.system/inherited-conversation", io)?,
                },
                WorkspaceMutation::PutFile {
                    path: crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("/{path}"), work))?,
                    bytes: copied,
                },
            ];
            fork_preparer_io::apply_generation_task_guarded(
                self.host,
                &workspace,
                Some(&initial),
                &mutations,
                idempotency_key,
                io,
                preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            )
            .await?
        };
        let generation = fork_preparer_io::generation_ref(&self.host.provider, committed.id(), io)?;
        if let Some(io) = io {
            fork_preparer_io::retain(&committed, Some(io)).await?;
        } else {
            self.host.retain_generation(&workspace, &generation).await?;
        }
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let reference = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                child_private.copy(work)?,
                fork_preparer_io::clone_text(path, io)?,
                version,
                descriptor,
                fork_preparer_io::clone_text("inherited-conversation.json", io)?,
                work,
            )
        })?;
        let committed = self
            .host
            .read_pinned_io(&reference, maximum_bytes, io)
            .await?;
        if committed.as_ref() != bytes.as_ref() {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Conflict,
                "inherited-context retry identity belongs to different prefix",
            ));
        }
        Ok(InheritedContextCapture {
            file: reference,
            generation,
            reference_grants,
            attachment_manifests,
        })
    }
}
