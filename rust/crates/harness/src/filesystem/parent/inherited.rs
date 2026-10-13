use super::*;
use crate::executor::{stock_boundary, stock_string_copy};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    ParentProjectController<'_, A, O>
{
    /// Freezes the completed exchange before staging its model prefix.
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
    /// Retains one canonical parent-owned model prefix.
    pub async fn stage_model_prefix(
        &self,
        private: &VolumeRef,
        prefix: &crate::model::ModelPrefix,
    ) -> Result<FileRef> {
        self.require("fork:publish", VolumeOperation::Read, None)?;
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
        let bytes = prefix.canonical_bytes(None)?;
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
                None,
            )
            .await
    }

    /// Projects the original parent prefix's exact reader grants.
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
            .map(|custody| PreparationIo::from_native_work(custody.work()))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<PreparationIo> = None;
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read, io.as_ref())?;
        if let Some(archive) = archive {
            crate::executor::with_history_io(io.as_ref(), |work| {
                archive.verify(&self.verifier, work)
            })?;
            if archive.parent() != parent.authority()
                || archive.parent_revision() != parent.revision()
            {
                return Err(stock_boundary(
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
            .map(|io| io.into_native_custody(grant_bytes(&grants)?))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok((grants, custody))
    }

    pub(super) fn selected_inherited_history<'p>(
        &self,
        parent: &'p Reducer,
        through_sequence: u64,
        maximum_messages: usize,
        archive: Option<&'p crate::conversation::ConversationState>,
        io: Option<&PreparationIo>,
    ) -> Result<&'p crate::conversation::ConversationState> {
        let reject = |code, reason| stock_boundary(io, code, reason);
        if parent.authority() != self.verifier.audience()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
        {
            return Err(reject(
                crate::wire::ErrorCode::Unauthorized,
                "inherited history is outside the actual parent",
            ));
        }
        let parent = parent.conversation().ok_or_else(|| {
            reject(
                crate::wire::ErrorCode::Invalid,
                "parent is not a conversation",
            )
        })?;
        let history = archive.unwrap_or(parent);
        if history.agent != parent.agent
            || through_sequence > parent.logical_revision()
            || archive.is_some()
                && (history.resident_after_sequence() != 0
                    || history.logical_revision() != through_sequence)
        {
            return Err(reject(
                crate::wire::ErrorCode::Conflict,
                "inherited archive differs from its original parent cut",
            ));
        }
        let count = usize::try_from(through_sequence).map_err(|_| {
            reject(
                crate::wire::ErrorCode::Invalid,
                "inherited prefix is too large",
            )
        })?;
        if count > maximum_messages || count > history.messages.len() {
            return Err(reject(
                crate::wire::ErrorCode::Invalid,
                "inherited prefix exceeds the selected history or limit",
            ));
        }
        Ok(history)
    }

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
        let reject = |reason| stock_boundary(io, crate::wire::ErrorCode::Invalid, reason);
        let count = usize::try_from(through_sequence)
            .map_err(|_| reject("inherited prefix is too large"))?;
        let records_hold;
        let mut records = Vec::new();
        let mut manifests = Vec::new();
        let mut manifest_bytes = 0_u64;
        for message in history.messages.iter().take(count) {
            crate::model::charge_items(io, 1)?;
            insert_reference(&mut records, &message.content, None, child_agent, io)?;
            for file in message.extensions.values() {
                insert_reference(&mut records, file, None, child_agent, io)?;
            }
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for item in items {
                        insert_reference(&mut records, &item.file, None, child_agent, io)?;
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    manifest_bytes = manifest_bytes
                        .checked_add(manifest.descriptor().byte_length())
                        .ok_or_else(|| reject("inherited manifests exceed byte limit"))?;
                    if manifest_bytes > maximum_bytes {
                        return Err(reject("inherited attachment manifest exceeds limit"));
                    }
                    let (items, _items_hold) =
                        crate::conversation::load_manifest(resolver, manifest, *item_count, io)
                            .await?;
                    if items.len() != *item_count as usize {
                        return Err(reject("inherited attachment manifest count mismatch"));
                    }
                    insert_reference(&mut records, manifest, None, child_agent, io)?;
                    let mut present = false;
                    for existing in &manifests {
                        crate::model::charge_items(io, 1)?;
                        if existing == manifest {
                            present = true;
                            break;
                        }
                    }
                    if !present {
                        let copy =
                            crate::executor::with_history_io(io, |work| manifest.copy(work))?;
                        crate::executor::stock_vec_push(&mut manifests, copy, io)?;
                    }
                    for item in &items {
                        insert_reference(
                            &mut records,
                            &item.file,
                            Some(manifest),
                            child_agent,
                            io,
                        )?;
                    }
                }
            }
            if records.len() > maximum_references {
                return Err(reject("inherited reference count exceeds limit"));
            }
        }
        records_hold = io
            .map(|io| io.guard_owned(grant_bytes(&records)?))
            .transpose()?;
        if io.is_some() {
            for record in &records {
                resolver.verify(&record.file, maximum_bytes, io)?.await?;
            }
        }
        for (index, reader) in attached_agents.iter().enumerate() {
            crate::model::charge_items(io, 1)?;
            if *reader == child_agent {
                return Err(reject("inherited readers are duplicated"));
            }
            for prior in &attached_agents[..index] {
                crate::model::charge_items(io, 1)?;
                if prior == reader {
                    return Err(reject("inherited readers are duplicated"));
                }
            }
        }
        let total = records
            .len()
            .checked_mul(
                attached_agents
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| reject("inherited read grants exceed limit"))?,
            )
            .ok_or_else(|| reject("inherited read grants exceed limit"))?;
        if total > maximum_references {
            return Err(reject("inherited read grants exceed limit"));
        }
        let mut grants = crate::model::vec_with_capacity(total, io)?;
        for reader in std::iter::once(child_agent).chain(attached_agents.iter().copied()) {
            for record in &records {
                crate::model::charge_items(io, 1)?;
                if record.file.volume().class() == VolumeClass::AgentPrivate
                    && record.file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                grants.push(crate::executor::with_history_io(io, |work| {
                    Ok(ReferenceGrant {
                        reader,
                        file: record.file.copy(work)?,
                        attachment_manifest: record
                            .attachment_manifest
                            .as_ref()
                            .map(|manifest| manifest.copy(work))
                            .transpose()?,
                    })
                })?);
            }
        }
        drop(records);
        drop(records_hold);
        Ok((grants, manifests))
    }

    /// Stages the canonical selected prefix in the child's original private volume.
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
            &Arc<crate::resource_preparation::TaskResourcePreparation>,
        >,
    ) -> Result<InheritedContextCapture> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native {
            native_creation_guard
                .ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "native inherited publication requires its original issued creation guard",
                    )
                })?
                .verify_creation_with_native_work(work)?
                .await?;
        }
        if io.is_none_or(|io| io.original_native_work().is_none())
            && let Some(preparation) = preparation
        {
            preparation.verify_creation().await?;
        }
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read, io)?;
        if parent.authority() != self.verifier.audience()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || child_private.owner() != &VolumeOwner::Agent(child_agent)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "inherited context is not parent-controlled and child-owned",
            ));
        }
        crate::executor::with_history_io(io, |work| child_private.validate(work))?;
        let history = self.selected_inherited_history(
            parent,
            through_sequence,
            maximum_messages,
            archived_history,
            io,
        )?;
        if attached_agents.len() as u64 > crate::fork::MAX_FORK_AGENTS as u64 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "too many attached agents for inherited context",
            ));
        }
        for (index, agent) in attached_agents.iter().enumerate() {
            for prior in &attached_agents[..index] {
                crate::model::charge_items(io, 1)?;
                if prior == agent {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "inherited attached agents are invalid",
                    ));
                }
            }
        }
        let attached_agents_copy = crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let copy = crate::core::copy_copy_vector(attached_agents, &mut quota)?;
            quota.transfer();
            Ok(copy)
        })?;
        let prefix = InheritedConversationPrefix {
            format_version: 3,
            parent: Authority {
                kind: self.verifier.audience().kind,
                id: stock_string_copy(&self.verifier.audience().id, io)?,
            },
            parent_revision: parent.revision(),
            parent_agent: history.agent.ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "inherited prefix parent is unbound",
                )
            })?,
            through_sequence,
            attached_agents: attached_agents_copy,
            message_digest: crate::executor::with_history_io(io, |work| {
                history.history_prefix_digest(through_sequence, work)
            })?,
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
            return Err(stock_boundary(
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
                    crate::model::charge_items(io, 1)?;
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
                    return Err(stock_boundary(
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
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "child private volume is not fresh",
                ));
            }
            fork_preparer_io::apply_generation_task_guarded(
                self.host,
                &workspace,
                Some(&initial),
                &[
                    WorkspaceMutation::CreateDirectory {
                        path: stock_string_copy("/.system/inherited-conversation", io)?,
                    },
                    WorkspaceMutation::PutFile {
                        path: crate::executor::with_history_io(io, |work| {
                            crate::contract::preparation_format(format_args!("/{path}"), work)
                        })?,
                        bytes: bytes.clone(),
                    },
                ],
                idempotency_key,
                io,
                preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            )
            .await?
        };
        let generation = fork_preparer_io::generation_ref(&self.host.provider, committed.id(), io)?;
        fork_preparer_io::retain(&committed, io).await?;
        let reference = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                child_private.copy(work)?,
                stock_string_copy(path, io)?,
                fork_preparer_io::hex_key(generation.as_resource().key(), io)?,
                descriptor,
                stock_string_copy("inherited-conversation.json", io)?,
                work,
            )
        })?;
        if self
            .host
            .read_pinned_io(&reference, maximum_bytes, io)
            .await?
            .as_ref()
            != bytes.as_ref()
        {
            return Err(stock_boundary(
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

fn grant_bytes(grants: &Vec<ReferenceGrant>) -> Result<u64> {
    let slots = (grants.capacity() as u64)
        .checked_mul(std::mem::size_of::<ReferenceGrant>() as u64)
        .ok_or_else(|| Error::Invalid("inherited grant output ownership overflows".into()))?;
    grants.iter().try_fold(slots, |bytes, grant| {
        let manifest = grant
            .attachment_manifest
            .as_ref()
            .map(FileRef::logical_owned_bytes)
            .transpose()?
            .unwrap_or(0);
        bytes
            .checked_add(grant.file.logical_owned_bytes()?)
            .and_then(|bytes| bytes.checked_add(manifest))
            .ok_or_else(|| Error::Invalid("inherited grant output ownership overflows".into()))
    })
}

fn insert_reference(
    records: &mut Vec<ReferenceGrant>,
    file: &FileRef,
    manifest: Option<&FileRef>,
    child_agent: AgentId,
    io: Option<&PreparationIo>,
) -> Result<()> {
    crate::model::charge_items(io, 1)?;
    for record in records.iter() {
        crate::model::charge_items(io, 1)?;
        if &record.file == file && record.attachment_manifest.as_ref() == manifest {
            return Ok(());
        }
    }
    let record = crate::executor::with_history_io(io, |work| {
        Ok(ReferenceGrant {
            file: file.copy(work)?,
            reader: child_agent,
            attachment_manifest: manifest.map(|manifest| manifest.copy(work)).transpose()?,
        })
    })?;
    crate::executor::stock_vec_push(records, record, io)
}
