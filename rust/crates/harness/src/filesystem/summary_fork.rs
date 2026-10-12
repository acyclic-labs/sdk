//! Child-owned Summary materialization through existing Filesystem operations.
use super::{ParentProjectController, PreparationIo, WorkspaceMutation, fork_preparer_io};
use crate::{
    Error, IdempotencyKey, Result,
    context::Context,
    conversation::{
        ContentResidencyVerifier, FileDescriptor, FileRef, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::Reducer,
    executor::ExecutionJournal,
    fork::{PreparedSummaryFork, SummaryForkCapture},
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ToolResultContent,
    },
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use std::collections::BTreeMap;

type References = BTreeMap<[u8; 32], FileRef>;

pub(super) fn summary_keys(
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<[IdempotencyKey; 2]> {
    Ok([
        IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("{}:summary-payloads", key.as_str()),
            io,
        )?)?,
        IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("{}:summary-context", key.as_str()),
            io,
        )?)?,
    ])
}

pub(super) fn split_references<'a>(
    prepared: &'a PreparedSummaryFork,
    io: Option<&PreparationIo>,
) -> Result<(
    BTreeMap<[u8; 32], &'a FileRef>,
    BTreeMap<[u8; 32], &'a FileRef>,
)> {
    let mut payloads = BTreeMap::new();
    let mut references = BTreeMap::new();
    for file in prepared
        .context()
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(prepared.context().metadata.values())
    {
        let key = crate::executor::with_history_io(io, |work| {
            crate::contract::canonical_json_digest_with_work(file, u64::MAX, work)
        })?;
        let selected = if crate::conversation::is_internal_path(file.path())
            && !crate::conversation::is_inherited_context_path(file.path())
        {
            if !file.path().starts_with(".system/execution/") {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "summary fork private payload family is unavailable",
                ));
            }
            &mut payloads
        } else {
            &mut references
        };
        match selected.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                fork_preparer_io::item(io, std::mem::size_of::<([u8; 32], &FileRef)>() as u64)?;
                entry.insert(file);
            }
            std::collections::btree_map::Entry::Occupied(_) => fork_preparer_io::item(io, 0)?,
        }
    }
    Ok((payloads, references))
}

fn rewritten_context(
    context: &Context,
    replacements: &References,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    fn replace(
        file: &mut FileRef,
        replacements: &References,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(copy) = replacements.get(&crate::executor::with_history_io(io, |work| {
            crate::contract::canonical_json_digest_with_work(file, u64::MAX, work)
        })?) {
            *file = crate::executor::with_history_io(io, |work| copy.copy(work))?;
        }
        Ok(())
    }
    fn file(
        file: &mut FileRef,
        policy: &mut FileProjectionPolicy,
        replacements: &References,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        replace(file, replacements, io)?;
        if let FileProjectionPolicy::Native(policy) = policy
            && let Some(binding) = &mut policy.configuration
        {
            replace(&mut binding.configuration.content, replacements, io)?;
        }
        Ok(())
    }
    let mut context = crate::context::copy_context(context, io)?;
    for message in &mut context.messages {
        let parts = match &mut message.content {
            ModelContent::Text(_) => continue,
            ModelContent::Part(part) => std::slice::from_mut(part),
            ModelContent::Parts(parts) => parts.as_mut_slice(),
        };
        for part in parts {
            match part {
                ModelContentPart::File {
                    file: reference,
                    policy,
                } => file(reference, policy, replacements, io)?,
                ModelContentPart::ToolResult {
                    content: ToolResultContent::Parts { parts },
                    ..
                } => {
                    for part in parts {
                        if let ModelDataPart::File {
                            file: reference,
                            policy,
                        } = part
                        {
                            file(reference, policy, replacements, io)?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for file in context.metadata.values_mut() {
        replace(file, replacements, io)?;
    }
    Ok(context)
}

fn copy_references(
    payloads: &BTreeMap<[u8; 32], &FileRef>,
    child: &VolumeRef,
    generation: &str,
    io: Option<&PreparationIo>,
) -> Result<References> {
    let mut copies = BTreeMap::new();
    for (key, source) in payloads {
        fork_preparer_io::item(io, std::mem::size_of::<([u8; 32], FileRef)>() as u64)?;
        let encoded = fork_preparer_io::hex_key(key, io)?;
        let path = crate::contract::preparation_format(
            format_args!(".system/inherited-conversation/summary-payloads/{encoded}"),
            io,
        )?;
        copies.insert(
            *key,
            crate::executor::with_history_io(io, |work| {
                FileRef::new(
                    child.copy(work)?,
                    path,
                    fork_preparer_io::clone_text(generation, io)?,
                    FileDescriptor::new(
                        *source.descriptor().sha256(),
                        source.descriptor().byte_length(),
                        fork_preparer_io::clone_text(source.descriptor().media_type(), io)?,
                    )?,
                    fork_preparer_io::clone_text(source.display_name(), io)?,
                    work,
                )
            })?,
        );
    }
    Ok(copies)
}

pub(super) fn clone_selection(
    selection: &crate::fork::SummaryForkSelection,
    io: Option<&PreparationIo>,
) -> Result<crate::fork::SummaryForkSelection> {
    Ok(crate::fork::SummaryForkSelection {
        checkpoint: crate::executor::with_history_io(io, |work| selection.checkpoint.copy(work))?,
        limits: selection.limits,
        history_limits: selection.history_limits,
    })
}

fn into_files(references: References, io: Option<&PreparationIo>) -> Result<Vec<FileRef>> {
    let mut files = fork_preparer_io::vector(references.len(), io)?;
    files.extend(references.into_values());
    Ok(files)
}

fn cloned_files(
    references: &BTreeMap<[u8; 32], &FileRef>,
    io: Option<&PreparationIo>,
) -> Result<Vec<FileRef>> {
    let mut files = fork_preparer_io::vector(references.len(), io)?;
    for file in references.values() {
        files.push(crate::executor::with_history_io(io, |work| {
            file.copy(work)
        })?);
    }
    Ok(files)
}

async fn publish<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    controller: &ParentProjectController<'_, A, O>,
    workspace: &crate::resources::WorkspaceRef,
    expected: &GenerationRef,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))] native_creation_guard: Option<
        &std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>,
    >,
) -> Result<GenerationRef> {
    let generation = fork_preparer_io::apply_generation_task_guarded(
        controller.host,
        workspace,
        Some(expected),
        mutations,
        key,
        io,
        preparation,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard,
    )
    .await?;
    fork_preparer_io::retain(&generation, io).await?;
    let reference =
        fork_preparer_io::generation_ref(&controller.host.provider, generation.id(), io)?;
    Ok(reference)
}

async fn original_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    controller: &ParentProjectController<'_, A, O>,
    workspace: &crate::resources::WorkspaceRef,
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<Option<GenerationRef>> {
    let Some(generation) =
        fork_preparer_io::operation_generation(controller.host, workspace, key, io).await?
    else {
        return Ok(None);
    };
    fork_preparer_io::retain(&generation, io).await?;
    let reference =
        fork_preparer_io::generation_ref(&controller.host.provider, generation.id(), io)?;
    Ok(Some(reference))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    pub(super) async fn materialize_summary_context(
        &self,
        parent: &Reducer,
        prepared: &PreparedSummaryFork,
        journal: &dyn ExecutionJournal,
        child_private: &VolumeRef,
        initial_generation: &GenerationRef,
        prefix: &FileRef,
        maximum_bytes: u64,
        maximum_references: usize,
        key: &IdempotencyKey,
        resolver: &dyn ContentResidencyVerifier,
        io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))] native_creation_guard: Option<
            &std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>,
        >,
    ) -> Result<(SummaryForkCapture, GenerationRef)> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        match native {
            Some(work) => native_creation_guard.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native summary publication requires its original issued creation guard",
            })?.verify_creation_with_native_work(work)?.await?,
            None => if let Some(preparation) = preparation { preparation.verify_creation().await?; },
        }
        #[cfg(target_arch = "wasm32")]
        if let Some(preparation) = preparation {
            preparation.verify_creation().await?;
        }
        if let Some(io) = io {
            let capability = io.capability(&self.project, "volume:read:")?;
            if !self.scope.capabilities().contains("fork:publish")
                || !self.scope.capabilities().contains(&capability)
            {
                return Err(fork_preparer_io::failure(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "summary fork scope lacks original publication authority",
                ));
            }
        } else {
            self.require("fork:publish", VolumeOperation::Read)?;
        }
        if parent.authority() != self.verifier.audience()
            || prepared.parent() != self.verifier.audience()
            || prepared.parent_revision() != parent.revision()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || prepared.through_sequence()
                != parent
                    .conversation()
                    .and_then(|state| state.messages().last())
                    .map_or(0, |message| message.sequence)
            || journal.canonical_authority() != Some(self.verifier.audience())
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || !matches!(child_private.owner(), VolumeOwner::Agent(_))
            || prefix.volume() != child_private
        {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "summary fork capture is not bound to the original parent and child",
            ));
        }
        let owner = parent
            .conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| {
                fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "summary fork parent is unbound",
                )
            })?;
        if maximum_bytes == 0 || maximum_references == 0 {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "summary fork capture allowance must be positive",
            ));
        }
        let limits = prepared.selection().limits;
        crate::context::validate_projected_context(prepared.context(), limits, io)?;
        let (payloads, references) = split_references(prepared, io)?;
        if payloads
            .len()
            .checked_add(references.len())
            .is_none_or(|count| count > maximum_references)
        {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "summary fork references exceed capture allowance",
            ));
        }
        let mut body_bytes = prefix.descriptor().byte_length();
        for file in payloads.values() {
            if file.volume().provider() != &self.host.provider
                || file.volume().class() != VolumeClass::AgentPrivate
                || file.volume().owner() != &VolumeOwner::Agent(owner)
            {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "summary fork private payload is not original-parent owned",
                ));
            }
            body_bytes = body_bytes
                .checked_add(file.descriptor().byte_length())
                .filter(|bytes| *bytes <= maximum_bytes)
                .ok_or_else(|| {
                    fork_preparer_io::failure(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "summary fork copies exceed aggregate byte allowance",
                    )
                })?;
        }
        const ZERO_VERSION: &str =
            "0000000000000000000000000000000000000000000000000000000000000000";
        let planned_extent = {
            #[cfg(not(target_arch = "wasm32"))]
            let temporary = io
                .and_then(PreparationIo::original_native_work)
                .map(PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let io = temporary.as_ref().or(io);
            let planned = copy_references(&payloads, child_private, ZERO_VERSION, io)?;
            let context = rewritten_context(prepared.context(), &planned, io)?;
            crate::context::validate_projected_context(&context, limits, io)?;
            let planned_bytes = match io {
                Some(io) => io.encode(&context, maximum_bytes.saturating_sub(body_bytes), true)?,
                None => crate::contract::canonical_json_bytes(&context)?.into(),
            };
            if body_bytes
                .checked_add(planned_bytes.len() as u64)
                .is_none_or(|bytes| bytes > maximum_bytes)
            {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "summary fork context exceeds aggregate byte allowance",
                ));
            }
            let planned_capture = SummaryForkCapture {
                selection: clone_selection(prepared.selection(), io)?,
                context: crate::executor::with_history_io(io, |work| {
                    FileRef::new(
                        child_private.copy(work)?,
                        fork_preparer_io::clone_text(
                            ".system/inherited-conversation/summary-context.json",
                            io,
                        )?,
                        fork_preparer_io::clone_text(ZERO_VERSION, io)?,
                        FileDescriptor::from_bytes(&planned_bytes, "application/json", work)?,
                        fork_preparer_io::clone_text("summary-context.json", io)?,
                        work,
                    )
                })?,
                payloads: into_files(planned, io)?,
                references: cloned_files(&references, io)?,
            };
            crate::executor::with_history_io(io, |work| {
                planned_capture.validate_with_work(child_private, work)
            })?;
            planned_bytes.len()
        };
        if let Some(io) = io {
            for file in references.values() {
                resolver.verify(file, limits.file_bytes, Some(io))?.await?;
            }
        }
        let keys = summary_keys(key, io)?;
        let workspace =
            fork_preparer_io::content_workspace(&self.host.provider, child_private, io)?;
        let payload_generation = original_generation(self, &workspace, &keys[0], io).await?;
        let context_generation = original_generation(self, &workspace, &keys[1], io).await?;
        let head = fork_preparer_io::resolve(self.host, &workspace, io)
            .await?
            .generation;
        if &head != initial_generation
            && payload_generation.as_ref() != Some(&head)
            && context_generation.as_ref() != Some(&head)
        {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Conflict,
                "child private workspace changed outside summary capture",
            ));
        }
        let payload_generation = if payloads.is_empty() {
            if let Some(io) = io {
                let target = fork_preparer_io::open(self.host, &workspace, io).await?;
                let generation =
                    fork_preparer_io::generation(self.host, &target, initial_generation, io)
                        .await?;
                fork_preparer_io::retain(&generation, Some(io)).await?;
                initial_generation.clone_with_io(io)?
            } else {
                self.host
                    .retain_generation(&workspace, initial_generation)
                    .await?;
                initial_generation.clone()
            }
        } else if let Some(generation) = payload_generation {
            generation
        } else {
            let capacity = payloads.len().checked_add(1).ok_or_else(|| {
                fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "summary mutation count overflow",
                )
            })?;
            let mut writes = fork_preparer_io::vector(capacity, io)?;
            writes.push(WorkspaceMutation::CreateDirectory {
                path: fork_preparer_io::clone_text(
                    "/.system/inherited-conversation/summary-payloads",
                    io,
                )?,
            });
            for (key, file) in &payloads {
                let bytes = match io {
                    #[cfg(not(target_arch = "wasm32"))]
                    Some(io) if io.original_native_work().is_some() => {
                        journal
                            .load_with_native_work(
                                file,
                                io.original_native_work().expect("original summary work"),
                            )?
                            .await?
                    }
                    Some(io) => {
                        let bytes = io.receive_content(
                            journal
                                .load_measured(file, io.remaining()?, io.cancellation())
                                .await,
                        )?;
                        io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
                        bytes
                    }
                    None => journal.load(file).await?,
                };
                crate::executor::with_history_io(io, |work| {
                    file.descriptor().verify(&bytes, work)
                })?;
                let encoded = fork_preparer_io::hex_key(key, io)?;
                writes.push(WorkspaceMutation::PutFile {
                    path: crate::contract::preparation_format(
                        format_args!("/.system/inherited-conversation/summary-payloads/{encoded}"),
                        io,
                    )?,
                    bytes,
                });
            }
            publish(
                self,
                &workspace,
                initial_generation,
                &writes,
                &keys[0],
                io,
                preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            )
            .await?
        };
        let payload_version =
            fork_preparer_io::hex_key(payload_generation.as_resource().key(), io)?;
        let copies = copy_references(&payloads, child_private, &payload_version, io)?;
        for file in copies.values() {
            self.host.read_pinned_io(file, maximum_bytes, io).await?;
        }
        let context = rewritten_context(prepared.context(), &copies, io)?;
        crate::context::validate_projected_context(&context, limits, io)?;
        let bytes = match io {
            Some(io) => io.encode(&context, maximum_bytes.saturating_sub(body_bytes), true)?,
            None => crate::contract::canonical_json_bytes(&context)?.into(),
        };
        if bytes.len() != planned_extent {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "summary fork context changed its preflight extent",
            ));
        }
        let generation = match context_generation {
            Some(generation) => generation,
            None => {
                publish(
                    self,
                    &workspace,
                    &payload_generation,
                    &[WorkspaceMutation::PutFile {
                        path: fork_preparer_io::clone_text(
                            "/.system/inherited-conversation/summary-context.json",
                            io,
                        )?,
                        bytes: bytes.clone(),
                    }],
                    &keys[1],
                    io,
                    preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_creation_guard,
                )
                .await?
            }
        };
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let context = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                child_private.copy(work)?,
                fork_preparer_io::clone_text(
                    ".system/inherited-conversation/summary-context.json",
                    io,
                )?,
                version,
                FileDescriptor::from_bytes(&bytes, "application/json", work)?,
                fork_preparer_io::clone_text("summary-context.json", io)?,
                work,
            )
        })?;
        let committed = self
            .host
            .read_pinned_io(&context, maximum_bytes, io)
            .await?;
        if committed.as_ref() != bytes.as_ref() {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Conflict,
                "summary fork retry changed its context",
            ));
        }
        let capture = SummaryForkCapture {
            selection: clone_selection(prepared.selection(), io)?,
            context,
            payloads: into_files(copies, io)?,
            references: cloned_files(&references, io)?,
        };
        crate::executor::with_history_io(io, |work| {
            capture.validate_with_work(child_private, work)
        })?;
        Ok((capture, generation))
    }
}
