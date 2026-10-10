//! Child-owned Summary materialization through existing Filesystem operations.
use super::{ParentProjectController, PreparationIo, WorkspaceMutation, fork_preparer_io};
use crate::{
    Error, IdempotencyKey, Result,
    context::Context,
    conversation::{ContentResidencyVerifier, FileDescriptor, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::Reducer,
    executor::ExecutionJournal,
    fork::{PreparedSummaryFork, SummaryForkCapture},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ToolResultContent},
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use std::collections::BTreeMap;

type References = BTreeMap<[u8; 32], FileRef>;

pub(super) fn summary_keys(key: &IdempotencyKey, io: Option<&PreparationIo>) -> Result<[IdempotencyKey; 2]> {
    if let Some(io) = io {
        io.claim((key.as_str().len() + ":summary-payloads".len()) as u64)?;
        io.claim((key.as_str().len() + ":summary-context".len()) as u64)?;
    }
    Ok([
        IdempotencyKey::new(format!("{}:summary-payloads", key.as_str()))?,
        IdempotencyKey::new(format!("{}:summary-context", key.as_str()))?,
    ])
}

pub(super) fn split_references(prepared: &PreparedSummaryFork, io: Option<&PreparationIo>) -> Result<(References, References)> {
    let mut payloads = BTreeMap::new();
    let mut references = BTreeMap::new();
    visit_references(prepared.context(), |file| {
        fork_preparer_io::item(io, std::mem::size_of::<([u8; 32], FileRef)>() as u64)?;
        let key = reference_digest(file, io)?;
        let file = fork_preparer_io::clone_file(file, io)?;
        if crate::conversation::is_internal_path(file.path())
            && !crate::conversation::is_inherited_context_path(file.path()) {
            if !file.path().starts_with(".system/execution/") {
                return Err(Error::Unsupported("summary fork private payload family is unavailable".into()));
            }
            payloads.insert(key, file);
        } else { references.insert(key, file); }
        Ok(())
    })?;
    Ok((payloads, references))
}

fn visit_references(context: &Context, mut visit: impl FnMut(&FileRef) -> Result<()>) -> Result<()> {
    fn file(file: &FileRef, policy: &FileProjectionPolicy, visit: &mut impl FnMut(&FileRef) -> Result<()>) -> Result<()> {
        visit(file)?;
        if let FileProjectionPolicy::Native(policy) = policy
            && let Some(binding) = &policy.configuration {
            visit(&binding.configuration.content)?;
        }
        Ok(())
    }
    for message in &context.messages {
        for part in message.content.parts() {
            match part {
                ModelContentPart::File { file: reference, policy } => file(reference, policy, &mut visit)?,
                ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => {
                    for part in parts {
                        if let ModelDataPart::File { file: reference, policy } = part {
                            file(reference, policy, &mut visit)?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for file in context.metadata.values() { visit(file)?; }
    Ok(())
}

fn reference_digest(file: &FileRef, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    let Some(io) = io else { return crate::contract::canonical_json_digest(file); };
    let encoded = io.encode_json(file, io.remaining()?.bytes_encoded, true)?;
    let work = acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
    io.preflight(work)?;
    io.charge(work)?;
    Ok(*blake3::hash(&encoded).as_bytes())
}

fn validate_context(context: &Context, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
    if let Some(io) = io {
        io.encode_json(context, limits.render_bytes, true)?;
        let mut references = 0_u64;
        visit_references(context, |_| {
            fork_preparer_io::item(Some(io), 0)?;
            references = references.checked_add(1).ok_or_else(|| Error::Invalid("summary reference count overflow".into()))?;
            Ok(())
        })?;
        // Existing projected validation uses a temporary reference Vec per
        // message. Admit its geometric logical capacity before that helper.
        io.claim(references.checked_mul((2 * std::mem::size_of::<&FileRef>()) as u64)
            .ok_or_else(|| Error::Invalid("summary validation ownership overflow".into()))?)?;
    }
    crate::context::validate_projected_context(context, limits)
}

fn rewritten_context(context: &Context, replacements: &References, io: Option<&PreparationIo>) -> Result<Context> {
    fn replace(file: &mut FileRef, replacements: &References, io: Option<&PreparationIo>) -> Result<()> {
        if let Some(copy) = replacements.get(&reference_digest(file, io)?) {
            *file = fork_preparer_io::clone_file(copy, io)?;
        }
        Ok(())
    }
    fn file(file: &mut FileRef, policy: &mut FileProjectionPolicy, replacements: &References, io: Option<&PreparationIo>) -> Result<()> {
        replace(file, replacements, io)?;
        if let FileProjectionPolicy::Native(policy) = policy
            && let Some(binding) = &mut policy.configuration {
            replace(&mut binding.configuration.content, replacements, io)?;
        }
        Ok(())
    }
    if let Some(io) = io {
        // Preserve admitted codec ownership while its tree is dropped and the
        // audited Context clone is constructed; opaque allocator nodes excluded.
        io.encode_for_clone(context, io.remaining()?.bytes_encoded)?;
        let mut body = context.messages.len().checked_mul(std::mem::size_of::<crate::model::ModelMessage>())
            .and_then(|bytes| context.metadata.len().checked_mul(std::mem::size_of::<(String, FileRef)>()).and_then(|metadata| bytes.checked_add(metadata)))
            .ok_or_else(|| Error::Invalid("summary context body ownership overflow".into()))?;
        for message in &context.messages {
            body = message.content.parts().len().checked_mul(std::mem::size_of::<ModelContentPart>()).and_then(|parts| body.checked_add(parts))
                .ok_or_else(|| Error::Invalid("summary part ownership overflow".into()))?;
            for part in message.content.parts() {
                match part {
                    ModelContentPart::File { policy: FileProjectionPolicy::Native(_), .. } => {
                        body = body.checked_add(std::mem::size_of::<crate::model::NativeMediaPolicy>())
                            .ok_or_else(|| Error::Invalid("summary native policy ownership overflow".into()))?;
                    }
                    ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => {
                        body = parts.len().checked_mul(std::mem::size_of::<ModelDataPart>()).and_then(|parts| body.checked_add(parts))
                            .ok_or_else(|| Error::Invalid("summary result ownership overflow".into()))?;
                        for part in parts {
                            if matches!(part, ModelDataPart::File { policy: FileProjectionPolicy::Native(_), .. }) {
                                body = body.checked_add(std::mem::size_of::<crate::model::NativeMediaPolicy>())
                                    .ok_or_else(|| Error::Invalid("summary result policy ownership overflow".into()))?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        io.claim(body as u64)?;
    }
    let mut context = context.clone();
    for message in &mut context.messages {
        let parts = match &mut message.content {
            ModelContent::Text(_) => continue,
            ModelContent::Part(part) => std::slice::from_mut(part),
            ModelContent::Parts(parts) => parts.as_mut_slice(),
        };
        for part in parts {
            match part {
                ModelContentPart::File { file: reference, policy } => file(reference, policy, replacements, io)?,
                ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => {
                    for part in parts {
                        if let ModelDataPart::File { file: reference, policy } = part {
                            file(reference, policy, replacements, io)?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for file in context.metadata.values_mut() { replace(file, replacements, io)?; }
    Ok(context)
}

fn copy_references(payloads: &References, child: &VolumeRef, generation: &str, io: Option<&PreparationIo>) -> Result<References> {
    let mut copies = BTreeMap::new();
    for (key, source) in payloads {
        fork_preparer_io::item(io, std::mem::size_of::<([u8; 32], FileRef)>() as u64)?;
        if let Some(io) = io {
            io.claim(64)?;
            io.claim((".system/inherited-conversation/summary-payloads/".len() + 64) as u64)?;
        }
        let path = format!(".system/inherited-conversation/summary-payloads/{}", hex::encode(key));
        copies.insert(*key, fork_preparer_io::file_ref(child, &path, generation, source.descriptor(), source.display_name(), io)?);
    }
    Ok(copies)
}

fn descriptor(bytes: &[u8], io: Option<&PreparationIo>) -> Result<FileDescriptor> {
    if let Some(io) = io {
        fork_preparer_io::strings(io, &["application/json"])?;
        let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
        io.preflight(hash)?;
        io.charge(hash)?;
    }
    FileDescriptor::from_bytes(bytes, "application/json")
}

fn clone_selection(prepared: &PreparedSummaryFork, io: Option<&PreparationIo>) -> Result<crate::fork::SummaryForkSelection> {
    if let Some(io) = io {
        io.encode_for_clone(prepared.selection(), io.remaining()?.bytes_encoded)?;
        io.claim(std::mem::size_of_val(prepared.selection()) as u64)?;
    }
    Ok(prepared.selection().clone())
}

fn into_files(references: References, io: Option<&PreparationIo>) -> Result<Vec<FileRef>> {
    if let Some(io) = io {
        let bytes = references.len().checked_mul(std::mem::size_of::<FileRef>())
            .ok_or_else(|| Error::Invalid("summary reference vector ownership overflow".into()))? as u64;
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    Ok(references.into_values().collect())
}

fn cloned_files(references: &References, io: Option<&PreparationIo>) -> Result<Vec<FileRef>> {
    if let Some(io) = io {
        let bytes = references.len().checked_mul(std::mem::size_of::<FileRef>())
            .ok_or_else(|| Error::Invalid("summary reference vector ownership overflow".into()))? as u64;
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let mut files = Vec::with_capacity(references.len());
    for file in references.values() { files.push(fork_preparer_io::clone_file(file, io)?); }
    Ok(files)
}

fn validate_capture(capture: &SummaryForkCapture, child: &VolumeRef, io: Option<&PreparationIo>) -> Result<()> {
    match io {
        Some(io) => {
            let mut borrowed = io.history_io();
            capture.validate_with_work(child, &mut Some(&mut borrowed))
        }
        None => capture.validate(child),
    }
}

async fn publish<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    controller: &ParentProjectController<'_, A, O>, workspace: &crate::resources::WorkspaceRef,
    expected: &GenerationRef, mutations: &[WorkspaceMutation], key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<GenerationRef> {
    let generation = match io {
        Some(io) => fork_preparer_io::apply_generation(controller.host, workspace, Some(expected), mutations, key, io).await?,
        None => controller.host.apply_generation(workspace, Some(expected), mutations, key).await?,
    };
    let reference = fork_preparer_io::generation_ref(controller.host, &generation, io)?;
    match io {
        Some(io) => fork_preparer_io::retain(controller.host, &generation, io).await?,
        None => {
            generation.pin(format!("harness-generation-{}", hex::encode(reference.as_resource().key())))
                .await.map_err(super::map_error)?;
        }
    }
    Ok(reference)
}

async fn original_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    controller: &ParentProjectController<'_, A, O>, workspace: &crate::resources::WorkspaceRef,
    key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<Option<GenerationRef>> {
    let Some(generation) = fork_preparer_io::operation_generation(controller.host, workspace, key, io).await? else { return Ok(None); };
    let reference = fork_preparer_io::generation_ref(controller.host, &generation, io)?;
    match io {
        Some(io) => fork_preparer_io::retain(controller.host, &generation, io).await?,
        None => controller.host.retain_generation(workspace, &reference).await?,
    }
    Ok(Some(reference))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one bounded capture with two existing idempotent publication steps"
    )]
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
    ) -> Result<(SummaryForkCapture, GenerationRef)> {
        if let Some(io) = io {
            let capability = io.capability(&self.volume, "volume:read:")?;
            if !self.scope.capabilities().contains("fork:publish") || !self.scope.capabilities().contains(&capability) {
                return Err(Error::Unauthorized("summary fork scope lacks original publication authority".into()));
            }
        } else { self.require("fork:publish", VolumeOperation::Read)?; }
        if parent.authority() != &self.parent
            || prepared.parent() != &self.parent
            || prepared.parent_revision() != parent.revision()
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || prepared.through_sequence()
                != parent
                    .conversation()
                    .and_then(|state| state.messages().last())
                    .map_or(0, |message| message.sequence)
            || journal.canonical_authority() != Some(&self.parent)
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || !matches!(child_private.owner(), VolumeOwner::Agent(_))
            || prefix.volume() != child_private
        {
            return Err(Error::Unauthorized(
                "summary fork capture is not bound to the original parent and child".into(),
            ));
        }
        let owner = parent
            .conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| Error::Invalid("summary fork parent is unbound".into()))?;
        if maximum_bytes == 0 || maximum_references == 0 {
            return Err(Error::Invalid(
                "summary fork capture allowance must be positive".into(),
            ));
        }
        let limits = prepared.selection().limits;
        validate_context(prepared.context(), limits, io)?;
        let (payloads, references) = split_references(prepared, io)?;
        if payloads
            .len()
            .checked_add(references.len())
            .is_none_or(|count| count > maximum_references)
        {
            return Err(Error::Invalid(
                "summary fork references exceed capture allowance".into(),
            ));
        }
        let mut body_bytes = prefix.descriptor().byte_length();
        for file in payloads.values() {
            if file.volume().provider() != &self.host.provider
                || file.volume().class() != VolumeClass::AgentPrivate
                || file.volume().owner() != &VolumeOwner::Agent(owner)
            {
                return Err(Error::Unauthorized(
                    "summary fork private payload is not original-parent owned".into(),
                ));
            }
            body_bytes = body_bytes
                .checked_add(file.descriptor().byte_length())
                .filter(|bytes| *bytes <= maximum_bytes)
                .ok_or_else(|| {
                    Error::Invalid("summary fork copies exceed aggregate byte allowance".into())
                })?;
        }
        // Generation keys encode to 64 hex bytes. Preflight the final context
        // before reading payloads or publishing either phase, without a cycle
        // between a context's bytes and the generation containing those bytes.
        const ZERO_VERSION: &str = "0000000000000000000000000000000000000000000000000000000000000000";
        let planned = copy_references(&payloads, child_private, ZERO_VERSION, io)?;
        let context = rewritten_context(prepared.context(), &planned, io)?;
        validate_context(&context, limits, io)?;
        let planned_bytes = match io {
            Some(io) => io.encode_json(&context, maximum_bytes.saturating_sub(body_bytes), true)?,
            None => crate::contract::canonical_json_bytes(&context)?,
        };
        if body_bytes
            .checked_add(planned_bytes.len() as u64)
            .is_none_or(|bytes| bytes > maximum_bytes)
        {
            return Err(Error::Invalid(
                "summary fork context exceeds aggregate byte allowance".into(),
            ));
        }
        let planned_capture = SummaryForkCapture {
            selection: clone_selection(prepared, io)?,
            context: fork_preparer_io::file_ref(child_private, ".system/inherited-conversation/summary-context.json",
                ZERO_VERSION, &descriptor(&planned_bytes, io)?, "summary-context.json", io)?,
            payloads: into_files(planned, io)?,
            references: cloned_files(&references, io)?,
        };
        validate_capture(&planned_capture, child_private, io)?;
        if let Some(io) = io {
            for file in references.values() {
                io.receive_content(resolver.verify_measured(file, limits.file_bytes, io.remaining()?, &io.cancellation).await)?;
            }
        }
        let keys = summary_keys(key, io)?;
        let workspace = fork_preparer_io::content_workspace(&self.host.provider, child_private, io)?;
        let payload_generation = original_generation(self, &workspace, &keys[0], io).await?;
        let context_generation = original_generation(self, &workspace, &keys[1], io).await?;
        let head = fork_preparer_io::resolve(self.host, &workspace, io).await?.generation;
        if &head != initial_generation
            && payload_generation.as_ref() != Some(&head)
            && context_generation.as_ref() != Some(&head)
        {
            return Err(Error::Conflict(
                "child private workspace changed outside summary capture".into(),
            ));
        }
        let payload_generation = if payloads.is_empty() {
            if let Some(io) = io {
                io.encode_for_clone(initial_generation, io.remaining()?.bytes_encoded)?;
                let target = fork_preparer_io::open(self.host, &workspace, io).await?;
                let generation = fork_preparer_io::generation(self.host, &target, initial_generation, io).await?;
                fork_preparer_io::retain(self.host, &generation, io).await?;
            } else { self.host.retain_generation(&workspace, initial_generation).await?; }
            initial_generation.clone()
        } else if let Some(generation) = payload_generation {
            generation
        } else {
            let capacity = payloads.len().checked_add(1)
                .ok_or_else(|| Error::Invalid("summary mutation count overflow".into()))?;
            if let Some(io) = io {
                let bytes = capacity.checked_mul(std::mem::size_of::<WorkspaceMutation>())
                    .ok_or_else(|| Error::Invalid("summary mutation ownership overflow".into()))? as u64;
                io.claim(bytes)?;
                io.allocation_attempt(bytes)?;
                fork_preparer_io::strings(io, &["/.system/inherited-conversation/summary-payloads"])?;
            }
            let mut writes = Vec::with_capacity(capacity);
            writes.push(WorkspaceMutation::CreateDirectory { path: "/.system/inherited-conversation/summary-payloads".into() });
            for (key, file) in &payloads {
                let bytes = match io {
                    Some(io) => {
                        let bytes = io.receive_content(journal.load_measured(file, io.remaining()?, &io.cancellation).await)?;
                        io.retain_owned(bytes.capacity() as u64)?;
                        let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
                        io.preflight(hash)?;
                        io.charge(hash)?;
                        bytes
                    }
                    None => journal.load(file).await?,
                };
                file.descriptor().verify(&bytes)?;
                if let Some(io) = io {
                    io.claim(64)?;
                    io.claim(64 + "/.system/inherited-conversation/summary-payloads/".len() as u64)?;
                }
                writes.push(WorkspaceMutation::PutFile {
                    path: format!(
                        "/.system/inherited-conversation/summary-payloads/{}",
                        hex::encode(key)
                    ),
                    bytes,
                });
            }
            publish(self, &workspace, initial_generation, &writes, &keys[0], io).await?
        };
        if let Some(io) = io { io.claim(64)?; }
        let copies = copy_references(
            &payloads,
            child_private,
            &hex::encode(payload_generation.as_resource().key()),
            io,
        )?;
        for file in copies.values() { self.host.read_pinned_io(file, maximum_bytes, io).await?; }
        let context = rewritten_context(prepared.context(), &copies, io)?;
        validate_context(&context, limits, io)?;
        let bytes = match io {
            Some(io) => io.encode_json(&context, maximum_bytes.saturating_sub(body_bytes), true)?,
            None => crate::contract::canonical_json_bytes(&context)?,
        };
        if bytes.len() != planned_bytes.len() {
            return Err(Error::Invalid(
                "summary fork context changed its preflight extent".into(),
            ));
        }
        let generation = if let Some(generation) = context_generation {
            generation
        } else {
            if let Some(io) = io {
                fork_preparer_io::strings(io, &["/.system/inherited-conversation/summary-context.json"])?;
                io.claim(bytes.len() as u64)?;
                io.allocation_attempt(bytes.len() as u64)?;
                io.preflight(acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() })?;
            }
            let writes = [WorkspaceMutation::PutFile {
                path: "/.system/inherited-conversation/summary-context.json".into(), bytes: bytes.clone(),
            }];
            if let Some(io) = io { io.charge(acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() })?; }
            publish(self, &workspace, &payload_generation, &writes, &keys[1], io).await?
        };
        if let Some(io) = io { io.claim(64)?; }
        let context = fork_preparer_io::file_ref(child_private,
            ".system/inherited-conversation/summary-context.json",
            &hex::encode(generation.as_resource().key()), &descriptor(&bytes, io)?,
            "summary-context.json", io)?;
        let committed = self.host.read_pinned_io(&context, maximum_bytes, io).await?;
        if committed.as_ref() != bytes {
            return Err(Error::Conflict(
                "summary fork retry changed its context".into(),
            ));
        }
        let capture = SummaryForkCapture {
            selection: clone_selection(prepared, io)?,
            context,
            payloads: into_files(copies, io)?,
            references: into_files(references, io)?,
        };
        validate_capture(&capture, child_private, io)?;
        Ok((capture, generation))
    }
}
