//! Child-owned Summary materialization through existing Filesystem operations.
use super::{ParentProjectController, WorkspaceMutation, filesystem_key, map_error, workspace_ref};
use crate::{
    Error, IdempotencyKey, Result,
    context::Context,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::Reducer,
    executor::ExecutionJournal,
    fork::{PreparedSummaryFork, SummaryForkCapture},
    model::{ModelContent, ModelContentPart},
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use std::collections::BTreeMap;

type References = BTreeMap<[u8; 32], FileRef>;

pub(super) fn summary_keys(key: &IdempotencyKey) -> Result<[IdempotencyKey; 2]> {
    Ok([
        IdempotencyKey::new(format!("{}:summary-payloads", key.as_str()))?,
        IdempotencyKey::new(format!("{}:summary-context", key.as_str()))?,
    ])
}

pub(super) fn split_references(prepared: &PreparedSummaryFork) -> Result<(References, References)> {
    let context = prepared.context();
    let mut payloads = BTreeMap::new();
    let mut references = BTreeMap::new();
    for file in context
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .chain(context.metadata.values())
    {
        let key = crate::contract::canonical_json_digest(file)?;
        if crate::conversation::is_internal_path(file.path())
            && !crate::conversation::is_inherited_context_path(file.path())
        {
            if !file.path().starts_with(".system/execution/") {
                return Err(Error::Unsupported(
                    "summary fork private payload family is unavailable".into(),
                ));
            }
            payloads.insert(key, file.clone());
        } else {
            references.insert(key, file.clone());
        }
    }
    Ok((payloads, references))
}

fn rewritten_context(context: &Context, replacements: &References) -> Result<Context> {
    fn replace(file: &mut FileRef, replacements: &References) -> Result<()> {
        if let Some(copy) = replacements.get(&crate::contract::canonical_json_digest(file)?) {
            *file = copy.clone();
        }
        Ok(())
    }
    let mut context = context.clone();
    for message in &mut context.messages {
        let parts = match &mut message.content {
            ModelContent::Text(_) => continue,
            ModelContent::Part(part) => std::slice::from_mut(part),
            ModelContent::Parts(parts) => parts.as_mut_slice(),
        };
        for part in parts {
            if let ModelContentPart::File { file, .. } = part {
                replace(file, replacements)?;
            }
        }
    }
    for file in context.metadata.values_mut() {
        replace(file, replacements)?;
    }
    Ok(context)
}

fn copy_references(
    payloads: &References,
    child: &VolumeRef,
    generation: &str,
) -> Result<References> {
    payloads
        .iter()
        .map(|(key, source)| {
            Ok((
                *key,
                FileRef::new(
                    child.clone(),
                    format!(
                        ".system/inherited-conversation/summary-payloads/{}",
                        hex::encode(key)
                    ),
                    generation,
                    source.descriptor().clone(),
                    source.display_name(),
                )?,
            ))
        })
        .collect()
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
    ) -> Result<(SummaryForkCapture, GenerationRef)> {
        self.require("fork:publish", VolumeOperation::Read)?;
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
        crate::context::validate_projected_context(prepared.context(), limits)?;
        let (payloads, references) = split_references(prepared)?;
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
        let planned = copy_references(&payloads, child_private, &"0".repeat(64))?;
        let context = rewritten_context(prepared.context(), &planned)?;
        crate::context::validate_projected_context(&context, limits)?;
        let planned_bytes = crate::contract::canonical_json_bytes(&context)?;
        if body_bytes
            .checked_add(planned_bytes.len() as u64)
            .is_none_or(|bytes| bytes > maximum_bytes)
        {
            return Err(Error::Invalid(
                "summary fork context exceeds aggregate byte allowance".into(),
            ));
        }
        let planned_capture = SummaryForkCapture {
            selection: prepared.selection().clone(),
            context: FileRef::new(
                child_private.clone(),
                ".system/inherited-conversation/summary-context.json",
                "0".repeat(64),
                FileDescriptor::from_bytes(&planned_bytes, "application/json")?,
                "summary-context.json",
            )?,
            payloads: planned.into_values().collect(),
            references: references.values().cloned().collect(),
        };
        planned_capture.validate(child_private)?;
        let keys = summary_keys(key)?;
        let workspace = workspace_ref(self.host.provider.clone(), &child_private.storage_name()?)?;
        let target = self.host.open(&workspace).await?;
        let payload_generation = target
            .operation_generation(filesystem_key(&keys[0]))
            .await
            .map_err(map_error)?
            .map(|generation| self.host.generation_ref(&generation))
            .transpose()?;
        let context_generation = target
            .operation_generation(filesystem_key(&keys[1]))
            .await
            .map_err(map_error)?
            .map(|generation| self.host.generation_ref(&generation))
            .transpose()?;
        let head = self.host.resolve(&workspace).await?.generation;
        if &head != initial_generation
            && payload_generation.as_ref() != Some(&head)
            && context_generation.as_ref() != Some(&head)
        {
            return Err(Error::Conflict(
                "child private workspace changed outside summary capture".into(),
            ));
        }
        let payload_generation = if payloads.is_empty() {
            initial_generation.clone()
        } else if let Some(generation) = payload_generation {
            generation
        } else {
            let mut writes = vec![WorkspaceMutation::CreateDirectory {
                path: "/.system/inherited-conversation/summary-payloads".into(),
            }];
            for (key, file) in &payloads {
                let bytes = journal.load(file).await?;
                file.descriptor().verify(&bytes)?;
                writes.push(WorkspaceMutation::PutFile {
                    path: format!(
                        "/.system/inherited-conversation/summary-payloads/{}",
                        hex::encode(key)
                    ),
                    bytes,
                });
            }
            self.host
                .apply(&workspace, Some(initial_generation), &writes, &keys[0])
                .await?
        };
        self.host
            .retain_generation(&workspace, &payload_generation)
            .await?;
        let copies = copy_references(
            &payloads,
            child_private,
            &hex::encode(payload_generation.as_resource().key()),
        )?;
        for file in copies.values() {
            let bytes = self.host.read_pinned(file, maximum_bytes).await?;
            file.descriptor().verify(&bytes)?;
        }
        let context = rewritten_context(prepared.context(), &copies)?;
        crate::context::validate_projected_context(&context, limits)?;
        let bytes = crate::contract::canonical_json_bytes(&context)?;
        if bytes.len() != planned_bytes.len() {
            return Err(Error::Invalid(
                "summary fork context changed its preflight extent".into(),
            ));
        }
        let generation = if let Some(generation) = context_generation {
            generation
        } else {
            self.host
                .apply(
                    &workspace,
                    Some(&payload_generation),
                    &[WorkspaceMutation::PutFile {
                        path: "/.system/inherited-conversation/summary-context.json".into(),
                        bytes: bytes.clone(),
                    }],
                    &keys[1],
                )
                .await?
        };
        self.host.retain_generation(&workspace, &generation).await?;
        let context = FileRef::new(
            child_private.clone(),
            ".system/inherited-conversation/summary-context.json",
            hex::encode(generation.as_resource().key()),
            FileDescriptor::from_bytes(&bytes, "application/json")?,
            "summary-context.json",
        )?;
        let committed = self.host.read_pinned(&context, maximum_bytes).await?;
        if committed.as_ref() != bytes {
            return Err(Error::Conflict(
                "summary fork retry changed its context".into(),
            ));
        }
        let capture = SummaryForkCapture {
            selection: prepared.selection().clone(),
            context,
            payloads: copies.into_values().collect(),
            references: references.into_values().collect(),
        };
        capture.validate(child_private)?;
        Ok((capture, generation))
    }
}
