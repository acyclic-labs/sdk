//! Inbox selections frozen by the executor's existing ContextPrepared record.
//! ModelStarted is admission evidence; selection alone consumes none.

use super::{
    FilesystemContentVerifier, FilesystemHost,
    agent_internal::AgentInternalFiles,
    agent_turns::{AdmittedAgent, NoticeBlocks, TurnAdmission},
};
use crate::{
    Error, OperationId, Result,
    agent::{AgentState, BoundaryCut, ModelBoundary, SteeringTranscript, TurnOutputs},
    conversation::{ContentAccess, ContentPublisher, ContentResidencyVerifier},
    core::RuntimeAuthority,
    model::{ModelContent, ModelContentPart, ModelMessage, ModelRole},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use std::sync::{Arc, Mutex};

struct Selected {
    step: u32,
    cut: BoundaryCut,
    messages: Vec<ModelMessage>,
}

#[derive(Default)]
struct Admitted {
    prepared_sources: Option<u64>,
    through: Option<u64>,
    step: Option<u32>,
    selected: Option<Selected>,
    messages: Vec<ModelMessage>,
}

pub(super) struct FilesystemBoundary<P: ?Sized, A: 'static, O: 'static> {
    pub stream: Arc<P>,
    pub host: Arc<FilesystemHost<A, O>>,
    pub issuer: RuntimeAuthority,
    pub admission: TurnAdmission,
    pub agent: AgentState,
    operation: OperationId,
    pub inbox: u64,
    pub files: Arc<AgentInternalFiles<A, O>>,
    pub writer: Arc<dyn ContentPublisher>,
    maximum: Option<u64>,
    admitted: Mutex<Admitted>,
}

fn identity(operation: OperationId, step: u32, item: u64) -> OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.steering.v1");
    hash.update(&operation.into_bytes());
    hash.update(&step.to_le_bytes());
    hash.update(&item.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

impl<P: ?Sized, A: 'static, O: 'static> FilesystemBoundary<P, A, O> {
    pub(super) fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
        agent: AgentState,
        operation: OperationId,
        inbox: u64,
        files: Arc<AgentInternalFiles<A, O>>,
        writer: Arc<dyn ContentPublisher>,
        maximum: Option<u64>,
    ) -> Self {
        Self {
            stream,
            host,
            issuer,
            admission,
            agent,
            operation,
            inbox,
            files,
            writer,
            maximum,
            admitted: Mutex::default(),
        }
    }

    fn require_operation(&self, operation: OperationId) -> Result<()> {
        if operation != self.operation {
            return Err(Error::Invalid(
                "steering boundary belongs to another turn".into(),
            ));
        }
        Ok(())
    }
}

impl<P, A, O> FilesystemBoundary<P, A, O>
where
    P: StreamProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn select(&self, step: u32, frozen: Option<BoundaryCut>) -> Result<Selected> {
        let from = if let Some(cut) = frozen {
            cut.from
        } else {
            let admitted = self
                .admitted
                .lock()
                .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
            admitted
                .through
                .unwrap_or_else(|| self.inbox.saturating_add(1).max(self.agent.steered_through))
        };
        // Preparation may publish source deltas behind other arrivals. Include
        // their ordered prefix, but do not replace the turn's original frozen
        // notice frontier with unrelated arrivals before the first dispatch.
        // Replay always uses ContextPrepared's exact original cut.
        let until = match frozen {
            Some(cut) => crate::agent::BoundaryEnd::Exact(cut.through),
            None if step == 0 => crate::agent::BoundaryEnd::Fence(
                self.admitted
                    .lock()
                    .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?
                    .prepared_sources
                    .unwrap_or(from)
                    .max(from),
            ),
            None => crate::agent::BoundaryEnd::Open,
        };
        let selection = crate::agent::select_boundary(&*self.stream, &self.agent.path, from, until)
            .await
            .map_err(Error::from)?;
        let initial_notices = crate::agent::turn_notices(&self.agent).map_err(Error::from)?;
        let inputs = selection.inputs.into_iter().filter(|(inbox, input)| {
            !matches!(input, crate::agent::InboxRecord::Notice {
                notice: crate::agent::NoticePayload::Quiet(_), ..
            } if initial_notices.as_ref().is_some_and(|selected|
                selected.range.contains(inbox)))
        });
        let mut messages = Vec::new();
        let mut notices: Option<NoticeBlocks> = None;
        let flush_notices = |notices: &mut Option<NoticeBlocks>,
                             messages: &mut Vec<ModelMessage>| {
            if let Some(notices) = notices.take() {
                messages.extend(notices.finish().into_iter().map(|content| ModelMessage {
                    role: ModelRole::User,
                    content,
                }));
            }
        };
        let mut item = 0_u64;
        for (inbox, input) in inputs {
            // Coalesce only consecutive unsent events in this frozen cut, using
            // the same bounded blocks as the turn's initial Notice selection.
            let input = match input {
                crate::agent::InboxRecord::Notice {
                    notice: crate::agent::NoticePayload::Quiet(notice),
                    ..
                } => {
                    notices
                        .get_or_insert_with(|| NoticeBlocks::new(self.maximum))
                        .push(notice)?;
                    continue;
                }
                input => crate::agent::turn_input(inbox, input).ok_or_else(|| {
                    Error::Invalid("selected steering record has no input".into())
                })?,
            };
            let waking_notice = matches!(&input.input, crate::agent::TurnInput::Notice { .. });
            if !waking_notice {
                flush_notices(&mut notices, &mut messages);
            }
            let content = crate::agent::model_input_at(inbox, &input.input).map_err(Error::from)?;
            let external = content.file_refs().cloned().collect::<Vec<_>>();
            // Staged inputs retain the original exact-file admission. Notice
            // JSON itself carries no authority to the paths it describes.
            if external.is_empty() {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content,
                });
                continue;
            }
            let authority = AdmittedAgent::verify(
                &self.issuer,
                &self.admission,
                &self.agent.path,
                &self.agent.volumes,
            )?
            .authority(&external)
            .await?;
            let mut reader = FilesystemContentVerifier::new(
                Arc::clone(&self.host),
                &authority.verifier,
                &authority.scope,
                crate::conversation::MAX_EXACT_JS_INTEGER,
                None,
            )?;
            let mut content = crate::agent::expand_attachments(content, &*reader).await?;
            if waking_notice {
                let ModelContent::Part(ModelContentPart::Notices { notices: events }) = content
                else {
                    return Err(Error::Invalid(
                        "staged steering Notice lost its event block".into(),
                    ));
                };
                for notice in events {
                    notices
                        .get_or_insert_with(|| NoticeBlocks::new(self.maximum))
                        .push(notice)?;
                }
                continue;
            }
            // The envelope delegates its validated immutable contents, not its
            // own exact-file grant. Derive those files under the same original
            // admission; an admitted caller's ceiling remains unchanged.
            if !content.file_refs().eq(external.iter()) {
                let expanded = content.file_refs().cloned().collect::<Vec<_>>();
                let authority = AdmittedAgent::verify(
                    &self.issuer,
                    &self.admission,
                    &self.agent.path,
                    &self.agent.volumes,
                )?
                .authority(&expanded)
                .await?;
                reader = FilesystemContentVerifier::new(
                    Arc::clone(&self.host),
                    &authority.verifier,
                    &authority.scope,
                    crate::conversation::MAX_EXACT_JS_INTEGER,
                    None,
                )?;
            }
            let parts = match &mut content {
                ModelContent::Text(_) => &mut [][..],
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts.as_mut_slice(),
            };
            // Pin newly authorized content into the private volume. The running
            // executor keeps its original scope; no live grant is widened.
            for part in parts {
                if let ModelContentPart::File { file, .. } = part {
                    let bytes = reader
                        .read(file, file.descriptor().byte_length(), None, None)?
                        .await?;
                    let operation = self.operation;
                    let id = identity(operation, step, item);
                    let destination =
                        format!("messages/.steering/{operation}/{step:010}/{item:020}");
                    let (published, _) = self
                        .writer
                        .stage_once(
                            id,
                            &destination,
                            &bytes,
                            file.descriptor().media_type(),
                            "steering",
                            ContentAccess::default(),
                        )?
                        .await?;
                    *file = published;
                    item = item
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("steering item overflow".into()))?;
                }
            }
            messages.push(ModelMessage {
                role: ModelRole::User,
                content,
            });
        }
        flush_notices(&mut notices, &mut messages);
        Ok(Selected {
            step,
            cut: BoundaryCut {
                from,
                through: selection.through,
            },
            messages,
        })
    }
}

impl<P, A, O> ModelBoundary for FilesystemBoundary<P, A, O>
where
    P: StreamProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn include_prepared_sources(&self, operation: OperationId, through: u64) -> Result<()> {
        self.require_operation(operation)?;
        let mut admitted = self
            .admitted
            .lock()
            .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
        if admitted.step.is_some() {
            return Err(Error::Conflict(
                "initial request is already admitted".into(),
            ));
        }
        admitted.prepared_sources = Some(
            admitted
                .prepared_sources
                .map_or(through, |previous| previous.max(through)),
        );
        Ok(())
    }

    fn messages(
        &self,
        operation: OperationId,
        step: u32,
        frozen: Option<BoundaryCut>,
    ) -> BoxProviderFuture<'_, Result<Vec<ModelMessage>>> {
        Box::pin(async move {
            self.require_operation(operation)?;
            let selected = self.select(step, frozen).await?;
            let messages = selected.messages.clone();
            self.admitted
                .lock()
                .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?
                .selected = Some(selected);
            Ok(messages)
        })
    }

    fn selection(&self, operation: OperationId, step: u32) -> Result<BoundaryCut> {
        self.require_operation(operation)?;
        let admitted = self
            .admitted
            .lock()
            .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
        admitted
            .selected
            .as_ref()
            .filter(|selected| selected.step == step)
            .map(|selected| selected.cut)
            .ok_or_else(|| Error::Storage("request has no selected steering range".into()))
    }

    fn admitted(&self, operation: OperationId, step: u32) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            self.require_operation(operation)?;
            let mut admitted = self
                .admitted
                .lock()
                .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
            if admitted.step.is_some_and(|previous| previous >= step) {
                return Ok(());
            }
            let selected = admitted
                .selected
                .as_mut()
                .filter(|selected| selected.step == step)
                .ok_or_else(|| {
                    Error::Storage("admitted request has no selected steering range".into())
                })?;
            let through = selected.cut.through;
            let messages = std::mem::take(&mut selected.messages);
            admitted.through = Some(
                admitted
                    .through
                    .map_or(through, |previous| previous.max(through)),
            );
            admitted.step = Some(admitted.step.map_or(step, |previous| previous.max(step)));
            admitted.messages.extend(messages);
            Ok(())
        })
    }

    fn prepared_snapshot(
        &self,
        operation: OperationId,
        step: u32,
    ) -> Result<Option<crate::agent::SteeringSnapshot>> {
        self.require_operation(operation)?;
        let admitted = self
            .admitted
            .lock()
            .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
        let selected = admitted
            .selected
            .as_ref()
            .filter(|selected| selected.step == step)
            .ok_or_else(|| Error::Storage("request has no selected steering range".into()))?;
        let mut messages = admitted.messages.clone();
        if admitted.step.is_none_or(|previous| previous < step) {
            messages.extend_from_slice(&selected.messages);
        }
        Ok(Some(crate::agent::SteeringSnapshot {
            through: selected.cut.through,
            messages,
        }))
    }

    fn restore_snapshot(
        &self,
        operation: OperationId,
        snapshot: Option<crate::agent::SteeringSnapshot>,
    ) -> Result<()> {
        self.require_operation(operation)?;
        let mut admitted = self
            .admitted
            .lock()
            .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
        // The journal is authoritative even if replay restored only a prefix.
        // A prepared but unadmitted candidate cannot survive cancellation.
        let (through, messages) = snapshot.map_or((None, Vec::new()), |value| {
            (Some(value.through), value.messages)
        });
        admitted.messages = messages;
        admitted.through = through;
        admitted.selected = None;
        Ok(())
    }

    fn transcript(
        &self,
        operation: OperationId,
    ) -> BoxProviderFuture<'_, Result<Option<SteeringTranscript>>> {
        Box::pin(async move {
            self.require_operation(operation)?;
            let Some((through, bytes)) = ({
                let admitted = self
                    .admitted
                    .lock()
                    .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
                if admitted.messages.is_empty() {
                    None
                } else {
                    admitted
                        .through
                        .map(|through| {
                            crate::contract::canonical_json_bytes(&admitted.messages)
                                .map(|bytes| (through, bytes))
                        })
                        .transpose()?
                }
            }) else {
                return Ok(None);
            };
            let file = self
                .files
                .stage(identity(operation, u32::MAX, 0), &bytes)
                .await?;
            Ok(Some(SteeringTranscript { through, file }))
        })
    }
}
