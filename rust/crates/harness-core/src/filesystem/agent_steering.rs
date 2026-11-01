//! Inbox selections frozen by the executor's existing ContextPrepared record.
//! ModelStarted is admission evidence; selection alone consumes none.

use super::{
    FilesystemContentVerifier, FilesystemHost,
    agent_internal::AgentInternalFiles,
    agent_turns::{AdmittedAgent, NoticeBlocks, TurnAdmission, agent_error},
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
    operation: OperationId,
    step: u32,
    cut: BoundaryCut,
    messages: Vec<ModelMessage>,
}

#[derive(Default)]
struct Admitted {
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
    pub inbox: u64,
    pub cursor: u64,
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
        inbox: u64,
        cursor: u64,
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
            inbox,
            cursor,
            files,
            writer,
            maximum,
            admitted: Mutex::default(),
        }
    }
}

impl<P, A, O> FilesystemBoundary<P, A, O>
where
    P: StreamProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn select(
        &self,
        operation: OperationId,
        step: u32,
        frozen: Option<BoundaryCut>,
    ) -> Result<Selected> {
        let from = frozen.map(|cut| cut.from).unwrap_or({
            let admitted = self
                .admitted
                .lock()
                .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
            admitted
                .through
                .unwrap_or_else(|| self.inbox.saturating_add(1).max(self.agent.steered_through))
        });
        // The first request uses the turn's frozen original input and Notice
        // selection. Arrivals become steering only after that request's next
        // model boundary; a resumed first request cannot gain a newer prefix.
        if step == 0 {
            if frozen.is_some_and(|cut| cut.from != from || cut.through != from) {
                return Err(Error::Storage(
                    "initial request has a nonempty steering range".into(),
                ));
            }
            return Ok(Selected {
                operation,
                step,
                cut: BoundaryCut {
                    from,
                    through: from,
                },
                messages: Vec::new(),
            });
        }
        let selection = crate::agent::select_boundary(
            &*self.stream,
            &self.agent,
            self.cursor,
            from,
            self.inbox,
            frozen.map(|cut| cut.through),
        )
        .await
        .map_err(agent_error)?;
        let initial_notices = crate::agent::turn_notices(&self.agent).map_err(agent_error)?;
        let inputs = selection.inputs.into_iter().filter(|input| {
            !matches!(&input.input, crate::agent::TurnInput::Notice { notice, .. }
                if !notice.wake && initial_notices.as_ref().is_some_and(|selected|
                    selected.range.contains(&input.inbox)))
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
        for input in inputs {
            // Coalesce only consecutive unsent events in this frozen cut, using
            // the same bounded blocks as the turn's initial Notice selection.
            if let crate::agent::TurnInput::Notice { notice, .. } = &input.input {
                notices
                    .get_or_insert_with(|| NoticeBlocks::new(self.maximum))
                    .push(notice.clone())?;
                continue;
            }
            flush_notices(&mut notices, &mut messages);
            let content = crate::agent::model_input(&input.input).map_err(agent_error)?;
            let external = content.file_refs().cloned().collect::<Vec<_>>();
            // The original inbox read already authorized the event. JSON in a
            // Notice grants no file authority, so it needs no file verifier,
            // derived grant or private-volume publication.
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
            operation,
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
    fn messages(
        &self,
        operation: OperationId,
        step: u32,
        frozen: Option<BoundaryCut>,
    ) -> BoxProviderFuture<'_, Result<Vec<ModelMessage>>> {
        Box::pin(async move {
            let selected = self.select(operation, step, frozen).await?;
            let messages = selected.messages.clone();
            self.admitted
                .lock()
                .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?
                .selected = Some(selected);
            Ok(messages)
        })
    }

    fn selection(&self, operation: OperationId, step: u32) -> Result<BoundaryCut> {
        let admitted = self
            .admitted
            .lock()
            .map_err(|_| Error::Storage("steering admission lock poisoned".into()))?;
        admitted
            .selected
            .as_ref()
            .filter(|selected| selected.operation == operation && selected.step == step)
            .map(|selected| selected.cut)
            .ok_or_else(|| Error::Storage("request has no selected steering range".into()))
    }

    fn admitted(&self, operation: OperationId, step: u32) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
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
                .filter(|selected| selected.operation == operation && selected.step == step)
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

    fn transcript(
        &self,
        operation: OperationId,
    ) -> BoxProviderFuture<'_, Result<Option<SteeringTranscript>>> {
        Box::pin(async move {
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
