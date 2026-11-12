//! A lazy reverse walk of accepted input, including inherited conversation cuts.
use super::*;
use crate::agent::{
    AgentRecord as _, ConversationEntry, InboxRecord, InputPreview, InputPreviewStep,
    InputSelection, StatusRecord,
};
use acyclic_stream::{StreamPath, StreamProviderExt as _};

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Select the last accepted message without collecting a transcript.
    ///
    /// Each progress step is one filtered source record. A selection carries
    /// the bounded prefix, or proves that the cut has no message. Dropping
    /// the stream stops the walk. Opening it performs no IO. The selected
    /// inbox tail and inherited conversation prefix are immutable thereafter.
    pub fn input_preview_steps(
        &self,
        agent: AgentPath,
        maximum: u64,
        selection: InputSelection,
    ) -> acyclic_stream::BoxProviderStream<'static, Result<InputPreviewStep, Error>> {
        let bound = self.clone();
        Box::pin(async_stream::try_stream! {
            bound.admit_path(&agent)?;
            let (state, consumed) = bound.agents.state_snapshot(&agent).await?;
            let provider = bound.agents.provider();
            let inbox = agent.inbox()?;
            let tail = provider.tail(inbox.clone()).await.map_err(Error::Stream)?;
            let floor = input_floor(provider, &agent).await?;
            if tail < floor {
                Err(Error::Record("input tail precedes adoption"))?;
            }
            let mut selected = inbox;
            let mut end = tail;
            let mut lower = floor;
            if selection == InputSelection::Pending {
                if maximum < 4 {
                    Err(Error::Record("pending input credit cannot hold a UTF-8 scalar"))?;
                }
                if state.closed {
                    yield InputPreviewStep::Selected(None);
                    return;
                }
                lower = lower.max(state.settled_before(consumed));
            }
            // No accepted message exists in the owned inbox after this walk,
            // so own settled turns cannot add a message. Start at adoption's
            // inherited cut instead of visiting own control-only history.
            let mut conversation = state.conversation_base;
            let mut admitted = None::<ConversationEntry>;
            let mut deferred = None::<(u64, InboxRecord)>;
            loop {
                while deferred.is_none() && end > lower {
                    end -= 1;
                    if selection == InputSelection::Pending && !state.input_pending(consumed, end) {
                        yield InputPreviewStep::Progress;
                        continue;
                    }
                    let record = input_record(provider, &selected, end).await?;
                    let input = InboxRecord::decode(&record.value)?;
                    // A reserved turn can start below an already answered
                    // steering message. The global high-water mark alone is
                    // not its disposition: use the one committed-reply proof
                    // for this ambiguous interval, never a second history walk.
                    if selection == InputSelection::Pending
                        && let InboxRecord::Message(message) = &input
                        && end < state.steered_through
                        && state.turn.as_ref().is_none_or(|turn| turn.inbox != end)
                    {
                        let disposition = crate::agent::reply(&**provider, &state, consumed, &message.id, end).await?;
                        if !matches!(disposition, crate::agent::Reply::Pending) {
                            yield InputPreviewStep::Progress;
                            continue;
                        }
                    }
                    let message = match &admitted {
                        Some(entry) => entry.admitted_message(end, &input),
                        None => match &input {
                            InboxRecord::Message(message) | InboxRecord::FollowUp(message) => Some(message),
                            _ => None,
                        },
                    };
                    match message {
                        Some(message) => {
                            let reader = bound.input_reader(&state, &message.body).await?;
                            let mut offset = 0;
                            loop {
                                let preview = input_prefix(&*reader, &message.body, offset, maximum).await?;
                                if selection == InputSelection::Latest {
                                    yield InputPreviewStep::Selected(Some(preview));
                                    return;
                                }
                                let next = offset + preview.text.len() as u64;
                                let complete = next == preview.bytes;
                                if !complete && next == offset {
                                    Err(Error::Record("pending input range made no progress"))?;
                                }
                                yield InputPreviewStep::Pending { inbox: end, offset, preview };
                                if complete { break; }
                                offset = next;
                            }
                        }
                        _ => {
                            // An ordinary message may have passed an earlier
                            // turn's steering boundary, before this hook's own
                            // turn. Keep its one original record until that
                            // entry's proof arrives; do not replay overlapping
                            // inbox ranges or silently discard the candidate.
                            if matches!(&input, InboxRecord::Message(_)) {
                                deferred = Some((end, input));
                            }
                            yield InputPreviewStep::Progress;
                        }
                    }
                }
                if selection == InputSelection::Pending {
                    yield InputPreviewStep::Selected(None);
                    return;
                }
                // A fork's private inbox starts fresh. Its context retains the
                // original appending stream on every inherited record, so no
                // parent lookup or grant on an entire parent volume is needed.
                let Some(sequence) = conversation.checked_sub(1) else {
                    if deferred.is_some() {
                        Err(Error::Record("message has no committed admission proof"))?;
                    }
                    yield InputPreviewStep::Selected(None);
                    return;
                };
                conversation = sequence;
                let record = input_record(provider, &agent.conversation()?, sequence).await?;
                let entry = ConversationEntry::decode(&record.value)?;
                let source = record.origin.as_str().strip_suffix("/conversation")
                    .ok_or(Error::Record("conversation origin is not an agent stream"))?;
                let source = AgentPath::try_from(source.to_owned())?;
                let source_inbox = source.inbox()?;
                if source_inbox != selected {
                    if deferred.is_some() {
                        Err(Error::Record("message admission crosses its original inbox"))?;
                    }
                    let floor = input_floor(provider, &source).await?;
                    if entry.inbox < floor {
                        Err(Error::Record("conversation input precedes adoption"))?;
                    }
                    selected = source_inbox;
                    end = entry.observed_through;
                } else {
                    end = end.min(entry.observed_through);
                }
                if entry.observed_through <= entry.inbox {
                    Err(Error::Record("conversation input has no admitted prefix"))?;
                }
                if let Some((inbox, input)) = &deferred
                    && let Some(message) = entry.admitted_message(*inbox, input)
                {
                    let reader = bound.input_reader(&state, &message.body).await?;
                    let preview = input_prefix(&*reader, &message.body, 0, maximum).await?;
                    yield InputPreviewStep::Selected(Some(preview));
                    return;
                }
                // Only this turn's exact input and ordinary admitted steering
                // can disclose a file. Older entries retain their own proof;
                // overlapping work is retained in the one deferred message.
                lower = entry.inbox;
                admitted = Some(entry);
                yield InputPreviewStep::Progress;
            }
        })
    }

    async fn input_reader(
        &self,
        state: &crate::agent::AgentState,
        file: &FileRef,
    ) -> Result<Arc<super::super::FilesystemContentVerifier<StreamAuthorityStore<P>, O>>, Error>
    {
        if file.descriptor().media_type() != "text/plain" {
            return Err(Error::Record("structured input has no plain-text preview"));
        }
        // The host selected this exact reference from an accepted inbox record
        // in the selected context. A caller never supplies an external file.
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            std::slice::from_ref(file),
        )
        .await
        .map_err(turn)?;
        super::super::FilesystemContentVerifier::new(
            Arc::clone(&self.volumes),
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)
    }
}

async fn input_prefix(
    reader: &dyn crate::conversation::ContentResidencyVerifier,
    file: &FileRef,
    offset: u64,
    maximum: u64,
) -> Result<InputPreview, Error> {
    let length = file.descriptor().byte_length();
    let remaining = length
        .checked_sub(offset)
        .ok_or(Error::Record("input offset is past its end"))?;
    let prefix = crate::conversation::ContentResidencyVerifier::read_range(
        reader,
        file,
        acyclic_fs::ByteRange {
            offset,
            length: remaining.min(maximum),
        },
        None,
        None,
    )
    .map_err(turn)?
    .await
    .map_err(turn)?;
    let text = match std::str::from_utf8(&prefix) {
        Ok(text) => text,
        Err(error) if error.error_len().is_none() && (prefix.len() as u64) < remaining => {
            std::str::from_utf8(&prefix[..error.valid_up_to()])
                .map_err(|_| Error::Record("input prefix is not UTF-8"))?
        }
        Err(_) => return Err(Error::Record("input is not UTF-8")),
    };
    Ok(InputPreview {
        text: text.to_owned(),
        bytes: length,
    })
}

async fn input_floor<P: crate::store::JournalProvider + ?Sized>(
    provider: &Arc<P>,
    agent: &AgentPath,
) -> Result<u64, Error> {
    let status = input_record(provider, &agent.status()?, 0).await?;
    Ok(StatusRecord::decode(&status.value)?.cursor)
}

async fn input_record<P: crate::store::JournalProvider + ?Sized>(
    provider: &Arc<P>,
    path: &StreamPath,
    sequence: u64,
) -> Result<crate::store::operations::ObservedRecord, Error> {
    crate::store::operations::one_record(provider, path, sequence, &mut None)
        .await
        .map_err(turn)?
        .ok_or(Error::Record("input preview source is absent"))
}
