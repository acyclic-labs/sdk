//! A lazy reverse walk of accepted input, including inherited conversation cuts.
use super::*;
use crate::agent::{
    AgentRecord as _, ConversationEntry, InboxRecord, InputPreview, InputPreviewStep,
    InputSelection, StatusRecord,
};
use acyclic_stream::{StreamPath, StreamProviderExt as _};
use futures::StreamExt as _;
mod text;

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Select the last accepted message without collecting a transcript.
    ///
    /// Each progress step is bounded source work, including a filtered record
    /// or a structured-text source chunk. A selection carries
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
            let (mut state, consumed) = bound.agents.state_snapshot(&agent).await?;
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
            if selection != InputSelection::Latest && maximum < 4 {
                Err(Error::Record("input credit cannot hold a UTF-8 scalar"))?;
            }
            if selection == InputSelection::Pending {
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
                        let disposition = crate::agent::reply(&**provider, &mut state, consumed, &message.id, end).await?;
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
                    let message = message.filter(|message| selection != InputSelection::History
                        || matches!(message.from, crate::agent::Sender::User { .. }));
                    match message {
                        Some(message) => {
                            let reader = bound.input_reader(&state, &message.body).await?;
                            if message.body.descriptor().media_type() == crate::agent::USER_INPUT_MEDIA_TYPE {
                                let mut ranges = structured_preview(&*reader, &message.body, maximum, selection, end);
                                while let Some(step) = ranges.next().await {
                                    yield step?;
                                }
                                if selection == InputSelection::Latest { return; }
                                continue;
                            }
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
                                yield InputPreviewStep::MessageRange { inbox: end, offset, preview };
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
                            if selection != InputSelection::History && matches!(&input, InboxRecord::Message(_)) {
                                deferred = Some((end, input));
                            }
                            yield InputPreviewStep::Progress;
                        }
                    }
                }
                if selection != InputSelection::Latest {
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
        if !matches!(
            file.descriptor().media_type(),
            "text/plain" | crate::agent::USER_INPUT_MEDIA_TYPE
        ) {
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

fn structured_chunks<'a>(
    reader: &'a dyn crate::conversation::ContentResidencyVerifier,
    file: &'a FileRef,
    maximum: u64,
) -> acyclic_stream::BoxProviderStream<'a, Result<String, Error>> {
    Box::pin(async_stream::try_stream! {
        // A chunk splits work; it never limits the original message's size.
        let chunk = maximum.clamp(4, 4096);
        let mut projection = text::Text::new(chunk as usize);
        let mut offset = 0;
        while offset < file.descriptor().byte_length() {
            let part = input_prefix(reader, file, offset, chunk).await?;
            if part.text.is_empty() { Err(Error::Record("input source made no progress"))?; }
            offset += part.text.len() as u64;
            yield projection.feed(&part.text)?;
        }
        projection.finish()?;
    })
}

fn structured_preview<'a>(
    reader: &'a dyn crate::conversation::ContentResidencyVerifier,
    file: &'a FileRef,
    maximum: u64,
    selection: InputSelection,
    inbox: u64,
) -> acyclic_stream::BoxProviderStream<'a, Result<InputPreviewStep, Error>> {
    Box::pin(async_stream::try_stream! {
        // Decoded size is part of the presentation contract. Count it without
        // retaining history, yielding credit for every bounded original-source read.
        let mut bytes = 0u64;
        let mut pending = String::new();
        let mut counting = structured_chunks(reader, file, maximum);
        while let Some(part) = counting.next().await {
            let part = part?;
            bytes = bytes.checked_add(part.len() as u64).ok_or(Error::Record("input text length overflows"))?;
            if selection == InputSelection::Latest && (pending.len() as u64) < maximum {
                let mut end = (part.len() as u64).min(maximum - pending.len() as u64) as usize;
                while !part.is_char_boundary(end) { end += 1; }
                pending.push_str(&part[..end]);
            }
            yield InputPreviewStep::Progress;
        }
        if selection == InputSelection::Latest {
            // Keep at most one extra scalar while counting, then trim it.
            // The requested prefix needs no second source read.
            while pending.len() as u64 > maximum { pending.pop(); }
            yield InputPreviewStep::Selected(Some(InputPreview { text: pending, bytes }));
            return;
        }
        if bytes == 0 || maximum == 0 {
            let preview = InputPreview { text: String::new(), bytes };
            yield InputPreviewStep::MessageRange { inbox, offset: 0, preview };
            return;
        }
        let mut ranges = structured_chunks(reader, file, maximum);
        let mut offset = 0;
        while let Some(part) = ranges.next().await {
            pending.push_str(&part?);
            while pending.len() as u64 >= maximum || (offset + pending.len() as u64 == bytes && !pending.is_empty()) {
                let mut end = (pending.len() as u64).min(maximum) as usize;
                while !pending.is_char_boundary(end) { end -= 1; }
                let rest = pending.split_off(end);
                let preview = InputPreview { text: std::mem::replace(&mut pending, rest), bytes };
                let start = offset;
                offset += preview.text.len() as u64;
                yield InputPreviewStep::MessageRange { inbox, offset: start, preview };
                if offset == bytes { return; }
            }
            yield InputPreviewStep::Progress;
        }
        if offset != bytes { Err(Error::Record("input projection ended early"))?; }
    })
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
