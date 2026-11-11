//! A lazy reverse walk of accepted input, including inherited conversation cuts.
use super::*;
use crate::agent::{
    AgentRecord as _, ConversationEntry, InboxRecord, InputPreview, InputPreviewStep, StatusRecord,
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
    ) -> acyclic_stream::BoxProviderStream<'static, Result<InputPreviewStep, Error>> {
        let bound = self.clone();
        Box::pin(async_stream::try_stream! {
            bound.admit_path(&agent)?;
            let state = bound.agents.state(&agent).await?;
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
            let mut conversation = state.conversation_tail;
            loop {
                while end > lower {
                    end -= 1;
                    let record = input_record(provider, &selected, end).await?;
                    match InboxRecord::decode(&record.value)? {
                        InboxRecord::Message(message) | InboxRecord::FollowUp(message) => {
                            let preview = bound.input_prefix(&state, &message.body, maximum).await?;
                            yield InputPreviewStep::Selected(Some(preview));
                            return;
                        }
                        _ => yield InputPreviewStep::Progress,
                    }
                }
                // A fork's private inbox starts fresh. Its context retains the
                // original appending stream on every inherited record, so no
                // parent lookup or grant on an entire parent volume is needed.
                let Some(sequence) = conversation.checked_sub(1) else {
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
                    lower = input_floor(provider, &source).await?;
                    selected = source_inbox;
                    end = entry.observed_through;
                    if end < lower {
                        Err(Error::Record("conversation input precedes adoption"))?;
                    }
                }
                // Consecutive entries from the same origin name earlier
                // prefixes already proved to contain no accepted message.
                yield InputPreviewStep::Progress;
            }
        })
    }

    async fn input_prefix(
        &self,
        state: &crate::agent::AgentState,
        file: &FileRef,
        maximum: u64,
    ) -> Result<InputPreview, Error> {
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
        let reader = super::super::FilesystemContentVerifier::new(
            Arc::clone(&self.volumes),
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let length = file.descriptor().byte_length();
        let prefix = crate::conversation::ContentResidencyVerifier::read_range(
            &*reader,
            file,
            acyclic_fs::ByteRange {
                offset: 0,
                length: length.min(maximum),
            },
            None,
            None,
        )
        .map_err(turn)?
        .await
        .map_err(turn)?;
        let text = match std::str::from_utf8(&prefix) {
            Ok(text) => text,
            Err(error) if error.error_len().is_none() && (prefix.len() as u64) < length => {
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
