//! Immutable conversation items use the original observation and retry index.
use super::*;
use crate::conversation::envelope::{ContextChange, Envelope, Payload};

const MEDIA: &str = "application/vnd.acyclic.conversation-item+json";

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Stages one immutable item and conditionally appends its original reference.
    /// The exclusive journal cursor and retry association are the existing ones.
    /// Staging alone never makes the item visible. This establishes neither a
    /// filesystem publication end nor a closed model/tool exchange.
    pub(super) fn append_conversation<'a>(
        &'a self,
        expected_tail: u64,
        key: &'a str,
        step: u32,
        envelope: &'a Envelope,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.append_contributed_conversation(expected_tail, key, step, envelope, &[], io)
    }

    /// Joins authorized producers' encoded records to the original journal cut.
    /// Only the successful cut advances their original publication fences.
    pub(in crate::filesystem) fn append_contributed_conversation<'a>(
        &'a self,
        expected_tail: u64,
        key: &'a str,
        step: u32,
        envelope: &'a Envelope,
        contributions: &'a [indexed_publication::Contribution<'a>],
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            if matches!(envelope.payload(), Payload::ContextBatch { .. }) {
                return Err(canonical_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "context changes require their original source and epoch cut",
                ));
            }
            self.authorize_envelope(envelope, Some(io))?;
            // Canonicalize the envelope only. Its native body remains an exact
            // string, so this never parses or rewrites provider wire bytes.
            let bytes = envelope.encode(io, super::MAXIMUM_PAYLOAD_BYTES)?;
            let bytes_hold = io.guard_bytes(&bytes)?;
            let result = self
                .append_conversation_bytes(
                    envelope.intent(),
                    expected_tail,
                    key,
                    step,
                    &bytes,
                    contributions,
                    None,
                    io,
                )
                .await;
            drop(bytes);
            drop(bytes_hold);
            result
        })
    }

    pub(super) async fn append_conversation_bytes(
        &self,
        intent: OperationId,
        expected_tail: u64,
        key: &str,
        step: u32,
        bytes: &acyclic_stream::OwnedBytes,
        contributions: &[indexed_publication::Contribution<'_>],
        confirmation: Option<&mut Option<acyclic_stream::CommitId>>,
        io: &PreparationIo,
    ) -> Result<bool> {
        let reference =
            <Self as ExecutionJournal>::stage(self, intent, key, bytes, MEDIA, Some(io))?.await?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let event = ExecutionEvent::ConversationItem {
            step,
            envelope: reference,
            publication: indexed_publication::contribution_identity(contributions, Some(io))?,
        };
        let result = self
            .append_contributed_if_tail(
                intent,
                expected_tail,
                key,
                &event,
                contributions,
                confirmation,
                Some(io),
            )?
            .await;
        drop(event);
        drop(reference_hold);
        result
    }

    fn authorize_envelope(&self, envelope: &Envelope, io: Option<&PreparationIo>) -> Result<()> {
        self.require_operation(envelope.intent(), io)?;
        if self.execution.is_none() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "conversation admission requires its original bound execution",
            ));
        }
        match envelope.payload() {
            Payload::Wire { .. } => Ok(()),
            Payload::ContextBatch { changes, .. } => {
                for change in changes {
                    self.authorize_context_source(
                        match change {
                            ContextChange::Skill { metadata } => &metadata.source,
                            ContextChange::Removed { source } => source,
                        },
                        io,
                    )?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn authorize_context_source(
        &self,
        source: &crate::context::PinnedContextPath,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let admitted = crate::executor::with_history_io(io, |work| {
            source.validate(work)?;
            crate::core::directory_read_granted_with_work(
                self.scope.capabilities(),
                &source.root.volume,
                &source.root.directory,
                work,
            )
        })?;
        if !admitted {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "conversation context lacks its original directory read grant",
            ));
        }
        Ok(())
    }

    pub(super) fn verify_conversation_envelope(
        &self,
        bytes: &[u8],
        reference: &FileRef,
        publication: Option<[u8; 32]>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if reference.descriptor().media_type() != MEDIA {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "conversation observation has another payload type",
            ));
        }
        let (envelope, hold) = crate::executor::decode_json::<Envelope>(bytes, io)?;
        if matches!(envelope.payload(), Payload::ContextBatch { .. }) && publication.is_none() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "context observation lacks its original contributed cut",
            ));
        }
        self.authorize_envelope(&envelope, io)?;
        drop(envelope);
        drop(hold);
        Ok(())
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;
