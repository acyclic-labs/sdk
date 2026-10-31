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
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.authorize_envelope(envelope, Some(io))?;
            // Canonicalize the envelope only. Its native body remains an exact
            // string, so this never parses or rewrites provider wire bytes.
            let bytes = io.encode(envelope, super::MAXIMUM_PAYLOAD_BYTES, true)?;
            let bytes_hold = io.guard_bytes(&bytes)?;
            let reference = <Self as ExecutionJournal>::stage(
                self,
                envelope.intent(),
                key,
                &bytes,
                MEDIA,
                Some(io),
            )?
            .await?;
            let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
            let event = ExecutionEvent::ConversationItem {
                step,
                envelope: reference,
            };
            let result = <Self as ExecutionJournal>::append_if_tail(
                self,
                envelope.intent(),
                expected_tail,
                key,
                &event,
                Some(io),
            )?
            .await;
            drop(event);
            drop(reference_hold);
            drop(bytes);
            drop(bytes_hold);
            result
        })
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
        let source = match envelope.payload() {
            Payload::Wire { .. } => return Ok(()),
            Payload::Context {
                change: ContextChange::Skill { metadata },
                ..
            } => &metadata.source,
            Payload::Context {
                change: ContextChange::Removed { source },
                ..
            } => source,
        };
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
        self.authorize_envelope(&envelope, io)?;
        drop(envelope);
        drop(hold);
        Ok(())
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;
