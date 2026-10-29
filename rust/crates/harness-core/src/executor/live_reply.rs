//! A model dispatch's text while it streams, staged as files for observers.
//!
//! The text a provider has produced is staged whenever the provider's stream
//! pauses, as `{directory}/{dispatch}/reply/{sequence}`, beside the turn's
//! tool output. So an observer following that directory shows the answer as
//! it arrives, over the same follow as tool output, and the staging rate is
//! bounded by the publication itself: no timer and no per-token write. A
//! replayed or reconciled step stages nothing; the settled turn is the record.
use super::*;
use crate::conversation::{ContentAccess, ContentPublisher};

/// Where an executor stages its live answer text.
#[derive(Clone)]
pub struct LiveReply {
    publisher: Arc<dyn ContentPublisher>,
    /// `tools/{turn}` on the publisher's volume.
    directory: String,
}

impl LiveReply {
    /// Stages the live text of each model dispatch under `directory` through
    /// `publisher`.
    #[must_use]
    pub fn new(publisher: Arc<dyn ContentPublisher>, directory: String) -> Self {
        Self {
            publisher,
            directory,
        }
    }

    pub(super) fn dispatch(&self) -> LiveText<'_> {
        LiveText {
            reply: self,
            directory: format!("{}/{}/reply", self.directory, OperationId::new()),
            pending: String::new(),
            sequence: 0,
        }
    }
}

/// One dispatch's text not yet staged.
pub(super) struct LiveText<'a> {
    reply: &'a LiveReply,
    directory: String,
    pending: String,
    sequence: u64,
}

impl LiveText<'_> {
    pub(super) fn observe(&mut self, event: &ModelEvent) {
        if let ModelEvent::Content { delta } = event {
            self.pending.push_str(delta);
        }
    }

    /// Stages the text observed since the last call, if any.
    pub(super) async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let path = format!("{}/{:016x}", self.directory, self.sequence);
        let digest = blake3::derive_key("acyclic.harness.live-reply/v1", path.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest[..16]);
        self.reply
            .publisher
            .stage_once(
                OperationId::from_bytes(operation),
                &path,
                self.pending.as_bytes(),
                "text/plain",
                "reply",
                ContentAccess::default(),
            )?
            .await?;
        self.pending.clear();
        self.sequence += 1;
        Ok(())
    }
}
