//! A model dispatch's text while it streams, staged as files for observers.
//!
//! The answer and reasoning a provider has produced are staged whenever the
//! provider's stream pauses, as `{directory}/{dispatch}/reply/{sequence}` and
//! `{directory}/{dispatch}/reasoning/{sequence}`, beside the turn's tool
//! output. So an observer following that directory shows each delta as it
//! arrives, over the same follow as tool output, and the staging rate is
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
        let directory = format!("{}/{}", self.directory, OperationId::new());
        LiveText {
            reply: self,
            reasoning: Staged::new(format!("{directory}/reasoning")),
            answer: Staged::new(format!("{directory}/reply")),
        }
    }
}

/// One dispatch's text not yet staged: its reasoning and its answer, each a
/// numbered sequence of chunks under its own directory.
pub(super) struct LiveText<'a> {
    reply: &'a LiveReply,
    reasoning: Staged,
    answer: Staged,
}

struct Staged {
    directory: String,
    pending: String,
    sequence: u64,
}

impl Staged {
    const fn new(directory: String) -> Self {
        Self {
            directory,
            pending: String::new(),
            sequence: 0,
        }
    }

    async fn flush(&mut self, publisher: &dyn ContentPublisher) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let path = format!("{}/{:016x}", self.directory, self.sequence);
        let digest = blake3::derive_key("acyclic.harness.live-reply/v1", path.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest[..16]);
        let label = self
            .directory
            .rsplit_once('/')
            .map_or(self.directory.as_str(), |(_, label)| label);
        publisher
            .stage_once(
                OperationId::from_bytes(operation),
                &path,
                self.pending.as_bytes(),
                "text/plain",
                label,
                ContentAccess::default(),
            )?
            .await?;
        self.pending.clear();
        self.sequence += 1;
        Ok(())
    }
}

impl LiveText<'_> {
    pub(super) fn observe(&mut self, event: &ModelEvent) {
        match event {
            ModelEvent::Content { delta } => self.answer.pending.push_str(delta),
            ModelEvent::Reasoning { delta } => self.reasoning.pending.push_str(delta),
            _ => {}
        }
    }

    /// Stages the text observed since the last call, if any: reasoning
    /// first, since a provider streams it before the answer it leads to.
    pub(super) async fn flush(&mut self) -> Result<()> {
        let publisher = self.reply.publisher.as_ref();
        self.reasoning.flush(publisher).await?;
        self.answer.flush(publisher).await
    }
}
