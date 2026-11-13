use super::{Children, Cut, Error, Result};
use crate::{AgentPath, AgentRecord, ConversationEntry, Reader, StatusRecord};
use acyclic_harness_core::{conversation::FileRef, ui::TurnText};
use std::borrow::Cow;

/// Reader-backed cut plus a lookup of immutable conversation file bytes. The
/// lookup must use the full FileRef, including its generation, and never IO.
pub struct ReaderCut<'a> {
    /// Reader's sole cache, retained by the caller for this render.
    pub reader: &'a Reader,
    /// Selected agent.
    pub agent: &'a AgentPath,
    /// Already resident immutable output text; absence asks the caller to read it.
    pub file: &'a dyn Fn(&FileRef) -> Option<String>,
}
impl Cut for ReaderCut<'_> {
    fn role(&self) -> Result<Cow<'_, str>> {
        super::agent_role(self.reader, self.agent)
            .map(Cow::Owned)
            .ok_or(Error::Pending)
    }
    fn agent(&self) -> &AgentPath {
        self.agent
    }
    fn status(&self) -> Result<Cow<'_, StatusRecord>> {
        let stream = self.agent.status()?;
        let tail = self.reader.cache().tail(&stream).ok_or(Error::Pending)?;
        let bytes = tail
            .checked_sub(1)
            .and_then(|n| self.reader.cache().record(&stream, n))
            .ok_or(Error::Pending)?;
        Ok(Cow::Owned(StatusRecord::decode(bytes)?))
    }
    fn tail(&self) -> Option<u64> {
        self.agent
            .conversation()
            .ok()
            .and_then(|p| self.reader.cache().tail(&p))
    }
    fn turn(&self, sequence: u64) -> Result<Cow<'_, str>> {
        let stream = self.agent.conversation()?;
        let bytes = self
            .reader
            .cache()
            .record(&stream, sequence)
            .ok_or(Error::Pending)?;
        let entry = ConversationEntry::decode(bytes)?;
        let output = (self.file)(&entry.output).ok_or(Error::Pending)?;
        let output: TurnText = serde_json::from_str(&output)?;
        Ok(Cow::Owned(output.text))
    }
    fn diagnostic(
        &self,
        source: &acyclic_harness_core::agent::Diagnostic,
        from: u64,
    ) -> Result<Cow<'_, acyclic_harness_core::agent::DiagnosticRead>> {
        if source.agent != *self.agent {
            return Err(Error::Invalid("diagnostic belongs to another cut"));
        }
        if !matches!(&self.status()?.phase,
            acyclic_harness_core::agent::Phase::Done {
                outcome: acyclic_harness_core::agent::AgentOutcome::FailedDiagnostic { diagnostic }
            } if diagnostic == source)
        {
            return Err(Error::Invalid("diagnostic differs from the selected cut"));
        }
        let stream = source.path()?;
        let mut range = source.range(from, acyclic_stream::MAX_RECORD_BYTES as u64);
        let mut bytes = Vec::with_capacity(range.capacity());
        for sequence in range.first()..range.first() + u64::from(range.records()) {
            let original = self
                .reader
                .cache()
                .record(&stream, sequence)
                .ok_or(Error::Pending)?;
            bytes.extend_from_slice(range.take_chunk(sequence, original)?);
        }
        Ok(Cow::Owned(range.finish(bytes)?))
    }
    fn children(&self, page: u32) -> Result<Option<Children<'_>>> {
        self.agent
            .children()
            .ok()
            .and_then(|p| self.reader.cache().listing(&p, page))
            .map(|listing| Children {
                children: &listing.children,
                more: listing.next.is_some(),
            })
            .map(Some)
            .ok_or(Error::Pending)
    }
}
