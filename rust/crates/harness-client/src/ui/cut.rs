use super::{Children, Cut, Error, Result};
use crate::{AgentPath, AgentRecord, ConversationEntry, Reader, StatusRecord};
use acyclic_harness_core::{conversation::FileRef, executor::TurnOutput};

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
    fn status(&self) -> Result<StatusRecord> {
        let stream = self.agent.status()?;
        let tail = self.reader.cache().tail(&stream).ok_or(Error::Pending)?;
        let bytes = tail
            .checked_sub(1)
            .and_then(|n| self.reader.cache().record(&stream, n))
            .ok_or(Error::Pending)?;
        Ok(StatusRecord::decode(bytes)?)
    }
    fn tail(&self) -> Option<u64> {
        self.agent
            .conversation()
            .ok()
            .and_then(|p| self.reader.cache().tail(&p))
    }
    fn turn(&self, sequence: u64) -> Result<String> {
        let stream = self.agent.conversation()?;
        let bytes = self
            .reader
            .cache()
            .record(&stream, sequence)
            .ok_or(Error::Pending)?;
        let entry = ConversationEntry::decode(bytes)?;
        let output = (self.file)(&entry.output).ok_or(Error::Pending)?;
        let output: TurnOutput = serde_json::from_str(&output)?;
        Ok(output.text)
    }
    fn children(&self, page: u32) -> Option<Children<'_>> {
        self.agent
            .children()
            .ok()
            .and_then(|p| self.reader.cache().listing(&p, page))
            .map(|listing| Children {
                children: &listing.children,
                more: listing.next.is_some(),
            })
    }
}
