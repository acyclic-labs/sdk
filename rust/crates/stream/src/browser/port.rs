use super::*;
use crate::replicated::{AppendIf, CommandJournal, JournalEntry, JournalFrame, JournalPage};

pub(super) struct BrowserPort<'a> {
    stream: &'a BrowserStream,
    transaction: Option<Transaction<'a>>,
    head: u64,
    bytes: u64,
    changed: Option<crate::change::Subscription>,
}

impl<'a> BrowserPort<'a> {
    pub(super) fn new(
        stream: &'a BrowserStream,
        transaction: Transaction<'a>,
    ) -> Result<Self, StreamError> {
        Ok(Self {
            stream,
            transaction: Some(transaction),
            head: 0,
            bytes: 0,
            changed: Some(stream.inner.publications.journal.subscribe()?),
        })
    }
}

impl CommandJournal for BrowserPort<'_> {
    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        let store = self
            .transaction
            .as_ref()
            .ok_or(StreamError::Unavailable)?
            .object_store(JOURNAL)
            .map_err(backend)?;
        let metadata = store
            .get::<JsValue, _, _>(METADATA)
            .primitive()
            .map_err(backend)?
            .await
            .map_err(backend)?;
        let (head, bytes) = match metadata {
            None => (0, 0),
            Some(value) => {
                let words = bounded_buffer(value, METADATA_WORDS * 8, METADATA_WORDS * 8)?
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|word| u64::from_le_bytes(*word))
                    .collect::<Vec<_>>();
                let mut words = words.into_iter();
                let head = words.next().ok_or(StreamError::InvalidResponse)?;
                let bytes = words.next().ok_or(StreamError::InvalidResponse)?;
                if !words.eq(self.stream.inner.limits.words()) {
                    return Err(StreamError::InvalidArgument);
                }
                (head, bytes)
            }
        };
        if from > head
            || head > self.stream.inner.limits.commands
            || bytes > self.stream.inner.limits.journal_bytes
        {
            return Err(StreamError::InvalidResponse);
        }
        self.head = head;
        self.bytes = bytes;
        let end = head.min(from.saturating_add(page as u64));
        let mut entries = Vec::with_capacity((end - from) as usize);
        for index in from..end {
            let key = format!("{index:016x}");
            let value = store
                .get::<JsValue, _, _>(key.as_str())
                .primitive()
                .map_err(backend)?
                .await
                .map_err(backend)?
                .ok_or(StreamError::InvalidResponse)?;
            let frame = bounded_buffer(value, 9, MAX_COMMAND_BYTES + 8)?;
            let (time, _) = frame.split_at(8);
            let at_millis = u64::from_le_bytes(time.try_into().map_err(backend)?);
            entries.push(JournalEntry {
                sequence: index + 1,
                at_millis,
                command: OwnedBytes::from(frame).slice(8..),
            });
        }
        Ok(JournalPage {
            entries,
            head,
            at_millis: BrowserClock.now_unix_millis(),
        })
    }

    async fn append_if(
        &mut self,
        expected: u64,
        frames: &[JournalFrame<'_>],
    ) -> Result<AppendIf, StreamError> {
        if expected != self.head {
            return Ok(AppendIf::Stale { head: self.head });
        }
        if frames.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
        let at_millis = frames.last().ok_or(StreamError::InvalidArgument)?.at_millis;
        let now = BrowserClock.now_unix_millis();
        if frames
            .iter()
            .any(|frame| frame.deadline.is_some_and(|deadline| now >= deadline))
        {
            return Err(StreamError::DeadlineElapsed);
        }
        let next = expected
            .checked_add(frames.len() as u64)
            .filter(|head| *head <= self.stream.inner.limits.commands)
            .ok_or(StreamError::Capacity)?;
        let bytes = frames
            .iter()
            .try_fold(self.bytes, |total, frame| {
                total.checked_add(frame.command.encoded_len() as u64 + 8)
            })
            .filter(|bytes| *bytes <= self.stream.inner.limits.journal_bytes)
            .ok_or(StreamError::Capacity)?;
        let store = self
            .transaction
            .as_ref()
            .ok_or(StreamError::Unavailable)?
            .object_store(JOURNAL)
            .map_err(backend)?;
        for (offset, frame) in frames.iter().enumerate() {
            let mut encoded = Vec::with_capacity(8 + frame.command.encoded_len());
            encoded.extend_from_slice(&frame.at_millis.to_le_bytes());
            frame.command.encode(&mut encoded);
            let key = format!("{:016x}", expected + offset as u64);
            store
                .add(Uint8Array::from(encoded))
                .with_key(key.as_str())
                .primitive()
                .map_err(backend)?
                .await
                .map_err(backend)?;
        }
        let metadata = [next, bytes]
            .into_iter()
            .chain(self.stream.inner.limits.words())
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        store
            .put(Uint8Array::from(metadata))
            .with_key(METADATA)
            .primitive()
            .map_err(backend)?
            .await
            .map_err(backend)?;
        // Refresh transactions wait for this strict writer, including abort.
        // Announcing before its await preserves wakeup custody on cancellation.
        self.stream.inner.publications.publish()?;
        self.transaction
            .take()
            .ok_or(StreamError::Unavailable)?
            .commit()
            .await
            .map_err(backend)?;
        self.head = next;
        self.bytes = bytes;
        Ok(AppendIf::Accepted {
            sequence: next,
            at_millis,
        })
    }

    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>> {
        let head = self.head;
        let registration = self.changed.take().ok_or(StreamError::Unavailable);
        Box::pin(async move {
            if head > after {
                return Ok(());
            }
            registration?.changed().await
        })
    }
}
