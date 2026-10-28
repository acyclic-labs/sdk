use super::*;
use crate::replicated::{
    AppendIf, CommandJournal, JournalEntry, JournalFrame as Frame, JournalPage,
};

/// One exclusively owned journal epoch. Cursors are byte positions; compaction
/// starts a new instance after installing its snapshot under visibility.
pub(super) struct LocalPort {
    pub(super) stream: LocalStream,
}

#[async_trait]
impl CommandJournal for LocalPort {
    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        let stream = self.stream.clone();
        let journal = stream.inner.journal.clone();
        let head = journal.lock().map_err(|_| stream.inner.poison())?.bytes;
        if from < HEADER_BYTES as u64 || from > head || page == 0 {
            return Err(StreamError::InvalidArgument);
        }
        let entries = if from == head {
            Vec::new()
        } else {
            spawn_blocking(move || {
                let journal = journal.lock().map_err(|_| StreamError::Unavailable)?;
                if journal.bytes != head {
                    return Err(StreamError::InvalidResponse);
                }
                let lease = journal.file.lease().map_err(|_| StreamError::Unavailable)?;
                let mut file = lease.file();
                file.seek(SeekFrom::Start(from))
                    .map_err(|_| StreamError::Unavailable)?;
                let mut cursor = from;
                let mut entries = Vec::new();
                while cursor < head && entries.len() < page {
                    let frame = read_frame(file, head - cursor)
                        .map_err(|_| StreamError::Unavailable)?
                        .ok_or(StreamError::InvalidResponse)?;
                    let time = frame
                        .command
                        .first_chunk::<8>()
                        .ok_or(StreamError::InvalidResponse)?;
                    let at_millis = u64::from_le_bytes(*time);
                    cursor += frame_bytes(&frame.length);
                    entries.push(JournalEntry {
                        sequence: cursor,
                        at_millis,
                        command: crate::OwnedBytes::from(frame.command).slice(8..),
                    });
                }
                Ok(entries)
            })
            .await
            .map_err(|_| StreamError::Unavailable)??
        };
        Ok(JournalPage {
            entries,
            head,
            at_millis: self.stream.inner.commit_clock.source.now_unix_millis(),
        })
    }

    async fn append_if(
        &mut self,
        expected: u64,
        frames: Vec<Frame>,
    ) -> Result<AppendIf, StreamError> {
        // LocalStream retains its visibility fence for this whole proposal.
        let head = self
            .stream
            .inner
            .journal
            .lock()
            .map_err(|_| self.stream.inner.poison())?
            .bytes;
        if head != expected {
            return Ok(AppendIf::Stale { head });
        }
        if frames.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
        let at_millis = self.stream.inner.commit_clock.source.now_unix_millis();
        if frames
            .iter()
            .any(|frame| frame.deadline.is_some_and(|deadline| at_millis >= deadline))
        {
            return Err(StreamError::DeadlineElapsed);
        }
        let mut prepared = Vec::with_capacity(frames.len());
        for frame in frames {
            let command = decode_command(&frame.command)?;
            let mut encoded = PreparedFrame::encode(&command, at_millis, self.stream.work.clone())
                .map_err(|error| self.stream.frame_error(error))?;
            encoded.deadline = encoded.deadline.into_iter().chain(frame.deadline).min();
            prepared.push(encoded);
        }
        // Reserve the entire proposal before its first physical action.
        {
            let journal = self
                .stream
                .inner
                .journal
                .lock()
                .map_err(|_| self.stream.inner.poison())?;
            let bytes = prepared
                .iter()
                .try_fold(journal.bytes, |bytes, frame| {
                    bytes.checked_add(frame.encoded.len() as u64)
                })
                .ok_or(StreamError::Capacity)?;
            let operations = journal
                .operations
                .checked_add(prepared.len() as u64)
                .ok_or(StreamError::Capacity)?;
            if bytes > journal.limits.journal_bytes
                || operations > journal.limits.journal_operations
            {
                return Err(StreamError::Capacity);
            }
        }
        for frame in prepared {
            self.stream.persist(frame).await?;
        }
        let sequence = self
            .stream
            .inner
            .journal
            .lock()
            .map_err(|_| self.stream.inner.poison())?
            .bytes;
        Ok(AppendIf::Accepted {
            sequence,
            at_millis,
        })
    }

    fn changed(
        &mut self,
        after: u64,
    ) -> crate::BoxProviderFuture<'static, Result<(), StreamError>> {
        let mut changed = self.stream.inner.revision.subscribe();
        let mut failed = self.stream.inner.failed.subscribe();
        let stream = self.stream.worker();
        Box::pin(async move {
            loop {
                stream.check_available()?;
                changed.borrow_and_update();
                let head = stream
                    .inner
                    .journal
                    .lock()
                    .map_err(|_| stream.inner.poison())?
                    .bytes;
                if head != after {
                    return Ok(());
                }
                match futures::future::select(
                    Box::pin(changed.changed()),
                    Box::pin(LocalInner::unavailable(&mut failed)),
                )
                .await
                {
                    futures::future::Either::Left((result, _)) => {
                        result.map_err(|_| StreamError::Unavailable)?
                    }
                    futures::future::Either::Right((error, _)) => return Err(error),
                }
            }
        })
    }
}
