use super::*;
use crate::replicated::{
    AppendIf, CommandJournal, JournalEntry, JournalFrame as Frame, JournalPage,
};

/// One exclusively owned journal epoch. Cursors are byte positions; compaction
/// starts a new instance after installing its snapshot under visibility.
pub(super) struct LocalPort {
    pub(super) stream: LocalStream,
    prepared: Option<PreparedFrame>,
}

enum Frames {
    One(Option<PreparedFrame>),
    Many {
        frames: std::vec::IntoIter<PreparedFrame>,
        _allocation: Option<acyclic_native_runtime::ReadAllocation>,
    },
}
impl Frames {
    fn as_slice(&self) -> &[PreparedFrame] {
        match self {
            Self::One(frame) => frame.as_slice(),
            Self::Many { frames, .. } => frames.as_slice(),
        }
    }
}
impl Iterator for Frames {
    type Item = PreparedFrame;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(frame) => frame.take(),
            Self::Many { frames, .. } => frames.next(),
        }
    }
}

impl LocalPort {
    pub(super) fn new(stream: LocalStream) -> Self {
        Self {
            stream,
            prepared: None,
        }
    }

    pub(super) fn prepared(stream: LocalStream, frame: PreparedFrame) -> Self {
        Self {
            stream,
            prepared: Some(frame),
        }
    }
}

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
            spawn_blocking("streams", "read-page", &format_args!("{:p}/{from}", Arc::as_ptr(&journal)), move || {
                let journal = journal.lock().map_err(|_| StreamError::Unavailable)?;
                if journal.bytes != head {
                    return Err(StreamError::InvalidResponse);
                }
                let lease = journal.file.lease().map_err(|error| {
                    stream
                        .inner
                        .fail(LocalStreamFailure::io("lease_read", &error))
                })?;
                let mut file = lease.file();
                file.seek(SeekFrom::Start(from))
                    .map_err(|error| stream.inner.fail(LocalStreamFailure::io("seek", &error)))?;
                let mut cursor = from;
                let mut entries = Vec::new();
                while cursor < head && entries.len() < page {
                    let frame = read_frame(file, head - cursor)
                        .map_err(|error| {
                            stream.inner.fail(LocalStreamFailure::local(
                                "read_frame",
                                &error,
                                stream.work.as_deref(),
                            ))
                        })?
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
        frames: &[Frame<'_>],
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
        let at_millis = frames.last().ok_or(StreamError::InvalidArgument)?.at_millis;
        let now = self.stream.inner.commit_clock.source.now_unix_millis();
        if frames
            .iter()
            .any(|frame| frame.deadline.is_some_and(|deadline| now >= deadline))
        {
            return Err(StreamError::DeadlineElapsed);
        }
        let prepared = (|| {
            if let Some(mut prepared) = self.prepared.take() {
                let [frame] = frames else {
                    return Err(StreamError::InvalidArgument);
                };
                prepared
                    .fill(frame.command, frame.at_millis)
                    .map_err(|error| self.stream.frame_error(error))?;
                prepared.deadline = frame.deadline;
                return Ok(Frames::One(Some(prepared)));
            }
            let (mut prepared, allocation) = if let Some(work) = &self.stream.work {
                work.reserve_durability(
                    (frames.len() as u64)
                        .checked_mul(4)
                        .ok_or(StreamError::Capacity)?,
                )?;
                let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&work.meter);
                let prepared = work.record(crate::native::vector(
                    frames.len(),
                    &work.meter,
                    &mut allocation,
                ))?;
                (prepared, Some(allocation))
            } else {
                (Vec::with_capacity(frames.len()), None)
            };
            for frame in frames {
                let mut encoded =
                    PreparedFrame::encode(frame.command, frame.at_millis, self.stream.work.clone())
                        .map_err(|error| self.stream.frame_error(error))?;
                encoded.deadline = frame.deadline;
                prepared.push(encoded);
            }
            Ok::<_, StreamError>(Frames::Many {
                frames: prepared.into_iter(),
                _allocation: allocation,
            })
        })();
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Ok(AppendIf::Refused(error)),
        };
        // Reserve the entire proposal before its first physical action.
        {
            let journal = self
                .stream
                .inner
                .journal
                .lock()
                .map_err(|_| self.stream.inner.poison())?;
            let bytes = prepared
                .as_slice()
                .iter()
                .try_fold(journal.bytes, |bytes, frame| {
                    bytes.checked_add(frame.encoded.len() as u64)
                })
                .ok_or(StreamError::Capacity)?;
            let operations = journal
                .operations
                .checked_add(prepared.as_slice().len() as u64)
                .ok_or(StreamError::Capacity)?;
            if bytes > journal.limits.journal_bytes
                || operations > journal.limits.journal_operations
            {
                return Err(StreamError::Capacity);
            }
        }
        self.stream.persist_frames(prepared).await?;
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
