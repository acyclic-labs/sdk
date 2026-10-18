//! A task file joined to its owner's canonical journal transaction.
use super::*;

pub(crate) struct JournalFile {
    pub(crate) condition: CommitCondition,
    pub(crate) path: StreamPath,
    pub(crate) record: acyclic_stream::OwnedBytes,
}

impl<P: StreamProvider> TaskVolume<P> {
    pub(crate) async fn prepare_journal_file(
        &self,
        key: FileKey,
        channel: Channel,
        payload: &[u8],
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<Option<JournalFile>> {
        self.prepare_chunk_file(key, channel, payload, false, io)
            .await
    }

    /// Reserve a name at the task's current chunk tail. Preparation rechecks it.
    pub(crate) async fn next_chunk_key(
        &self,
        task: Component,
        execution: crate::OperationId,
        turn: u64,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<FileKey> {
        self.refresh_journal(io).await?;
        // Lock order: index only, released before returning the reserved name.
        let index = self.index.lock().await;
        Ok(FileKey::Task {
            file: TaskFile::Chunk {
                sequence: next_sequence(&index, &task)?,
                execution,
                turn,
            },
            task,
        })
    }

    /// A peer's publication after naming is a conflict, never a reused sequence.
    pub(crate) async fn prepare_named_chunk(
        &self,
        key: FileKey,
        channel: Channel,
        payload: &[u8],
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<Option<JournalFile>> {
        self.prepare_chunk_file(key, channel, payload, true, io)
            .await
    }

    async fn prepare_chunk_file(
        &self,
        key: FileKey,
        channel: Channel,
        payload: &[u8],
        named: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<Option<JournalFile>> {
        if !matches!(
            key,
            FileKey::Task {
                file: TaskFile::Chunk { .. },
                ..
            }
        ) {
            return Err(Error::Unauthorized.into());
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if crate::test_support::accounted::chunk_scan_enabled() {
            let _ = self.snapshot(&Query::Root, 1024).await?;
        }
        self.refresh_journal(io).await?;
        // Lock order: index only across validation/encoding; no provider I/O.
        // The captured head fences the sequence choice against every peer write.
        let index = self.index.lock().await;
        if named
            && let FileKey::Task {
                task,
                file: TaskFile::Chunk { sequence, .. },
            } = &key
        {
            if *sequence != next_sequence(&index, task)? {
                return Err(Error::SequenceConflict(key).into());
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let original = io.and_then(crate::filesystem::PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let original = None;
        let (record, offset) =
            segment::encode_parts(&key, Some(channel), None, None, payload, original)
                .map_err(journal_error)?;
        let file = FileValue {
            key,
            channel: Some(channel),
            publication: None,
            requester: None,
            bytes: record.slice(offset..),
        };
        validate_file(&index, &file)?;
        Ok(Some(JournalFile {
            condition: CommitCondition::Head {
                path: self.path.clone(),
                expected: index.next,
                commit_id: index.commit,
            },
            path: self.path.clone(),
            record,
        }))
    }

    // Incrementally refresh the same authoritative index as ordinary file writes,
    // routing every provider read through the caller's original native ledger.
    async fn refresh_journal(
        &self,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<()> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        loop {
            let expected = {
                let index = self.index.lock().await;
                self.generation(&index)
            };
            let bounds =
                crate::store::operations::bounds_with_work(&self.provider, &self.path, &mut work)
                    .await?
                    .map_err(|e| crate::store::operations::stream_error_with_work(e, &work))?;
            if expected.position > bounds.tail {
                return Err(Error::ResetRequired.into());
            }
            let count =
                (bounds.tail - expected.position).min(acyclic_stream::MAX_ITEMS as u64 - 1) + 1;
            let mut records = crate::store::operations::read_records(
                &self.provider,
                &self.path,
                expected.position - 1,
                count as u32,
                &mut work,
            )
            .await?
            .map_err(|e| crate::store::operations::stream_error_with_work(e, &work))?;
            let anchor = records.next(&mut work).await?.ok_or(Error::ResetRequired)?;
            item_examined();
            if anchor.sequence != expected.position - 1
                || anchor.commit_id.as_bytes() != &expected.commit
            {
                return Err(Error::ResetRequired.into());
            }
            let mut expected = expected;
            while expected.position < bounds.tail {
                let Some(record) = records.next(&mut work).await? else {
                    break;
                };
                item_examined();
                if record.sequence != expected.position {
                    return Err(Error::ResetRequired.into());
                }
                let file = segment::decode_journal(&record.value, io).map_err(journal_error)?;
                let mut index = self.index.lock().await;
                if self.generation(&index) != expected {
                    break;
                }
                validate_file(&index, &file)?;
                let next = index.next.checked_add(1).ok_or(Error::LimitExceeded)?;
                apply(&mut index, file, next);
                index.next = next;
                index.commit = record.commit_id;
                expected = self.generation(&index);
            }
            let index = self.index.lock().await;
            if index.next >= bounds.tail {
                return Ok(());
            }
        }
    }
}

fn journal_error(error: Error) -> crate::Error {
    match error {
        Error::FileRead(error) => error,
        error => error.into(),
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "journal_tests.rs"]
mod tests;

// The per-task tail is rebuilt from published files, independently of execution
// IDs or per-turn ordinals. Naming and preparation use this same indexed lookup.
fn next_sequence(index: &Index, task: &Component) -> Result<u64> {
    item_examined();
    match index.task_chunk_tails.get(task) {
        Some(previous) => previous.checked_add(1).ok_or(Error::LimitExceeded),
        None => Ok(0),
    }
}
