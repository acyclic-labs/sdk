use super::*;
use crate::usn_records::{PAGE_BYTES, UsnPage, WindowsUsnRecord};
use crate::{NativeMaterializationWork, WorkCounters};
use windows::Win32::System::Ioctl::{
    FSCTL_READ_UNPRIVILEGED_USN_JOURNAL, FSCTL_READ_USN_JOURNAL, READ_USN_JOURNAL_DATA_V1,
};

/// Bounded replay from a saved cursor to a fixed journal fence.
///
/// The held root and volume handles prevent path retargeting. No file contents
/// are opened. Records outside the selected root must be filtered by the
/// consumer using their parent identities; nameless records require identity
/// resolution and cannot be silently discarded.
pub struct WindowsUsnReplay {
    _root: HostRoot,
    volume: std::fs::File,
    checkpoint: WindowsUsnCheckpoint,
    fence: u64,
}

/// Progress after one fully accepted journal page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowsUsnReplayProgress {
    /// First unapplied USN. Persist this in the same transaction as the deltas.
    pub checkpoint: WindowsUsnCheckpoint,
    /// Every record before the fence captured at open has been accepted.
    pub caught_up: bool,
    /// Records accepted from this bounded page.
    pub records: u64,
}

impl WindowsUsnReplay {
    /// Opens the exact checkpoint root and captures a fixed replay fence.
    ///
    /// # Errors
    /// Rejects root/journal changes, truncation, unavailable journals, and work
    /// denied by the original native invocation.
    pub fn open(
        root: &Path,
        checkpoint: WindowsUsnCheckpoint,
        work: &mut NativeMaterializationWork,
    ) -> Result<Self, WindowsUsnError> {
        Self::open_with_work(root, checkpoint, work)
    }

    fn open_with_work<W: SourceDemandReadWork>(
        root: &Path,
        checkpoint: WindowsUsnCheckpoint,
        work: &mut W,
    ) -> Result<Self, WindowsUsnError> {
        work.check()?;
        let root = HostRoot::open(root, work)?;
        let volume = open_journal_volume(&root, work)?;
        let journal = query_journal_file(&volume, work)?;
        match classify_continuity(root.identity(), checkpoint, &journal) {
            WindowsUsnContinuity::Unchanged
            | WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced) => {}
            WindowsUsnContinuity::BaselineRequired(reason) => {
                return Err(WindowsUsnError::Discontinuous(reason));
            }
        }
        Ok(Self {
            _root: root,
            volume,
            checkpoint,
            fence: journal.next_usn,
        })
    }

    /// Returns the last completely accepted cursor, including after failure.
    #[must_use]
    pub const fn checkpoint(&self) -> WindowsUsnCheckpoint {
        self.checkpoint
    }

    /// Reads and accepts at most one 8 KiB native page without waiting for
    /// future records. The entire page is validated before any callback runs.
    ///
    /// Applications are at least once: a failed callback leaves the cursor
    /// unchanged, so previously accepted records in that page can be repeated.
    /// The consumer must apply them idempotently and publish the returned
    /// cursor atomically with its index changes.
    ///
    /// # Errors
    /// Returns native, malformed-page, discontinuity, work, or callback errors.
    /// Failure never advances the saved cursor.
    pub fn next_page(
        &mut self,
        work: &mut NativeMaterializationWork,
        accept: impl FnMut(
            WindowsUsnRecord<'_>,
            &mut NativeMaterializationWork,
        ) -> Result<(), WindowsUsnError>,
    ) -> Result<WindowsUsnReplayProgress, WindowsUsnError> {
        self.next_page_with_work(work, accept)
    }

    fn next_page_with_work<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
        accept: impl FnMut(WindowsUsnRecord<'_>, &mut W) -> Result<(), WindowsUsnError>,
    ) -> Result<WindowsUsnReplayProgress, WindowsUsnError> {
        work.check()?;
        if self.checkpoint.next_usn == self.fence {
            return Ok(WindowsUsnReplayProgress {
                checkpoint: self.checkpoint,
                caught_up: true,
                records: 0,
            });
        }
        let request = READ_USN_JOURNAL_DATA_V1 {
            StartUsn: i64::try_from(self.checkpoint.next_usn)
                .map_err(|_| WindowsUsnError::InvalidCheckpoint)?,
            ReasonMask: u32::MAX,
            ReturnOnlyOnClose: 0,
            Timeout: 0,
            BytesToWaitFor: 0,
            UsnJournalID: self.checkpoint.journal_id,
            MinMajorVersion: 2,
            MaxMajorVersion: 3,
        };
        let mut buffer = [0; PAGE_BYTES];
        let mut returned = 0;
        let mut control = FSCTL_READ_UNPRIVILEGED_USN_JOURNAL;
        loop {
            work.admit(WorkCounters {
                source_bytes_read: PAGE_BYTES as u64,
                ..WorkCounters::UNCHARGED
            })?;
            work.started_io(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            // SAFETY: the held volume, immutable request, initialized fixed
            // output buffer, and byte-count pointer outlive this synchronous
            // call; no overlapped operation escapes it.
            let result = unsafe {
                DeviceIoControl(
                    HANDLE(self.volume.as_raw_handle()),
                    control,
                    Some((&raw const request).cast()),
                    size_of::<READ_USN_JOURNAL_DATA_V1>() as u32,
                    Some(buffer.as_mut_ptr().cast()),
                    PAGE_BYTES as u32,
                    Some(&raw mut returned),
                    None,
                )
            };
            if returned as usize > PAGE_BYTES {
                return Err(WindowsUsnError::InvalidJournal);
            }
            work.record(WorkCounters {
                source_bytes_read: u64::from(returned),
                ..WorkCounters::UNCHARGED
            })?;
            match result {
                Ok(()) => break,
                Err(error) => {
                    let code = error.code().0 as u32 & 0xffff;
                    match code {
                        1 | 50 | 87 if control == FSCTL_READ_UNPRIVILEGED_USN_JOURNAL => {
                            control = FSCTL_READ_USN_JOURNAL;
                            returned = 0;
                        }
                        1181 => {
                            return Err(WindowsUsnError::Discontinuous(
                                WindowsUsnDiscontinuity::JournalTruncated,
                            ));
                        }
                        1178 | 1179 => {
                            return Err(WindowsUsnError::Discontinuous(
                                WindowsUsnDiscontinuity::JournalChanged,
                            ));
                        }
                        _ => return Err(WindowsUsnError::Native(error.to_string())),
                    }
                }
            }
        }
        let page = UsnPage::validate(&buffer[..returned as usize], self.checkpoint.next_usn, work)
            .map_err(page_error)?;
        let records = page.apply(&mut self.checkpoint.next_usn, self.fence, work, accept)?;
        Ok(WindowsUsnReplayProgress {
            checkpoint: self.checkpoint,
            caught_up: self.checkpoint.next_usn == self.fence,
            records,
        })
    }
}

fn page_error(error: crate::demand::DemandError) -> WindowsUsnError {
    match error {
        crate::demand::DemandError::InvalidRequest => WindowsUsnError::InvalidJournal,
        error => WindowsUsnError::Demand(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]

    fn live_usn_replay_is_bounded_and_resumes_only_after_acceptance() {
        let root = tempfile::tempdir().unwrap();
        let stop = crate::CancellationToken::new();
        let mut work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &stop).unwrap();
        let checkpoint = capture_windows_usn_checkpoint(root.path(), &mut work).unwrap();
        let file = root.path().join("before");
        std::fs::write(&file, b"journal replay").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap()
            .sync_all()
            .unwrap();
        std::fs::rename(file, root.path().join("after")).unwrap();
        let mut replay = WindowsUsnReplay::open(root.path(), checkpoint, &mut work).unwrap();
        let failure = replay.next_page(&mut work, |_, _| Err(WindowsUsnError::InvalidJournal));
        assert!(failure.is_err());
        assert_eq!(replay.checkpoint(), checkpoint);
        let mut records = 0;
        let mut own_reasons = 0;
        let start = std::time::Instant::now();
        loop {
            let before = work.receipt();
            let progress = replay
                .next_page(&mut work, |record, _| {
                    // NTFS directory identities are the lower 64-bit file reference.
                    if record.parent_id[..8] == checkpoint.root_identity.to_bytes()[8..] {
                        own_reasons |= record.reason;
                    }
                    records += 1;
                    Ok(())
                })
                .unwrap();
            assert!(progress.records <= 127);
            assert!(
                work.receipt().source_bytes_read - before.source_bytes_read <= PAGE_BYTES as u64
            );
            assert_eq!(replay.checkpoint(), progress.checkpoint);
            if progress.caught_up {
                break;
            }
            assert!(start.elapsed() < std::time::Duration::from_secs(3));
        }
        assert!(records > 0);
        assert_ne!(
            own_reasons & 0x100,
            0,
            "creation is replayed by parent identity even if nameless"
        );
        assert_ne!(own_reasons & 0x1000, 0, "old rename binding is retained");
        assert_ne!(own_reasons & 0x2000, 0, "new rename binding is retained");
        let before = work.receipt();
        let done = replay
            .next_page(&mut work, |_, _| {
                panic!("completed fence must not apply records")
            })
            .unwrap();
        assert!(done.caught_up);
        assert_eq!(done.records, 0);
        assert_eq!(
            before,
            work.receipt(),
            "completed replay performs no native reads"
        );
    }
}
