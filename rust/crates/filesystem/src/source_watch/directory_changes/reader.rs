use super::*;

pub(super) unsafe extern "system" fn reader_thread(parameter: *mut std::ffi::c_void) -> u32 {
    // SAFETY: successful CreateThread transfers exactly one Arc reference.
    let shared = unsafe { Arc::<Shared>::from_raw(parameter.cast()) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| shared.run()));
    if result.is_err() {
        shared.stopping.store(true, Ordering::Release);
        if shared.delivery.original.is_none() {
            let mut reads = shared.lock_reads();
            let mut work = shared.delivery.callback_work();
            let _ = shared.cancel_pending(&mut reads, &mut work);
        }
        let _ =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| shared.delivery.abandon()));
    }
    if shared.delivery.original.is_some() {
        shared.finish_native_reader();
    }
    u32::from(result.is_err())
}

impl Shared {
    fn run(&self) {
        {
            let mut reads = self.lock_reads();
            while !is_open(&self.completed) && !self.stopping.load(Ordering::Acquire) {
                reads = self
                    .reading_changed
                    .wait(reads)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        while !self.stopping.load(Ordering::Acquire) {
            // SAFETY: retirement signals this owned event before closing it.
            unsafe {
                WaitForSingleObject(HANDLE(self.completed.load(Ordering::Acquire)), INFINITE)
            };
            let mut reads = self.lock_reads();
            while reads.blocked && !self.stopping.load(Ordering::Acquire) {
                reads = self
                    .reading_changed
                    .wait(reads)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            let mut work = self.delivery.callback_work();
            if let Err(error) = self.drain(&mut reads, None, &mut work) {
                reads.blocked = true;
                self.delivery.failed_with_work(error, &mut work);
            }
        }
    }
    pub(super) fn issue<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let buffer = reads
            .buffer
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        *reads.overlapped = OVERLAPPED {
            hEvent: HANDLE(self.completed.load(Ordering::Acquire)),
            ..OVERLAPPED::default()
        };
        work.started_io(READ)?;
        // SAFETY: the read lock pins buffer and OVERLAPPED until completion.
        unsafe {
            ReadDirectoryChangesExW(
                HANDLE(self.directory.load(Ordering::Acquire)),
                buffer.as_mut_ptr().cast(),
                u32::try_from(BUFFER_BYTES).map_err(|_| DemandError::InvalidRequest)?,
                self.recursive,
                FILE_NOTIFY_CHANGE_FILE_NAME
                    | FILE_NOTIFY_CHANGE_DIR_NAME
                    | FILE_NOTIFY_CHANGE_ATTRIBUTES
                    | FILE_NOTIFY_CHANGE_SIZE
                    | FILE_NOTIFY_CHANGE_LAST_WRITE
                    | FILE_NOTIFY_CHANGE_CREATION
                    | FILE_NOTIFY_CHANGE_SECURITY,
                None,
                Some(&raw mut *reads.overlapped),
                None,
                if self.extended.load(Ordering::Acquire) {
                    ReadDirectoryNotifyExtendedInformation
                } else {
                    ReadDirectoryNotifyInformation
                },
            )
        }
        .map_err(io::Error::from)?;
        reads.pending = true;
        Ok(())
    }
    fn finish<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        wait: bool,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        if !reads.pending {
            return Ok(true);
        }
        if wait {
            work.record(READ)?;
        } else {
            work.admit(SOURCE)?;
            work.started_io(READ)?;
        }
        let mut transferred = 0;
        // SAFETY: the pending read's original storage remains pinned.
        let result = unsafe {
            GetOverlappedResult(
                HANDLE(self.directory.load(Ordering::Acquire)),
                &raw const *reads.overlapped,
                &mut transferred,
                wait,
            )
        };
        match result {
            Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult() => return Ok(false),
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => {}
            Err(error) if error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult() => reads.loss = true,
            Err(error) => {
                reads.pending = false;
                return Err(io::Error::from(error).into());
            }
            Ok(()) => {
                reads.filled = (transferred as usize).min(size_of::<[u64; BUFFER_WORDS]>());
                reads.cursor = 0;
                reads.loss |= transferred == 0 || u64::from(transferred) > BUFFER_BYTES;
                reads.pending = false;
                work.record(WorkCounters {
                    source_bytes_read: reads.filled as u64,
                    ..WorkCounters::UNCHARGED
                })?;
            }
        }
        reads.pending = false;
        Ok(true)
    }
    fn drain<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        deadline: Option<(Instant, Duration)>,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        loop {
            work.check()?;
            if self.stopping.load(Ordering::Acquire) || expired(deadline) {
                return Ok(false);
            }
            if reads.loss {
                self.delivery
                    .deliver_wide(&[HostChangeWide::Everything], work)?;
                reads.loss = false;
            }
            while reads.cursor < reads.filled {
                if self.stopping.load(Ordering::Acquire) || expired(deadline) {
                    return Ok(false);
                }
                work.started_io(ITEM)?;
                let buffer = reads
                    .buffer
                    .as_ref()
                    .ok_or(DemandError::NativeWorkUnavailable)?;
                // SAFETY: only completed, aligned bytes are borrowed under the read lock.
                let bytes = unsafe {
                    std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), reads.filled)
                };
                let Some((next, action, path, file)) = bytes
                    .get(reads.cursor..)
                    .and_then(|record| notification(record, self.extended.load(Ordering::Acquire)))
                else {
                    self.delivery
                        .deliver_wide(&[HostChangeWide::Everything], work)?;
                    reads.cursor = reads.filled;
                    break;
                };
                let paired = if action == 4 && next != 0 {
                    notification(
                        &bytes[reads.cursor + next..],
                        self.extended.load(Ordering::Acquire),
                    )
                    .filter(|(_, action, _, identity)| *action == 5 && *identity == file)
                } else {
                    None
                };
                let changes = [
                    HostChangeWide::Path { action, path },
                    HostChangeWide::File(file.unwrap_or(0)),
                ];
                let delivered = if file.is_some() {
                    changes.as_slice()
                } else {
                    std::slice::from_ref(&changes[0])
                };
                if let Some((_, _, to, _)) = paired {
                    work.started_io(ITEM)?;
                    let pair = [
                        HostChangeWide::Path { action, path },
                        HostChangeWide::Path {
                            action: 5,
                            path: to,
                        },
                        HostChangeWide::File(file.unwrap_or(0)),
                    ];
                    self.delivery
                        .deliver_wide(&pair[..if file.is_some() { 3 } else { 2 }], work)?;
                } else {
                    self.delivery.deliver_wide(delivered, work)?;
                }
                let consumed = paired.map_or(
                    next,
                    |(after, _, _, _)| {
                        if after == 0 { 0 } else { next + after }
                    },
                );
                reads.cursor = if consumed == 0 {
                    reads.filled
                } else {
                    reads.cursor + consumed
                };
            }
            if self.stopping.load(Ordering::Acquire) {
                return Ok(false);
            }
            if reads.pending {
                if !self.finish(reads, false, work)? {
                    return Ok(self.delivery.is_exact());
                }
            } else {
                let budget = work.with_original_leaf(|work| work.budget);
                if self.delivery.original.is_none() {
                    super::startup::reserve_cleanup(work, io_work(1, 1))?;
                }
                let issued = work
                    .admit(SOURCE)
                    .and_then(|_| self.issue(reads, work))
                    .and_then(|_| self.finish(reads, false, work));
                if self.delivery.original.is_none() {
                    work.with_original_leaf(|work| work.budget = budget);
                }
                if !issued? {
                    return Ok(self.delivery.is_exact());
                }
            }
        }
    }
}

impl NativeSourceWatch {
    #[cfg(test)]
    pub(crate) fn fence(&self) -> io::Result<()> {
        let mut work = self.shared.delivery.callback_work();
        self.fence_with_work(Duration::from_secs(10), &mut work)
            .map(|_| ())
            .map_err(crate::native_host::ordinary_error)
    }
    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self,
        timeout: Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if self.shared.stopping.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.shared.delivery.take_failure(work)?;
        if timeout.is_zero() {
            self.shared
                .delivery
                .deliver_wide(&[HostChangeWide::Everything], work)?;
            return Ok(false);
        }
        let started = Instant::now();
        let mut reads = self.shared.lock_reads();
        let result = (|| {
            if reads.pending {
                work.admit(io_work(1, 1).checked_add(SOURCE)?)?;
                work.started_io(WRITE)?;
                // SAFETY: the read lock retains this pending OVERLAPPED.
                let _ = unsafe {
                    CancelIoEx(
                        HANDLE(self.shared.directory.load(Ordering::Acquire)),
                        Some(&raw const *reads.overlapped),
                    )
                };
                self.shared.finish(&mut reads, true, work)?;
            }
            self.shared
                .drain(&mut reads, Some((started, timeout)), work)
        })();
        reads.blocked = !matches!(result, Ok(true));
        if !reads.blocked {
            self.shared.reading_changed.notify_all();
        }
        drop(reads);
        if matches!(result, Ok(false)) {
            self.shared
                .delivery
                .deliver_wide(&[HostChangeWide::Everything], work)?;
        }
        if matches!(result, Ok(true)) && !self.shared.complete.load(Ordering::Acquire) {
            self.shared
                .delivery
                .deliver_wide(&[HostChangeWide::Unconfirmed], work)?;
            return Ok(false);
        }
        result
    }
}
fn expired(deadline: Option<(Instant, Duration)>) -> bool {
    deadline.is_some_and(|(started, timeout)| started.elapsed() >= timeout)
}
pub(super) fn notification(
    record: &[u8],
    extended: bool,
) -> Option<(usize, u32, &[u16], Option<u64>)> {
    let (name_offset, length_offset) = if extended {
        (
            std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileNameLength),
        )
    } else {
        (
            std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileNameLength),
        )
    };
    let field = |at: usize| {
        record
            .get(at..at + 4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
    };
    let next = field(0)? as usize;
    let action = field(4)?;
    let length = field(length_offset)? as usize;
    let name = record.get(name_offset..name_offset.checked_add(length)?)?;
    if !length.is_multiple_of(2) || name.as_ptr().align_offset(std::mem::align_of::<u16>()) != 0 {
        return None;
    }
    let file = if extended {
        let at = std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileId);
        Some(u64::from_le_bytes(record.get(at..at + 8)?.try_into().ok()?))
    } else {
        None
    };
    if next != 0 && (next < name_offset + length || next > record.len()) {
        return None;
    }
    // SAFETY: the checked, aligned span contains exactly length / 2 UTF-16 units.
    Some((
        next,
        action,
        unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u16>(), length / 2) },
        file,
    ))
}
