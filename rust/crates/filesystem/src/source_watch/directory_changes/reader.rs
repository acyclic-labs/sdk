use super::*;

impl Shared {
    /// Serves one packet from the hub: a read completed, or a wake.
    pub(super) fn serve(self: Arc<Self>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.serve_packet()));
        if result.is_err() {
            self.stopping.store(true, Ordering::Release);
            if self.delivery.original.is_none() {
                let mut reads = self.lock_reads();
                let mut work = self.delivery.callback_work();
                let _ = self.cancel_pending(&mut reads, &mut work);
            }
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.delivery.abandon()));
            if self.delivery.original.is_some() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.retire_native(&mut self.lock_reads());
                }));
            }
        }
    }
    fn serve_packet(&self) {
        // A stale packet for a watch with no retirement left to resume.
        if self.stopping.load(Ordering::Acquire) && !self.resumes_retirement() {
            return;
        }
        // Never wait for the reads: their holder may be waiting for a
        // completion only this thread collects. It wakes the hub on release.
        self.missed.store(true, Ordering::Release);
        let Some(mut reads) = self.try_lock_reads() else {
            return;
        };
        self.missed.store(false, Ordering::Release);
        if self.stopping.load(Ordering::Acquire) {
            // Retirement that had to wait for a cancelled read goes on here.
            if self.delivery.original.is_some() {
                self.retire_native(&mut reads);
            } else if !self.stop_completed.load(Ordering::Acquire) {
                let mut work = self.delivery.callback_work();
                if let Err(error) = self.retire(&mut reads, &mut work) {
                    self.delivery.failed_with_work(error, &mut work);
                }
                if self.retired(&reads) {
                    self.stop_completed.store(true, Ordering::Release);
                }
            }
            return;
        }
        // A fence that left the reads blocked drains them itself.
        if reads.blocked {
            return;
        }
        let mut work = self.delivery.callback_work();
        if let Err(error) = self.drain(&mut reads, None, &mut work) {
            reads.blocked = true;
            self.delivery.failed_with_work(error, &mut work);
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
        let length = u32::try_from(size_of_val::<[std::mem::MaybeUninit<u64>]>(buffer))
            .map_err(|_| DemandError::InvalidRequest)?;
        // The hub learns of the completion from the port, not an event.
        *reads.overlapped = OVERLAPPED::default();
        work.started_io(READ)?;
        hub::retain_for_packet(self);
        // SAFETY: the read lock pins buffer and OVERLAPPED until completion,
        // and the packet owns the count just taken.
        let issued = unsafe {
            ReadDirectoryChangesExW(
                HANDLE(self.directory.load(Ordering::Acquire)),
                buffer.as_mut_ptr().cast(),
                length,
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
        };
        if let Err(error) = issued {
            // A read that fails at once queues no packet.
            hub::release_unqueued(self);
            return Err(io::Error::from(error).into());
        }
        reads.pending = true;
        Ok(())
    }
    /// Whether a packet for this stopping watch may have retirement to go
    /// on with: a native owner needs a credit, or its owner's drop.
    fn resumes_retirement(&self) -> bool {
        if self.stop_completed.load(Ordering::Acquire) {
            return false;
        }
        self.delivery.original.is_none()
            || (!self.parked.load(Ordering::Acquire)
                && (self.dropped.load(Ordering::Acquire) || self.cleanup_pending()))
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
        let capacity = reads.capacity();
        if wait {
            if !self.await_completion(reads) {
                return Ok(false);
            }
            work.record(READ)?;
        } else {
            work.admit(source(capacity))?;
            work.started_io(READ)?;
        }
        let mut transferred = 0;
        // SAFETY: the pending read's original storage remains pinned.
        let result = unsafe {
            GetOverlappedResult(
                HANDLE(self.directory.load(Ordering::Acquire)),
                &raw const *reads.overlapped,
                &mut transferred,
                false,
            )
        };
        if !matches!(&result, Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult()) {
            reads.cancelling = false;
        }
        match result {
            Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult() => return Ok(false),
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => {}
            Err(error) if error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult() => reads.loss = true,
            Err(error) => {
                reads.pending = false;
                return Err(io::Error::from(error).into());
            }
            Ok(()) => {
                // No bytes from a completed read: the kernel dropped changes
                // that overflowed its buffer.
                reads.filled = usize::try_from(u64::from(transferred).min(capacity))
                    .map_err(|_| DemandError::InvalidRequest)?;
                reads.cursor = 0;
                reads.loss |= transferred == 0 || u64::from(transferred) > capacity;
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
                // A larger buffer reads on a fresh handle before the rescan
                // is reported, so no change escapes both.
                if !reads.pending && self.grow(reads, work)? {
                    self.reissue(reads, work)?;
                }
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
            let completed = if reads.pending {
                self.finish(reads, false, work)?
            } else {
                self.reissue(reads, work)?
            };
            if !completed {
                if deadline.is_none() {
                    return Ok(self.delivery.is_exact());
                }
                // A fence: the read may already hold changes the hub has
                // not collected. Cancelling collects them now.
                self.collect(reads, work)?;
                if reads.cursor >= reads.filled && !reads.loss && !self.reissue(reads, work)? {
                    // Nothing was waiting; the fresh read stays pending.
                    return Ok(self.delivery.is_exact());
                }
            }
        }
    }
    /// Cancels the pending read and collects its completion: its changes,
    /// or its cancellation.
    fn collect<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if !reads.pending {
            return Ok(());
        }
        work.admit(io_work(1, 1).checked_add(source(reads.capacity()))?)?;
        work.started_io(WRITE)?;
        // SAFETY: the read lock retains this pending OVERLAPPED.
        let _ = unsafe {
            CancelIoEx(
                HANDLE(self.directory.load(Ordering::Acquire)),
                Some(&raw const *reads.overlapped),
            )
        };
        if !self.finish(reads, true, work)? {
            return Err(DemandError::WorkerUnavailable);
        }
        Ok(())
    }
    /// Issues the next read and collects it if it already completed.
    fn reissue<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let budget = work.with_original_leaf(|work| work.budget);
        if self.delivery.original.is_none() {
            super::startup::reserve_cleanup(work, io_work(1, 1))?;
        }
        let issued = work
            .admit(source(reads.capacity()))
            .and_then(|_| self.issue(reads, work))
            .and_then(|_| self.finish(reads, false, work));
        if self.delivery.original.is_none() {
            work.with_original_leaf(|work| work.budget = budget);
        }
        issued
    }
    /// After the kernel dropped changes, doubles the buffer, up to
    /// `MAXIMUM_BUFFER_BYTES`, on a fresh handle to the same directory: the
    /// first read on a handle fixes the kernel's buffer for that handle's
    /// lifetime. Answers whether it grew; a watch whose growth is not
    /// admitted keeps its buffer, and the rescan still covers the loss.
    fn grow<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let capacity = reads.capacity();
        let next = capacity.saturating_mul(2).min(self.maximum_buffer_bytes);
        if next <= capacity || !is_open(&self.directory) {
            return Ok(false);
        }
        // The open, its attachment and the old handle's close. The new
        // handle's own close is the cleanup already reserved for the old.
        let admitted = work.admit(io_work(1, 2)).and_then(|()| {
            self.ownership
                .claim_retained(next, WorkCounters::UNCHARGED, work)
        });
        match admitted {
            Err(DemandError::Work(_)) => return Ok(false),
            admitted => admitted?,
        }
        // SAFETY: the read lock keeps the open handle installed throughout.
        let current = unsafe {
            std::os::windows::io::BorrowedHandle::borrow_raw(self.directory.load(Ordering::Acquire))
        };
        let opened = super::startup::open(current, self.pin_name, work).and_then(|handle| {
            work.started_io(WRITE)?;
            hub::attach(self, HANDLE(handle.as_raw_handle()))?;
            Ok(handle)
        });
        let handle = match opened {
            Ok(handle) => handle,
            Err(error) => {
                self.ownership.retire(next)?;
                if !self.ownership.has_native_custody() {
                    work.release(next)?;
                }
                return Err(error);
            }
        };
        work.started_io(WRITE)?;
        let previous = self
            .directory
            .swap(handle.into_raw_handle(), Ordering::AcqRel);
        reads.buffer = Some(buffer(next));
        reads.filled = 0;
        reads.cursor = 0;
        self.buffer_bytes.store(next, Ordering::Release);
        // The old buffer's bytes leave with it; the new buffer's stay owned.
        self.ownership.retire(capacity)?;
        if !self.ownership.has_native_custody() {
            work.release(next)?;
        }
        // SAFETY: no read is pending on the replaced handle, which nothing
        // else names.
        unsafe { CloseHandle(HANDLE(previous)) }.map_err(io::Error::from)?;
        Ok(true)
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
        // The hub collects every completion, so it cannot wait for one; a
        // fence from its callbacks reports everything instead.
        if timeout.is_zero() || hub::serving() {
            self.shared
                .delivery
                .deliver_wide(&[HostChangeWide::Everything], work)?;
            return Ok(false);
        }
        let started = Instant::now();
        let mut reads = self.shared.lock_reads();
        let result = self.shared.collect(&mut reads, work).and_then(|()| {
            self.shared
                .drain(&mut reads, Some((started, timeout)), work)
        });
        reads.blocked = !matches!(result, Ok(true));
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
