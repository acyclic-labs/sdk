use super::retired::{RetiredKey, RetiredProactor};
use super::*;

// Only this module can change the owner's backend. A disabled ring cannot
// retain a live driver; a retiring driver cannot accept new ring operations.
enum Backend {
    Unprobed,
    Ring(Proactor),
    Positional,
    Retiring(RetiredProactor),
}

#[derive(Debug, PartialEq, Eq)]
enum Selected {
    Ring,
    Positional,
}

pub(super) struct Owner {
    backend: Backend,
}

impl Owner {
    pub(super) fn new() -> Self {
        Self {
            backend: Backend::Unprobed,
        }
    }

    pub(super) fn run(mut self, receiver: &mpsc::Receiver<OwnerJob>, wake: &OwnerWake) {
        loop {
            self.reap_retired();
            let job = if let Backend::Retiring(retired) = &mut self.backend {
                // Publish the ring wake before checking the mailbox: a send
                // either precedes this check or interrupts the kernel wait.
                *wake
                    .driver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retired.waker());
                wake.retiring.store(true, Ordering::SeqCst);
                match receiver.try_recv() {
                    Ok(job) => job,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                    Err(mpsc::TryRecvError::Empty) => {
                        if let Err(error) = retired.wait_event()
                            && error.kind() != io::ErrorKind::Interrupted
                        {
                            // A failed completion source cannot release a
                            // buffer that may still be owned by the kernel.
                            tracing::error!(%error, "native completion source failed during retirement");
                            std::process::abort();
                        }
                        continue;
                    }
                }
            } else {
                wake.retiring.store(false, Ordering::SeqCst);
                wake.driver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match receiver.recv() {
                    Ok(job) => job,
                    Err(_) => break,
                }
            };
            match job {
                OwnerJob::Read(file, reads, finish) => finish(self.read_batch(&file, &reads)),
                OwnerJob::Write(file, writes, finish) => finish(self.write_batch(&file, writes)),
                OwnerJob::Stop => break,
            }
        }
        if matches!(self.backend, Backend::Retiring(_)) {
            std::process::abort();
        }
    }

    fn reap_retired(&mut self) {
        if let Backend::Retiring(retired) = &mut self.backend
            && retired.poll_terminal()
        {
            self.backend = Backend::Unprobed;
        }
    }

    fn select(&mut self) -> io::Result<Selected> {
        self.select_with(Proactor::new)
    }

    fn select_with(
        &mut self,
        probe: impl FnOnce() -> io::Result<Proactor>,
    ) -> io::Result<Selected> {
        if matches!(self.backend, Backend::Unprobed) {
            self.backend = match probe() {
                Ok(driver) => Backend::Ring(driver),
                Err(error) if ring_unavailable(&error) => Backend::Positional,
                Err(error) => return Err(error),
            };
        }
        match self.backend {
            Backend::Ring(_) => Ok(Selected::Ring),
            Backend::Positional | Backend::Retiring(_) => Ok(Selected::Positional),
            Backend::Unprobed => unreachable!("probe selected a backend or returned its error"),
        }
    }

    fn retire(&mut self, key: RetiredKey, error: io::Error) -> io::Error {
        let Backend::Ring(driver) = std::mem::replace(&mut self.backend, Backend::Unprobed) else {
            std::process::abort();
        };
        self.backend = Backend::Retiring(RetiredProactor::new(driver, key));
        uncertain_completion(error)
    }

    fn read_batch(
        &mut self,
        file: &Arc<NativeFilePayload>,
        reads: &[OwnedRead],
    ) -> io::Result<Vec<Bytes>> {
        reads
            .iter()
            .map(|read| self.read_one(file, *read))
            .collect()
    }

    fn read_one(&mut self, file: &Arc<NativeFilePayload>, read: OwnedRead) -> io::Result<Bytes> {
        let mut bytes = Vec::new();
        let mut count = 0;
        while count < read.length {
            let offset = read.offset + count as u64;
            if self.select()? == Selected::Positional {
                let length = (read.length - count).min(MAX_IO_BYTES);
                let mut chunk = vec![0; length];
                let received = file.read_at(&mut chunk, offset)?;
                let filled = chunk
                    .get(..received)
                    .ok_or_else(|| io::Error::other("overlong native read"))?;
                bytes.extend_from_slice(filled);
                count += received;
                if received < length {
                    break;
                }
                continue;
            }
            let length = (read.length - count).min(MAX_IO_BYTES * IO_ENTRIES);
            let operation = ReadAt::new(
                SharedFd::new(Arc::clone(file)),
                offset,
                compio_buf::IoBuf::slice(Vec::with_capacity(length), ..length),
            );
            let completion = self.finish(operation, RetiredKey::Read)?;
            let (received, operation) = completion.into_parts();
            let received = match received {
                Ok(count) => count,
                Err(error) => {
                    self.backend = Backend::Unprobed;
                    return Err(error);
                }
            };
            if received > length {
                return Err(io::Error::other("overlong native read"));
            }
            let mut buffer = operation.into_inner();
            // SAFETY: terminal completion initialized the bounded reported prefix.
            unsafe {
                buffer.set_len(received);
            }
            let chunk = Bytes::from(buffer.into_inner());
            count += received;
            let eof = received < length;
            if bytes.is_empty() && (eof || count == read.length) {
                return Ok(chunk);
            }
            bytes.extend_from_slice(&chunk);
            if eof {
                break;
            }
        }
        Ok(Bytes::from(bytes))
    }

    fn write_batch(
        &mut self,
        file: &Arc<NativeFilePayload>,
        writes: Vec<OwnedWrite>,
    ) -> io::Result<()> {
        let mut writes = chunk_owned_writes(writes).peekable();
        while let Some(first) = writes.next() {
            let first = first?;
            let mut buffers = std::array::from_fn(|_| Bytes::new());
            let mut length = first.bytes.len();
            let mut entries = 1;
            *buffers.first_mut().ok_or(io::ErrorKind::InvalidInput)? = first.bytes;
            while entries < IO_ENTRIES {
                let Some(Ok(next)) = writes.peek() else { break };
                if next.offset != first.offset + length as u64
                    || length + next.bytes.len() > MAX_IO_BYTES
                {
                    break;
                }
                let Some(next) = writes.next() else { break };
                let next = next?;
                length += next.bytes.len();
                *buffers
                    .get_mut(entries)
                    .ok_or(io::ErrorKind::InvalidInput)? = next.bytes;
                entries += 1;
            }
            self.write_contiguous(file, first.offset, buffers, length)?;
        }
        Ok(())
    }

    // One vectored operation owns every buffer through its terminal. Unlike
    // independent submissions, a short write is one ordered prefix, preserving
    // append-only journal recovery. Gaps and bounded runs finish in input order.
    fn write_contiguous(
        &mut self,
        file: &Arc<NativeFilePayload>,
        mut offset: u64,
        buffers: [Bytes; IO_ENTRIES],
        length: usize,
    ) -> io::Result<()> {
        let (mut skip, buffers) = if self.select()? == Selected::Positional {
            (0, buffers)
        } else {
            let result = self.finish(
                WriteVectoredAt::new(SharedFd::new(Arc::clone(file)), offset, buffers),
                RetiredKey::Write,
            )?;
            let (result, operation) = result.into_parts();
            let count = result.inspect_err(|_| self.backend = Backend::Unprobed)?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            if count > length {
                return Err(io::Error::other("overlong native vectored write"));
            }
            (count, operation.into_inner())
        };
        // The operation returned its original buffers after completion. Finish
        // a short prefix with the existing positional write-all mechanism.
        for bytes in buffers {
            let covered = skip.min(bytes.len());
            offset += covered as u64;
            skip -= covered;
            let rest = bytes.get(covered..).ok_or(io::ErrorKind::InvalidData)?;
            if !rest.is_empty() {
                file.write_all_at(rest, offset)?;
                offset += rest.len() as u64;
            }
        }
        Ok(())
    }

    fn finish<O: compio_driver::OpCode + 'static>(
        &mut self,
        operation: O,
        retired: fn(Key<O>) -> RetiredKey,
    ) -> io::Result<compio_buf::BufResult<usize, O>> {
        let mut fence = PendingUnwind(true);
        let Backend::Ring(driver) = &mut self.backend else {
            std::process::abort();
        };
        let mut key = match driver.push(operation) {
            PushEntry::Ready(result) => {
                fence.0 = false;
                return Ok(result);
            }
            PushEntry::Pending(key) => key,
        };
        loop {
            match driver.poll(None) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(self.retire(retired(key), error)),
            }
            match driver.pop(key) {
                PushEntry::Pending(pending) => key = pending,
                PushEntry::Ready(result) => {
                    fence.0 = false;
                    return Ok(result);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
