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

    pub(super) fn run(mut self, receiver: &mpsc::Receiver<OwnerJob>) {
        loop {
            let next = if !matches!(self.backend, Backend::Retiring(_)) {
                receiver
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
            } else {
                receiver.recv_timeout(Duration::from_millis(100))
            };
            let job = match next {
                Ok(job) => job,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.reap_retired();
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            self.reap_retired();
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
                bytes.extend_from_slice(&chunk[..received]);
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
        for write in chunk_owned_writes(writes) {
            let write = write?;
            if self.select()? == Selected::Positional {
                file.write_all_at(&write.bytes, write.offset)?;
                continue;
            }
            let result = self.finish(
                WriteAt::new(
                    SharedFd::new(Arc::clone(file)),
                    write.offset,
                    write.bytes.clone(),
                ),
                RetiredKey::Write,
            )?;
            let count = match result.into_parts().0 {
                Ok(count) => count,
                Err(error) => {
                    self.backend = Backend::Unprobed;
                    return Err(error);
                }
            };
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            if count > write.bytes.len() {
                return Err(io::Error::other("overlong native write"));
            }
            if count < write.bytes.len() {
                file.write_all_at(&write.bytes[count..], write.offset + count as u64)?;
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
        let deadline = std::time::Instant::now() + OWNER_IO_DEADLINE;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(self.retire(retired(key), io::ErrorKind::TimedOut.into()));
            }
            match driver.poll(Some(OWNER_POLL_SLICE)) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
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
