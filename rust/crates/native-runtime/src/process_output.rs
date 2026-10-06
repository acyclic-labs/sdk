//! Owned, interruptible readers for native process output.
//!
//! A process can outlive its direct parent while retaining an inherited pipe.
//! A plain blocking `Read` thread would then outlive the operation as well.
//! This module keeps the reader task joinable and gives the host adapter a
//! platform cancellation primitive before it joins the task.

#![allow(unsafe_code, reason = "native pipe and event APIs require raw handles")]

use std::{
    io,
    sync::mpsc::{Receiver, RecvTimeoutError},
    thread::JoinHandle,
    time::Duration,
};

/// One owned process-output reader.
///
/// The task is always joined before this value is dropped. Callers should
/// invoke [`OutputReader::cancel_and_join`] after terminating the process
/// tree when a bounded receive did not observe EOF.
pub struct OutputReader {
    receiver: Receiver<io::Result<Vec<u8>>>,
    cancel: Option<Box<dyn FnOnce() -> io::Result<()> + Send>>,
    handle: Option<JoinHandle<()>>,
    completed: bool,
}

impl OutputReader {
    /// Receives the completed output, waiting at most `timeout`.
    pub fn receive(&mut self, timeout: Duration) -> io::Result<Option<Vec<u8>>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => {
                self.completed = true;
                result.map(Some)
            }
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => {
                self.completed = true;
                Err(io::Error::other(
                    "process output reader disconnected without a result",
                ))
            }
        }
    }

    /// Cancels any pending native read and joins the reader task.
    pub fn cancel_and_join(&mut self) -> io::Result<Option<Vec<u8>>> {
        let cancellation = self.cancel.take().and_then(|cancel| cancel().err());
        let joined = self.join();
        joined?;
        let result = match self.receiver.recv() {
            Ok(result) => result.map(Some),
            Err(_) => Err(io::Error::other(
                "process output reader disconnected without a result",
            )),
        };
        if let Some(error) = cancellation {
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(None);
            }
            return Err(error);
        }
        if let Err(error) = &result {
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(None);
            }
        }
        result
    }

    /// Joins a reader that has already delivered its output.
    pub fn join(&mut self) -> io::Result<()> {
        let cancellation = if self.completed {
            // The reader has completed, so release the platform cancellation
            // handle before joining. This avoids sending a late wakeup to a
            // finished native thread when the task is subsequently dropped.
            self.cancel.take();
            None
        } else {
            // Joining an unread reader must wake the worker before joining.
            self.cancel.take().and_then(|cancel| cancel().err())
        };
        let joined = self.join_inner();
        joined?;
        if let Some(error) = cancellation {
            return Err(error);
        }
        Ok(())
    }

    fn join_inner(&mut self) -> io::Result<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle
            .join()
            .map_err(|_| io::Error::other("process output reader panicked"))
    }
}

impl Drop for OutputReader {
    fn drop(&mut self) {
        // The owner must never detach an OS read task. Normal call paths have
        // already terminated the process tree before dropping this value; the
        // cancellation is still attempted here for error and unwind paths.
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel();
        }
        let _ = self.join_inner();
    }
}

/// Starts an owned reader for a process pipe.
///
/// The callback runs on the reader task for every non-empty chunk. Returning
/// `false` stops collection, allowing a host adapter to record an output-limit
/// breach without duplicating reader orchestration here.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub fn spawn_output_reader<R, F>(reader: R, mut consume: F) -> io::Result<OutputReader>
where
    R: std::io::Read + std::os::fd::AsRawFd + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    unix::spawn(reader, move |chunk| consume(chunk))
}

/// Starts an owned reader for a native Windows process pipe.
///
/// Windows uses `PeekNamedPipe` before each native `ReadFile` call. This
/// intentionally accepts only a native pipe handle: arbitrary `Read`
/// implementations have no cancellation guarantee and therefore must not be
/// admitted to this path.
#[cfg(windows)]
pub fn spawn_output_reader<R, F>(reader: R, mut consume: F) -> io::Result<OutputReader>
where
    R: std::os::windows::io::AsRawHandle + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    windows::spawn(reader, move |chunk| consume(chunk))
}

/// Fallback for native targets without a platform cancellation primitive.
/// Refuse before creating a reader task; an uninterruptible native pipe is not
/// a supported execution capability.
#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
pub fn spawn_output_reader<R, F>(reader: R, mut consume: F) -> io::Result<OutputReader>
where
    R: std::io::Read + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    let _ = (reader, &mut consume);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "interruptible native output readers are unavailable on this platform",
    ))
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
mod unix {
    use super::OutputReader;
    #[cfg(target_os = "linux")]
    use std::os::fd::FromRawFd;
    use std::{
        io::{self, Read},
        os::fd::{AsRawFd, OwnedFd},
        sync::mpsc,
        thread,
    };

    pub(super) fn spawn<R, F>(reader: R, consume: F) -> io::Result<OutputReader>
    where
        R: Read + AsRawFd + Send + 'static,
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        let (cancel_read, cancel_write) = make_cancel_pipe()?;
        let cancel_read = std::sync::Arc::new(cancel_read);
        let worker_cancel_read = std::sync::Arc::clone(&cancel_read);
        let (sender, receiver) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("acyclic-output-reader".into())
            .spawn(move || {
                let result = run_reader_poll(reader, worker_cancel_read, consume);
                let _ = sender.send(result);
            })?;
        let cancel = Box::new(move || {
            let byte = [1_u8];
            loop {
                let written = unsafe {
                    libc::write(cancel_write.as_raw_fd(), byte.as_ptr().cast(), byte.len())
                };
                if written == 1 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                // Keep the read end alive through this closure so a failed
                // worker cannot turn cancellation into SIGPIPE.
                let _ = &cancel_read;
                return Err(error);
            }
        });
        Ok(OutputReader {
            receiver,
            cancel: Some(cancel),
            handle: Some(handle),
            completed: false,
        })
    }

    fn run_reader_poll<R, F>(
        mut reader: R,
        cancel: std::sync::Arc<OwnedFd>,
        mut consume: F,
    ) -> io::Result<Vec<u8>>
    where
        R: Read + AsRawFd,
        F: FnMut(&[u8]) -> bool,
    {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            wait_for_input(&reader, &cancel)?;
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                return Ok(bytes);
            }
            let chunk = buffer
                .get(..read)
                .ok_or_else(|| io::Error::other("process reader returned an invalid length"))?;
            if !consume(chunk) {
                return Ok(bytes);
            }
            bytes.extend_from_slice(chunk);
        }
    }

    fn wait_for_input<R: AsRawFd>(reader: &R, cancel: &OwnedFd) -> io::Result<()> {
        let mut descriptors = [
            libc::pollfd {
                fd: reader.as_raw_fd(),
                events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
            libc::pollfd {
                fd: cancel.as_raw_fd(),
                events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
        ];
        loop {
            let polled = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if polled < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if descriptors[1].revents != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "process output reader cancelled",
                ));
            }
            if descriptors[0].revents != 0 {
                return Ok(());
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn make_cancel_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
        let mut descriptors = [0; 2];
        if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pipe initialized both descriptors on success and ownership
        // is transferred exactly once to these OwnedFd values.
        Ok(unsafe {
            (
                OwnedFd::from_raw_fd(descriptors[0]),
                OwnedFd::from_raw_fd(descriptors[1]),
            )
        })
    }

    #[cfg(target_vendor = "apple")]
    fn make_cancel_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
        // Apple has no atomic CLOEXEC pipe primitive exposed by the current
        // SDK dependency. Refuse this execution capability rather than using
        // a racy `pipe` followed by `fcntl(FD_CLOEXEC)` sequence.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interruptible native output readers require an atomic CLOEXEC pipe on Apple",
        ))
    }
}

#[cfg(windows)]
mod windows {
    use super::OutputReader;
    use std::{
        io,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
    };
    use windows_sys::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{FILE_TYPE_PIPE, GetFileType, ReadFile},
        System::{
            Pipes::PeekNamedPipe,
            Threading::{CreateEventW, SetEvent, WaitForSingleObject},
        },
    };

    const WAIT_OBJECT_0: u32 = 0;
    const WAIT_TIMEOUT: u32 = 258;

    pub(super) fn spawn<R, F>(reader: R, consume: F) -> io::Result<OutputReader>
    where
        R: AsRawHandle + Send + 'static,
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        let reader_handle: HANDLE = reader.as_raw_handle().cast();
        if unsafe { GetFileType(reader_handle) } != FILE_TYPE_PIPE {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native output reader requires a pipe handle",
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateEventW returned an owned event handle transferred to
        // this OwnedHandle exactly once.
        // The worker and cancellation closure each retain the event until the
        // worker has joined; cancellation cannot close a handle still in use.
        let event = Arc::new(unsafe { OwnedHandle::from_raw_handle(event) });
        let worker_event = Arc::clone(&event);
        let worker_cancelled = Arc::clone(&cancelled);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sender, receiver) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("acyclic-output-reader".into())
            .spawn(move || {
                if ready_sender.send(()).is_err() {
                    return;
                }
                let result = run_reader(reader, worker_event, worker_cancelled, consume);
                let _ = sender.send(result);
            })?;
        if ready_receiver.recv().is_err() {
            let _ = handle.join();
            return Err(io::Error::other("process output reader failed to start"));
        }
        let cancel = Box::new(move || {
            cancelled.store(true, Ordering::Release);
            if unsafe { SetEvent(event.as_raw_handle().cast()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
        Ok(OutputReader {
            receiver,
            cancel: Some(cancel),
            handle: Some(handle),
            completed: false,
        })
    }

    fn run_reader<R, F>(
        reader: R,
        event: Arc<OwnedHandle>,
        cancelled: Arc<AtomicBool>,
        mut consume: F,
    ) -> io::Result<Vec<u8>>
    where
        R: AsRawHandle,
        F: FnMut(&[u8]) -> bool,
    {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "process output reader cancelled",
                ));
            }
            match peek_available(reader.as_raw_handle().cast(), &mut buffer)? {
                None => return Ok(bytes),
                Some(0) => {
                    let wait = unsafe { WaitForSingleObject(event.as_raw_handle().cast(), 50) };
                    if cancelled.load(Ordering::Acquire) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "process output reader cancelled",
                        ));
                    }
                    if wait != WAIT_OBJECT_0 && wait != WAIT_TIMEOUT {
                        return Err(if wait == u32::MAX {
                            io::Error::last_os_error()
                        } else {
                            io::Error::other(format!("unexpected output reader wait status {wait}"))
                        });
                    }
                }
                Some(available) => {
                    let Some(read) =
                        read_available(reader.as_raw_handle().cast(), &mut buffer, available)?
                    else {
                        return Ok(bytes);
                    };
                    if read == 0 {
                        return Ok(bytes);
                    }
                    let chunk = &buffer[..read];
                    if !consume(chunk) {
                        return Ok(bytes);
                    }
                    bytes.extend_from_slice(chunk);
                }
            }
        }
    }

    fn peek_available(handle: HANDLE, buffer: &mut [u8]) -> io::Result<Option<usize>> {
        let mut available = 0_u32;
        let mut read = 0_u32;
        let capacity = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
        let success = unsafe {
            PeekNamedPipe(
                handle,
                buffer.as_mut_ptr().cast(),
                capacity,
                &mut read,
                &mut available,
                std::ptr::null_mut(),
            )
        };
        if success == 0 {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(109 | 232 | 233)) {
                return Ok(None);
            }
            return Err(error);
        }
        Ok(Some(usize::try_from(read).unwrap_or(buffer.len())))
    }

    // PeekNamedPipe establishes an upper bound for this read. The native
    // handle is an owned process-pipe read end, so no other reader can consume
    // those bytes between the probe and ReadFile on the supported path.
    fn read_available(
        handle: HANDLE,
        buffer: &mut [u8],
        available: usize,
    ) -> io::Result<Option<usize>> {
        let requested = available.min(buffer.len());
        let requested = u32::try_from(requested).unwrap_or(u32::MAX);
        let mut read = 0_u32;
        let success = unsafe {
            ReadFile(
                handle,
                buffer.as_mut_ptr().cast(),
                requested,
                &mut read,
                std::ptr::null_mut(),
            )
        };
        if success == 0 {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(109 | 232 | 233)) {
                return Ok(None);
            }
            return Err(error);
        }
        Ok(Some(usize::try_from(read).unwrap_or(buffer.len())))
    }
}
#[cfg(all(test, any(target_os = "linux", target_vendor = "apple")))]
mod tests {
    use super::spawn_output_reader;
    use std::{fs::File, os::fd::FromRawFd, time::Duration};

    #[test]
    fn cancellation_joins_reader_when_pipe_writer_is_held() {
        let mut descriptors = [0; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        // SAFETY: both descriptors were initialized by pipe and ownership is
        // transferred exactly once to these files.
        let reader_file = unsafe { File::from_raw_fd(descriptors[0]) };
        // Keep the writer alive while the reader is cancelled. This models a
        // descendant retaining an inherited output pipe indefinitely.
        let writer_file = unsafe { File::from_raw_fd(descriptors[1]) };
        let mut reader = spawn_output_reader(reader_file, |_| true).expect("spawn reader");
        assert_eq!(
            reader.receive(Duration::from_millis(10)).expect("receive"),
            None
        );
        assert_eq!(reader.cancel_and_join().expect("cancel and join"), None);
        drop(writer_file);
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::spawn_output_reader;
    use std::{fs::File, io::Write, os::windows::io::FromRawHandle, sync::mpsc, time::Duration};
    use windows_sys::Win32::{Foundation::HANDLE, System::Pipes::CreatePipe};

    fn anonymous_pipe() -> (File, File) {
        let mut read_handle: HANDLE = std::ptr::null_mut();
        let mut write_handle: HANDLE = std::ptr::null_mut();
        assert_ne!(
            unsafe { CreatePipe(&mut read_handle, &mut write_handle, std::ptr::null(), 0,) },
            0
        );
        // SAFETY: CreatePipe initialized both handles and ownership is moved
        // exactly once into these values.
        let reader = unsafe { File::from_raw_handle(read_handle) };
        let writer = unsafe { File::from_raw_handle(write_handle) };
        (reader, writer)
    }

    #[test]
    fn cancellation_before_first_read_joins_reader() {
        let (reader_file, _writer_file) = anonymous_pipe();
        let mut reader = spawn_output_reader(reader_file, |_| true).expect("spawn reader");
        assert_eq!(reader.cancel_and_join().expect("cancel and join"), None);
    }

    #[test]
    fn join_before_receive_cancels_reader() {
        let (reader_file, _writer_file) = anonymous_pipe();
        let mut reader = spawn_output_reader(reader_file, |_| true).expect("spawn reader");
        reader.join().expect("join reader");
    }

    #[test]
    fn drop_cancels_reader_with_writer_alive() {
        let (reader_file, _writer_file) = anonymous_pipe();
        let reader = spawn_output_reader(reader_file, |_| true).expect("spawn reader");
        drop(reader);
    }

    #[test]
    fn cancellation_between_chunks_joins_reader() {
        let (reader_file, mut writer_file) = anonymous_pipe();
        let (seen_sender, seen_receiver) = mpsc::channel();
        let mut reader = spawn_output_reader(reader_file, move |_| {
            let _ = seen_sender.send(());
            true
        })
        .expect("spawn reader");
        writer_file.write_all(b"first").expect("write first chunk");
        seen_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("first output chunk");
        assert_eq!(reader.cancel_and_join().expect("cancel and join"), None);
    }

    #[test]
    fn native_pipe_read_consumes_peeked_bytes() {
        let (reader_file, mut writer_file) = anonymous_pipe();
        let mut reader = spawn_output_reader(reader_file, |_| true).expect("spawn reader");
        writer_file.write_all(b"first").expect("write output");
        drop(writer_file);
        let output = reader
            .receive(Duration::from_secs(5))
            .expect("receive")
            .expect("completed output");
        assert_eq!(output, b"first");
        reader.join().expect("join reader");
    }

    #[test]
    fn rejects_non_pipe_handle_before_reader_spawn() {
        let temporary = tempfile::NamedTempFile::new().expect("temporary file");
        let result = spawn_output_reader(temporary.reopen().expect("reopen file"), |_| true);
        match result {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::Unsupported),
            Ok(_) => panic!("regular files are not cancellable process pipes"),
        }
    }
}
