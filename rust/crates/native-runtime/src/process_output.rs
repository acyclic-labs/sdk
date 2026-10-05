//! Owned, interruptible readers for native process output.
//!
//! A process can outlive its direct parent while retaining an inherited pipe.
//! A plain blocking `Read` thread would then outlive the operation as well.
//! This module keeps the reader task joinable and gives the host adapter a
//! platform cancellation primitive before it joins the task.

use std::{
    io::{self, Read},
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender},
    thread::{self, JoinHandle},
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
}

impl OutputReader {
    /// Receives the completed output, waiting at most `timeout`.
    pub fn receive(&self, timeout: Duration) -> io::Result<Option<Vec<u8>>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => result.map(Some),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(io::Error::other(
                "process output reader disconnected without a result",
            )),
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
        // The reader has completed, so release the platform cancellation
        // handle before joining. This avoids sending a late wakeup to a
        // finished native thread when the task is subsequently dropped.
        self.cancel.take();
        self.join_inner()
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
    R: Read + std::os::fd::AsRawFd + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    unix::spawn(reader, move |chunk| consume(chunk))
}

/// Starts an owned reader for a process pipe.
#[cfg(windows)]
pub fn spawn_output_reader<R, F>(reader: R, mut consume: F) -> io::Result<OutputReader>
where
    R: Read + std::os::windows::io::AsRawHandle + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    windows::spawn(reader, move |chunk| consume(chunk))
}

/// Fallback for native targets without a platform cancellation primitive.
/// The task remains owned and joined; process providers should still supply
/// an execution boundary that closes inherited pipes before cancellation.
#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
pub fn spawn_output_reader<R, F>(reader: R, mut consume: F) -> io::Result<OutputReader>
where
    R: Read + Send + 'static,
    F: FnMut(&[u8]) -> bool + Send + 'static,
{
    generic::spawn(reader, move |chunk| consume(chunk))
}

fn run_reader<R, F>(mut reader: R, sender: Sender<io::Result<Vec<u8>>>, mut consume: F)
where
    R: Read,
    F: FnMut(&[u8]) -> bool,
{
    let result = (|| {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let chunk = buffer
                .get(..read)
                .ok_or_else(|| io::Error::other("process reader returned an invalid length"))?;
            if !consume(chunk) {
                break;
            }
            bytes.extend_from_slice(chunk);
        }
        Ok(bytes)
    })();
    let _ = sender.send(result);
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
mod unix {
    use super::OutputReader;
    use std::{
        io::{self, Read},
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        sync::mpsc,
        thread,
    };

    pub(super) fn spawn<R, F>(reader: R, consume: F) -> io::Result<OutputReader>
    where
        R: Read + AsRawFd + Send + 'static,
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        let (cancel_read, cancel_write) = make_cancel_pipe()?;
        let (sender, receiver) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("acyclic-output-reader".into())
            .spawn(move || {
                let result = run_reader_poll(reader, cancel_read, consume);
                let _ = sender.send(result);
            })?;
        let cancel = Box::new(move || {
            let byte = [1_u8];
            let written =
                unsafe { libc::write(cancel_write.as_raw_fd(), byte.as_ptr().cast(), byte.len()) };
            if written == 1 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        Ok(OutputReader {
            receiver,
            cancel: Some(cancel),
            handle: Some(handle),
        })
    }

    fn run_reader_poll<R, F>(mut reader: R, cancel: OwnedFd, mut consume: F) -> io::Result<Vec<u8>>
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

    fn make_cancel_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
        let mut descriptors = [0; 2];
        if unsafe { libc::pipe(descriptors.as_mut_ptr()) } != 0 {
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
}

#[cfg(windows)]
mod windows {
    use super::{OutputReader, run_reader};
    use std::{
        io::{self, Read},
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        sync::mpsc,
        thread,
    };
    use windows_sys::Win32::System::Threading::{
        CancelSynchronousIo, GetCurrentThreadId, OpenThread, THREAD_TERMINATE,
    };

    pub(super) fn spawn<R, F>(reader: R, consume: F) -> io::Result<OutputReader>
    where
        R: Read + AsRawHandle + Send + 'static,
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sender, receiver) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("acyclic-output-reader".into())
            .spawn(move || {
                let thread_handle =
                    unsafe { OpenThread(THREAD_TERMINATE, 0, GetCurrentThreadId()) };
                if thread_handle.is_null() {
                    let _ = ready_sender.send(Err(io::Error::last_os_error()));
                    return;
                }
                // SAFETY: OpenThread returned an owned handle that is closed
                // by OwnedHandle after the cancellation closure consumes it.
                let thread_handle = unsafe { OwnedHandle::from_raw_handle(thread_handle) };
                if ready_sender.send(Ok(thread_handle)).is_err() {
                    return;
                }
                run_reader(reader, sender, consume);
            })?;
        let thread_handle = match ready_receiver.recv() {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => {
                let _ = handle.join();
                return Err(error);
            }
            Err(_) => {
                let _ = handle.join();
                return Err(io::Error::other("process output reader failed to start"));
            }
        };
        let cancel = Box::new(move || {
            // SAFETY: the handle is owned and names exactly the reader thread.
            if unsafe { CancelSynchronousIo(thread_handle.as_raw_handle().cast()) } != 0 {
                Ok(())
            } else {
                let error = io::Error::last_os_error();
                // There is no pending synchronous request when the reader has
                // already completed. The subsequent join remains authoritative.
                if error.raw_os_error() == Some(1168) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        });
        Ok(OutputReader {
            receiver,
            cancel: Some(cancel),
            handle: Some(handle),
        })
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
mod generic {
    use super::{OutputReader, run_reader};
    use std::{io::Read, sync::mpsc, thread};

    pub(super) fn spawn<R, F>(reader: R, consume: F) -> std::io::Result<OutputReader>
    where
        R: Read + Send + 'static,
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("acyclic-output-reader".into())
            .spawn(move || run_reader(reader, sender, consume))?;
        Ok(OutputReader {
            receiver,
            cancel: None,
            handle: Some(handle),
        })
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
