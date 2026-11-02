//! The original credited input reader and its independent kernel terminal.

use super::{INPUT_EXITED, OPENED, Opened, wait::Producer};
use std::io;
use std::os::windows::io::BorrowedHandle;
use std::sync::mpsc;
use windows_sys::Win32::Foundation::{ERROR_INVALID_DATA, HANDLE};
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

/// The existing reader owns its typed terminal. The kernel reports its thread
/// exit independently of the very completion post that may have failed.
pub(super) struct Input {
    pub(super) receive: mpsc::Receiver<Opened>,
    producer: Producer,
}

impl Input {
    pub(super) fn start(
        input: impl io::BufRead + Send + 'static,
        port: HANDLE,
    ) -> io::Result<Self> {
        // One queued record is credit, not a population limit. The original
        // reader keeps the next record while waiting for the serving owner.
        let (send, receive) = mpsc::sync_channel(1);
        let address = port.expose_provenance();
        let thread = std::thread::Builder::new()
            .name("custodian-input".into())
            .spawn(move || read_trees(input, send, address))?;
        Self::watch(receive, thread, port)
    }

    fn watch(
        receive: mpsc::Receiver<Opened>,
        thread: std::thread::JoinHandle<io::Result<()>>,
        port: HANDLE,
    ) -> io::Result<Self> {
        // SAFETY: the original port and thread remain owned through dequeue.
        // Already-exited threads still produce this same terminal packet.
        let producer = Producer::new(
            thread,
            unsafe { BorrowedHandle::borrow_raw(port) },
            INPUT_EXITED,
        )?;
        Ok(Self { receive, producer })
    }

    /// Called only for the original kernel thread-exit packet. Joining cannot
    /// park on stdin, queue credit or a user-mode completion post.
    pub(super) fn finish(&mut self) -> io::Result<()> {
        self.producer.finish()
    }
}

fn read_trees(
    mut input: impl io::Read,
    send: mpsc::SyncSender<Opened>,
    port: usize,
) -> io::Result<()> {
    let mut frame = [0; Opened::FRAME_BYTES];
    loop {
        let mut filled = 0;
        while filled < frame.len() {
            let count = input.read(&mut frame[filled..])?;
            if count == 0 {
                return if filled == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(
                        ERROR_INVALID_DATA.cast_signed(),
                    ))
                };
            }
            filled += count;
        }
        let tree = Opened::decode(&frame)
            .ok_or_else(|| io::Error::from_raw_os_error(ERROR_INVALID_DATA.cast_signed()))?;
        send.send(tree).map_err(|_| io::ErrorKind::BrokenPipe)?;
        // SAFETY: the original custodian retains the port. Failure returns
        // through the thread's result and its independent kernel exit packet.
        if unsafe {
            PostQueuedCompletionStatus(
                std::ptr::with_exposed_provenance_mut(port),
                0,
                OPENED,
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_tree::custodian::{Handoff, event};
    use std::fs::File;
    use std::io::Write as _;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, ERROR_ACCESS_DENIED, ERROR_INVALID_HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::IO::{
        CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, INFINITE};

    fn pipe() -> io::Result<(File, File)> {
        let (mut read, mut write) = (std::ptr::null_mut(), std::ptr::null_mut());
        // SAFETY: writable outputs, each transferred once into a File below.
        if unsafe { CreatePipe(&raw mut read, &raw mut write, std::ptr::null(), 0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: these are the two distinct handles just created.
        Ok(unsafe {
            (
                File::from(OwnedHandle::from_raw_handle(read)),
                File::from(OwnedHandle::from_raw_handle(write)),
            )
        })
    }

    fn port() -> io::Result<OwnedHandle> {
        // SAFETY: create one original IOCP, retained through each dequeue.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transferred this handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(port) })
    }

    fn packet(port: &OwnedHandle) -> io::Result<usize> {
        let mut message = 0;
        let mut key = 0;
        let mut overlapped: *mut OVERLAPPED = std::ptr::null_mut();
        // SAFETY: original retained port and writable outputs. Its producer is
        // the original input thread's kernel association, not another post.
        if unsafe {
            GetQueuedCompletionStatus(
                port.as_raw_handle(),
                &raw mut message,
                &raw mut key,
                &raw mut overlapped,
                INFINITE,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(key)
    }

    /// Use the original handover writer, with actual duplicated native objects.
    /// This test retains their handles because its receiver never opens a tree.
    fn frame() -> io::Result<(Vec<u8>, Vec<OwnedHandle>)> {
        let source = event(false, None)?;
        // SAFETY: the current-process pseudo-handle remains live.
        let mut handoff = Handoff::new(unsafe { GetCurrentProcess() });
        for _ in 0..4 {
            handoff.duplicate(source.as_raw_handle(), 0, DUPLICATE_SAME_ACCESS)?;
        }
        let handles = handoff.handles;
        let mut bytes = Vec::with_capacity(Opened::FRAME_BYTES);
        let mut published = Ok(());
        let allocations = acyclic_test_support::measure(|| {
            published = handoff.publish(&mut bytes, 7, &[3; 16]);
        });
        published?;
        // SAFETY: the original publication transferred these distinct handles
        // into this process; no State owns or closes them in this control.
        let retained = handles
            .into_iter()
            .map(|handle| unsafe { OwnedHandle::from_raw_handle(handle) })
            .collect::<Vec<_>>();
        assert_eq!(
            allocations.count_total, 0,
            "restored decimal/hex formatting allocates"
        );
        Ok((bytes, retained))
    }

    #[test]
    fn input_terminal_survives_full_credit_read_and_completion_post_failure() -> io::Result<()> {
        let port = port()?;
        let (bytes, handles) = frame()?;

        // Ordinary EOF remains distinct from malformed input. Both original
        // packets can arrive before observation; the record stays available.
        let (read, mut write) = pipe()?;
        let mut input = Input::start(io::BufReader::new(read), port.as_raw_handle())?;
        write.write_all(&bytes)?;
        drop(write);
        assert_eq!(packet(&port)?, OPENED);
        assert_eq!(packet(&port)?, INPUT_EXITED);
        input.finish()?;
        let opened = input.receive.try_recv().unwrap();
        assert_eq!((opened.tree, opened.secret), (7, [3; 16]));
        for (actual, expected) in [opened.job, opened.process, opened.ready, opened.done]
            .into_iter()
            .zip(&handles)
        {
            assert_eq!(actual, expected.as_raw_handle());
        }

        // The first original post proves the one queue slot is occupied.
        // Withdraw its receiver while the next actual record waits for credit:
        // producer loss must terminate without another user-mode post.
        let (next, _next_handles) = frame()?;
        let (read, mut write) = pipe()?;
        let input = Input::start(io::BufReader::new(read), port.as_raw_handle())?;
        write.write_all(&bytes)?;
        write.write_all(&next)?;
        drop(write);
        assert_eq!(packet(&port)?, OPENED);
        let Input {
            receive,
            mut producer,
        } = input;
        drop(receive);
        assert_eq!(packet(&port)?, INPUT_EXITED);
        assert_eq!(
            producer.finish().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        drop(producer);

        // Inject the actual failing post argument at the original reader,
        // retaining the SAME kernel thread-exit observer on its original port.
        let (read, mut write) = pipe()?;
        let (send, receive) = mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            let input = io::BufReader::new(read);
            let mut result = Ok(());
            let allocations = acyclic_test_support::measure(|| {
                result = read_trees(input, send, 0);
            });
            assert_eq!(
                allocations.count_total, 0,
                "a growing input buffer allocates"
            );
            result
        });
        let mut input = Input::watch(receive, thread, port.as_raw_handle())?;
        write.write_all(&bytes)?;
        drop(write);
        assert_eq!(packet(&port)?, INPUT_EXITED);
        assert_eq!(
            input.finish().unwrap_err().raw_os_error(),
            Some(ERROR_INVALID_HANDLE.cast_signed())
        );
        assert_eq!(input.receive.try_recv().unwrap().tree, 7);

        // Truncate the original native frame at each byte or mutate its version
        // or selected handle. No invalid owner may reach the credited receiver,
        // including the readiness handle that State::open adopts as OwnedHandle.
        let mut malformed = (1..bytes.len())
            .map(|cut| bytes[..cut].to_vec())
            .collect::<Vec<_>>();
        for version in [0, 2] {
            let mut frame = bytes.clone();
            frame[0] = version;
            malformed.push(frame);
        }
        for index in 1..=4 {
            for sentinel in [0, usize::MAX as u64] {
                let mut frame = bytes.clone();
                frame[1 + index * 8..1 + (index + 1) * 8].copy_from_slice(&sentinel.to_le_bytes());
                malformed.push(frame);
            }
        }
        for bytes in malformed {
            let (read, mut write) = pipe()?;
            let mut input = Input::start(io::BufReader::new(read), port.as_raw_handle())?;
            write.write_all(&bytes)?;
            drop(write);
            assert_eq!(packet(&port)?, INPUT_EXITED);
            assert_eq!(
                input.finish().unwrap_err().raw_os_error(),
                Some(ERROR_INVALID_DATA.cast_signed())
            );
            assert!(input.receive.try_recv().is_err());
        }

        // Read from the actual write-only native pipe handle. Its native cause
        // must survive the original worker result and kernel terminal join.
        let (_read, write) = pipe()?;
        let mut input = Input::start(io::BufReader::new(write), port.as_raw_handle())?;
        assert_eq!(packet(&port)?, INPUT_EXITED);
        assert_eq!(
            input.finish().unwrap_err().raw_os_error(),
            Some(ERROR_ACCESS_DENIED.cast_signed())
        );
        Ok(())
    }
}
