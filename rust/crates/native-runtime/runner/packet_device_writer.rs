//! One physical notifier for the shared packet host, independent of its
//! request/reply event loop. Packet storage stays with the submitting session.

use super::sys::{self, Errno, Fd};
use core::ptr;
use core::sync::atomic::{AtomicI32, AtomicPtr, AtomicUsize, Ordering};

struct Shared {
    command: Fd,
    completion: Fd,
    token: AtomicUsize,
    device: AtomicI32,
    bytes: AtomicPtr<u8>,
    length: AtomicUsize,
    result: AtomicI32,
}

pub(super) struct Writer {
    shared: *mut Shared,
    thread: Option<sys::Thread>,
    command: Fd,
    completion: Fd,
    busy: bool,
}

impl Writer {
    pub(super) fn new() -> Result<Self, Errno> {
        let command = sys::pipe()?;
        let completion = match sys::pipe() {
            Ok(pipe) => pipe,
            Err(error) => {
                for file in command {
                    sys::close(file);
                }
                return Err(error);
            }
        };
        let shared = match sys::map(size_of::<Shared>()) {
            Ok(mapping) => mapping.cast::<Shared>(),
            Err(error) => {
                for file in command.into_iter().chain(completion) {
                    sys::close(file);
                }
                return Err(error);
            }
        };
        // SAFETY: this fresh mapping has one parent owner through thread join.
        unsafe {
            shared.write(Shared {
                command: command[0],
                completion: completion[1],
                token: AtomicUsize::new(0),
                device: AtomicI32::new(-1),
                bytes: AtomicPtr::new(ptr::null_mut()),
                length: AtomicUsize::new(0),
                result: AtomicI32::new(0),
            });
        }
        // SAFETY: the mapping and original pipe ends remain owned until the
        // entry exits and its kernel clear-child-tid event has been joined.
        let thread = match unsafe { sys::Thread::spawn(run, shared.expose_provenance()) } {
            Ok(thread) => thread,
            Err(error) => {
                for file in command.into_iter().chain(completion) {
                    sys::close(file);
                }
                // SAFETY: spawn failed before any borrower could exist.
                unsafe { sys::unmap(shared.cast(), size_of::<Shared>()) };
                return Err(error);
            }
        };
        let writer = Self {
            shared,
            thread: Some(thread),
            command: command[1],
            completion: completion[0],
            busy: false,
        };
        // Startup is one receipt, before this host can admit a mounted view.
        read_event(writer.completion)?;
        // SAFETY: the startup receipt publishes this shared result.
        let result = unsafe { &*writer.shared }.result.load(Ordering::Acquire);
        if result != 0 {
            return Err(Errno(result));
        }
        sys::nonblocking(writer.completion)?;
        Ok(writer)
    }

    pub(super) fn completion(&self) -> Fd {
        self.completion
    }

    /// The device and immutable packet must remain live until `complete`.
    /// Only one submission may be in flight; the host owns its ready queue.
    pub(super) unsafe fn submit(
        &mut self,
        token: usize,
        device: Fd,
        bytes: *mut u8,
        length: usize,
    ) -> Result<(), Errno> {
        assert!(!self.busy);
        // SAFETY: this owner retains the mapping through its thread's join.
        let shared = unsafe { &*self.shared };
        shared.device.store(device, Ordering::Relaxed);
        shared.bytes.store(bytes, Ordering::Relaxed);
        shared.length.store(length, Ordering::Relaxed);
        shared.token.store(token, Ordering::Release);
        // At most one byte is outstanding: this write cannot wait for pipe
        // credit while the notifier is blocked inside the kernel.
        write_event(self.command)?;
        self.busy = true;
        Ok(())
    }

    pub(super) fn complete(&mut self) -> Result<(usize, Result<(), Errno>), Errno> {
        assert!(self.busy);
        read_event(self.completion)?;
        // SAFETY: the worker publishes result before the completion event.
        let shared = unsafe { &*self.shared };
        let result = shared.result.load(Ordering::Acquire);
        let token = shared.token.load(Ordering::Acquire);
        self.busy = false;
        Ok((
            token,
            if result == 0 {
                Ok(())
            } else {
                Err(Errno(result))
            },
        ))
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // The host must settle every admitted packet before surrendering its
        // session storage or joining the notifier. Retiring owners continue
        // receiving error replies until that physical obligation completes.
        assert!(!self.busy);
        sys::close(self.command);
        drop(self.thread.take());
        sys::close(self.completion);
        // SAFETY: the worker exited; its pipe ends closed before kernel join.
        unsafe { sys::unmap(self.shared.cast(), size_of::<Shared>()) };
    }
}

extern "C" fn run(argument: usize) -> ! {
    // SAFETY: Writer owns the mapping and joins this entry before unmapping it.
    let shared = unsafe { &*ptr::with_exposed_provenance::<Shared>(argument) };
    let queue = sys::epoll();
    shared.result.store(
        queue.as_ref().err().map_or(0, |error| error.0),
        Ordering::Release,
    );
    let ready = write_event(shared.completion).is_ok();
    if let Ok(queue) = queue {
        if ready {
            while read_event(shared.command).is_ok() {
                let _token = shared.token.load(Ordering::Acquire);
                let device = shared.device.load(Ordering::Relaxed);
                let length = shared.length.load(Ordering::Relaxed);
                // SAFETY: the release/acquire submission retains these immutable
                // bytes and their device until the original physical completion.
                let bytes = unsafe {
                    core::slice::from_raw_parts(shared.bytes.load(Ordering::Relaxed), length)
                };
                let result = publish(queue, device, bytes);
                shared
                    .result
                    .store(result.err().map_or(0, |error| error.0), Ordering::Release);
                if write_event(shared.completion).is_err() {
                    break;
                }
            }
        }
        sys::close(queue);
    }
    // Only this entry owns these original ends. The parent closes the other
    // ends, joins the kernel thread, and then unmaps the shared record.
    sys::close(shared.command);
    sys::close(shared.completion);
    sys::exit_thread()
}

fn publish(queue: Fd, device: Fd, bytes: &[u8]) -> Result<(), Errno> {
    loop {
        match sys::write(device, bytes) {
            Ok(length) if length == bytes.len() => return Ok(()),
            Ok(_) => return Err(Errno(sys::EPIPE)),
            Err(Errno(sys::EINTR)) => {}
            Err(Errno(sys::EAGAIN)) => {
                sys::interest(queue, device, true, sys::EPOLL_OUT, 0)?;
                let mut events = [sys::EpollEvent {
                    events: 0,
                    token: 0,
                }];
                let ready = sys::events(queue, &mut events);
                sys::unwatch(queue, device);
                ready?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn read_event(file: Fd) -> Result<(), Errno> {
    loop {
        match sys::read(file, &mut [0]) {
            Ok(1) => return Ok(()),
            Ok(_) => return Err(Errno(sys::EPIPE)),
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
}

fn write_event(file: Fd) -> Result<(), Errno> {
    loop {
        match sys::write(file, &[1]) {
            Ok(1) => return Ok(()),
            Ok(_) => return Err(Errno(sys::EPIPE)),
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
}
