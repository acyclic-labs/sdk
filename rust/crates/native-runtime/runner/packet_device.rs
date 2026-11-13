//! One event-driven host for packet devices. The owner retains only a stream.
//! No callback, namespace lookup, or wait for an owner runs in this process.

use super::packet_device_receipt::Receipt;
use super::{fuse_protocol, receive_handles, sys};
use core::ptr;
use sys::{Errno, Fd};

const CONTROL: u64 = 0;
const OWNER: u64 = 1;
const DEVICE: u64 = 2;
const STREAM: u64 = 3;

struct Packet {
    bytes: *mut u8,
    capacity: usize,
    filled: usize,
    sent: usize,
    routed: bool,
}

impl Packet {
    fn new(capacity: usize) -> Result<Self, Errno> {
        Ok(Self {
            bytes: sys::map(capacity)?,
            capacity,
            filled: 0,
            sent: 0,
            routed: false,
        })
    }

    fn slice(&mut self, start: usize, end: usize) -> &mut [u8] {
        // SAFETY: all callers use checked offsets within this owned mapping.
        unsafe { core::slice::from_raw_parts_mut(self.bytes.add(start), end - start) }
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        // SAFETY: the packet's last owner surrendered its whole mapping.
        unsafe { sys::unmap(self.bytes, self.capacity) };
    }
}

struct Session {
    previous: *mut Session,
    next: *mut Session,
    garbage: *mut Session,
    active: bool,
    mounted: bool,
    waiting_helper: bool,
    device_registered: bool,
    stream_registered: bool,
    control: Fd,
    owner: Fd,
    device: Fd,
    stream: Fd,
    input: Option<Packet>,
    output: Option<Packet>,
    protocol: Option<fuse_protocol::Session>,
    response: Response,
    initializing: bool,
    initialization_trace: Fd,
}

enum Response {
    Backend,
    Session {
        transition: Option<fuse_protocol::Transition>,
        failure: Option<fuse_protocol::Error>,
        close: bool,
    },
}

// Both supported Linux architectures have pages of at least 4096 bytes.
// Client funding owns one whole page for this fixed metadata mapping.
const _: () = assert!(core::mem::size_of::<Session>() <= 4096);

impl Session {
    fn new(control: Fd) -> Result<*mut Self, Errno> {
        let base = sys::map(core::mem::size_of::<Self>())?.cast::<Self>();
        // SAFETY: the fresh mapping is sufficiently aligned and exclusively owned.
        unsafe {
            base.write(Self {
                previous: ptr::null_mut(),
                next: ptr::null_mut(),
                garbage: ptr::null_mut(),
                active: true,
                mounted: false,
                waiting_helper: false,
                device_registered: false,
                stream_registered: false,
                control,
                owner: -1,
                device: -1,
                stream: -1,
                input: None,
                output: None,
                protocol: None,
                response: Response::Backend,
                initializing: false,
                initialization_trace: -1,
            })
        };
        Ok(base)
    }

    fn interest(&mut self, queue: Fd) -> Result<(), Errno> {
        let base = ptr::from_ref(self).addr() as u64;
        if self.waiting_helper {
            sys::interest(
                queue,
                self.device,
                !self.device_registered,
                sys::EPOLL_IN,
                base | DEVICE,
            )?;
            self.device_registered = true;
        } else if self.mounted
            && let Some(protocol) = &self.protocol
        {
            let input = self.input.as_ref().ok_or(Errno(sys::EINVAL))?;
            let output = self.output.as_ref().ok_or(Errno(sys::EINVAL))?;
            let complete = output.filled >= 4 && output.filled == frame_length(output);
            let device_events = if input.filled == 0 && output.filled == 0 && !protocol.closed() {
                sys::EPOLL_IN
            } else {
                0
            } | if complete { sys::EPOLL_OUT } else { 0 };
            let stream_events =
                if !complete && protocol.ready() && matches!(self.response, Response::Backend) {
                    sys::EPOLL_IN
                } else {
                    0
                } | if input.filled != 0 && input.routed {
                    sys::EPOLL_OUT
                } else {
                    0
                };
            sys::interest(
                queue,
                self.device,
                !self.device_registered,
                device_events,
                base | DEVICE,
            )?;
            self.device_registered = true;
            sys::interest(
                queue,
                self.stream,
                !self.stream_registered,
                stream_events,
                base | STREAM,
            )?;
            self.stream_registered = true;
        }
        Ok(())
    }
    fn admit(&mut self, queue: Fd) -> Result<(), Errno> {
        let mut body = [0; 12];
        let request = receive_handles(self.control, &mut body)?.ok_or(Errno(sys::EPIPE))?;
        if request.length != 12 || request.count != 3 || body[4..8] != 1_u32.to_ne_bytes() {
            request.close();
            return Err(Errno(sys::EINVAL));
        }
        self.device = request.descriptors[0];
        self.stream = request.descriptors[1];
        self.owner = request.descriptors[2];
        let capacity =
            u32::from_ne_bytes(body[..4].try_into().map_err(|_| Errno(sys::EINVAL))?) as usize;
        if capacity < 4 {
            return Err(Errno(sys::EINVAL));
        }
        match u32::from_ne_bytes(body[8..12].try_into().map_err(|_| Errno(sys::EINVAL))?) {
            2 => {}
            3 => self.waiting_helper = true,
            _ => return Err(Errno(sys::EINVAL)),
        }
        self.input = Some(Packet::new(capacity)?);
        self.output = Some(Packet::new(capacity)?);
        sys::nonblocking(self.device)?;
        sys::nonblocking(self.stream)?;
        sys::interest(
            queue,
            self.owner,
            true,
            sys::EPOLL_IN,
            ptr::from_ref(self).addr() as u64 | OWNER,
        )?;
        self.interest(queue)?;
        // Acceptance proves that this host owns all three original handles.
        write_control(self.control, &sys::getpid().to_ne_bytes())
    }

    fn command(&mut self, queue: Fd) -> Result<(), Errno> {
        let mut body = [0_u8; 20];
        let request = receive_handles(self.control, &mut body)?.ok_or(Errno(sys::EPIPE))?;
        if request.length == 20 && request.count <= 1 && body[..4] == 2_u32.to_ne_bytes() {
            if !self.mounted || self.waiting_helper || self.protocol.is_some() {
                request.close();
                return Err(Errno(sys::EINVAL));
            }
            if request.count == 1 {
                // A memfd cannot perform a lookup or callback into an owner's
                // mount. Never accept a caller's arbitrary filesystem here.
                self.initialization_trace = request.descriptors[0];
                sys::memory_file(self.initialization_trace)?;
            }
            let word = |offset| u32::from_ne_bytes(super::array4(&body[offset..offset + 4]));
            self.protocol = Some(
                fuse_protocol::Session::new(fuse_protocol::Configuration {
                    maximum_write: word(4),
                    page_size: word(8),
                    cache_capabilities: u64::from_ne_bytes(
                        body[12..20].try_into().map_err(|_| Errno(sys::EINVAL))?,
                    ),
                })
                .map_err(Errno)?,
            );
            self.initializing = true;
            return self.interest(queue);
        }
        if request.count != 1 || request.length != 20 || self.mounted || self.waiting_helper {
            request.close();
            return Err(Errno(sys::EINVAL));
        }
        let result = (|| {
            let word = |offset| u32::from_ne_bytes(super::array4(&body[offset..offset + 4]));
            if word(0) != 1 {
                return Err(Errno(sys::EINVAL));
            }
            use core::fmt::Write;
            let mut target = Options {
                bytes: [0; 128],
                length: 0,
            };
            // The original already-open directory is the target. No lookup
            // through an owner's FUSE namespace can block this event loop.
            write!(&mut target, "/proc/self/fd/{}", request.descriptors[0])
                .map_err(|_| Errno(sys::EINVAL))?;
            let target = core::ffi::CStr::from_bytes_with_nul(&target.bytes[..target.length + 1])
                .map_err(|_| Errno(sys::EINVAL))?;
            let mut options = Options {
                bytes: [0; 128],
                length: 0,
            };
            write!(
                &mut options,
                "fd={},rootmode={:o},user_id={},group_id={},default_permissions",
                self.device,
                word(4),
                word(8),
                word(12)
            )
            .map_err(|_| Errno(sys::EINVAL))?;
            let options =
                core::ffi::CStr::from_bytes_with_nul(&options.bytes[..options.length + 1])
                    .map_err(|_| Errno(sys::EINVAL))?;
            sys::mount_fuse(target, options, word(16) != 0)
        })();
        request.close();
        if result.is_ok() {
            self.mounted = true;
            self.interest(queue)?;
        }
        let status = match result {
            Ok(()) => 0,
            Err(Errno(error)) => -error,
        };
        write_control(self.control, &status.to_ne_bytes())
    }
    fn helper_ready(&mut self, queue: Fd) -> Result<(), Errno> {
        let mut body = [0; 5];
        let request = receive_handles(self.device, &mut body)?.ok_or(Errno(sys::EPIPE))?;
        let device = super::ancillary::Handles {
            descriptors: request.descriptors,
            count: request.count,
        }
        .one(sys::close)
        .map_err(|()| Errno(sys::EINVAL))?;
        sys::unwatch(queue, self.device);
        sys::close(self.device);
        self.device = device;
        self.device_registered = false;
        self.waiting_helper = false;
        self.mounted = true;
        sys::nonblocking(self.device)?;
        self.interest(queue)?;
        write_control(self.control, &0_i32.to_ne_bytes())
    }
    fn progress(&mut self) -> Result<(), Errno> {
        let protocol = self.protocol.as_ref().ok_or(Errno(sys::EINVAL))?;
        let output = self.output.as_mut().ok_or(Errno(sys::EINVAL))?;
        if protocol.ready() && matches!(self.response, Response::Backend) {
            let needed = if output.filled < 4 {
                4
            } else {
                frame_length(output)
            };
            if needed < 4 || needed > output.capacity {
                return Err(Errno(sys::EINVAL));
            }
            if output.filled < needed
                && let Some(count) = read(self.stream, output.slice(output.filled, needed))?
            {
                if count == 0 {
                    return Err(Errno(sys::EPIPE));
                }
                output.filled += count;
            }
        }
        self.publish()?;
        let input = self.input.as_mut().ok_or(Errno(sys::EINVAL))?;
        let output = self.output.as_mut().ok_or(Errno(sys::EINVAL))?;
        let protocol = self.protocol.as_ref().ok_or(Errno(sys::EINVAL))?;
        if input.filled == 0
            && output.filled == 0
            && !protocol.closed()
            && let Some(count) = read(self.device, input.slice(0, input.capacity))?
        {
            if count == 0 {
                return Err(Errno(sys::EPIPE));
            }
            input.filled = count;
        }
        let mut local = false;
        if input.filled != 0 && !input.routed && output.filled == 0 {
            let request =
                fuse_protocol::Request::parse(input.slice(0, input.filled)).map_err(Errno)?;
            let prepared = if request.owner_allowed(sys::effective_user()) {
                protocol.prepare(&request, output.slice(0, output.capacity))
            } else {
                Err(fuse_protocol::Error::Errno(sys::EACCES))
            };
            match prepared {
                Ok(fuse_protocol::Prepared::Backend) => input.routed = true,
                Ok(fuse_protocol::Prepared::Reply { length, transition }) => {
                    output.filled = length;
                    self.response = Response::Session {
                        transition,
                        failure: None,
                        close: request.opcode == 38,
                    };
                    local = true;
                }
                Err(error) => {
                    // Reference-release messages have no physical response.
                    if matches!(request.opcode, 2 | 42) {
                        return Err(Errno(error.errno()));
                    }
                    output.filled = fuse_protocol::reply(
                        output.slice(0, output.capacity),
                        request.unique,
                        &[],
                        Some(error.errno()),
                    )
                    .map_err(Errno)?;
                    self.response = Response::Session {
                        transition: None,
                        failure: Some(error),
                        close: request.opcode == 26
                            || matches!(error, fuse_protocol::Error::Closed),
                    };
                    local = true;
                }
            }
            if local && self.initialization_trace < 0 {
                input.filled = 0;
                input.sent = 0;
            }
        }
        if input.filled != 0
            && input.routed
            && let Some(count) = write(self.stream, input.slice(input.sent, input.filled))?
        {
            if count == 0 {
                return Err(Errno(sys::EPIPE));
            }
            input.sent += count;
            if input.sent == input.filled {
                input.sent = 0;
                input.filled = 0;
                input.routed = false;
            }
        }
        if local {
            self.publish()?;
        }
        Ok(())
    }

    /// The only positive publication boundary is the physical device write.
    fn publish(&mut self) -> Result<(), Errno> {
        let output = self.output.as_mut().ok_or(Errno(sys::EINVAL))?;
        if output.filled < 4 {
            return Ok(());
        }
        let needed = frame_length(output);
        if needed < 4 || needed > output.capacity {
            return Err(Errno(sys::EINVAL));
        }
        if output.filled != needed {
            return Ok(());
        }
        let notification = needed >= 16 && output.slice(8, 16) == [0; 8];
        let count = match write(self.device, output.slice(0, needed)) {
            Ok(Some(count)) => count,
            Ok(None) => return Ok(()),
            Err(error) if matches!(self.response, Response::Backend) => {
                // A failed atomic device write publishes no bytes. Preserve
                // the original errno for the caller's policy (cache invalidation
                // can legitimately miss an inode) and keep the session alive.
                output.filled = 0;
                send_receipt(self.control, Receipt::Rejected(notification, error.0))?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if count != needed {
            return Err(Errno(sys::EPIPE));
        }
        if self.initialization_trace >= 0 && matches!(self.response, Response::Session { .. }) {
            let input = self.input.as_mut().ok_or(Errno(sys::EINVAL))?;
            let mut lengths = [0; 16];
            lengths[..8].copy_from_slice(&(input.filled as u64).to_ne_bytes());
            lengths[8..].copy_from_slice(&(needed as u64).to_ne_bytes());
            write_memory(self.initialization_trace, &lengths)?;
            write_memory(self.initialization_trace, input.slice(0, input.filled))?;
            write_memory(self.initialization_trace, output.slice(0, needed))?;
            input.filled = 0;
            input.sent = 0;
        }
        output.filled = 0;
        let response = core::mem::replace(&mut self.response, Response::Backend);
        match response {
            Response::Backend => {
                send_receipt(self.control, Receipt::Published(notification))?;
            }
            Response::Session {
                transition,
                failure,
                close,
            } => {
                if let Some(transition) = transition {
                    let protocol = self.protocol.as_ref().ok_or(Errno(sys::EINVAL))?;
                    protocol.delivered(transition);
                    if protocol.ready() && self.initializing {
                        self.initializing = false;
                        self.initialized(None)?;
                    }
                }
                if close {
                    if self.initializing {
                        self.initializing = false;
                        self.initialized(failure)?;
                    }
                    return Err(Errno(sys::EPIPE));
                }
            }
        }
        Ok(())
    }

    fn initialized(&mut self, failure: Option<fuse_protocol::Error>) -> Result<(), Errno> {
        if self.initialization_trace >= 0 {
            sys::close(self.initialization_trace);
            self.initialization_trace = -1;
        }
        let mut response = [0; 12];
        if let Some(error) = failure {
            response[..4].copy_from_slice(&(-error.errno()).to_ne_bytes());
            if let fuse_protocol::Error::UnsupportedCapabilities(flags) = error {
                response[4..].copy_from_slice(&flags.to_ne_bytes());
            }
        }
        write_control(self.control, &response)
    }
    fn close(&mut self, queue: Fd) {
        self.active = false;
        // Device closure must precede the control EOF acknowledging retirement.
        for file in [
            &mut self.device,
            &mut self.stream,
            &mut self.owner,
            &mut self.initialization_trace,
        ] {
            if *file >= 0 {
                sys::unwatch(queue, *file);
                sys::close(*file);
                *file = -1;
            }
        }
    }
}

fn frame_length(packet: &Packet) -> usize {
    // SAFETY: callers require four initialized bytes in this live mapping.
    let bytes = unsafe { core::slice::from_raw_parts(packet.bytes, 4) };
    u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
}

fn read(file: Fd, bytes: &mut [u8]) -> Result<Option<usize>, Errno> {
    loop {
        match sys::read(file, bytes) {
            Err(Errno(sys::EINTR)) => {}
            Err(Errno(sys::EAGAIN)) => return Ok(None),
            result => return result.map(Some),
        }
    }
}

fn write(file: Fd, bytes: &[u8]) -> Result<Option<usize>, Errno> {
    loop {
        match sys::write(file, bytes) {
            Err(Errno(sys::EINTR)) => {}
            Err(Errno(sys::EAGAIN)) => return Ok(None),
            result => return result.map(Some),
        }
    }
}

// Only an admitted memfd reaches this writer. A diagnostic record may span
// writes; the physical FUSE reply above must always remain one atomic write.
fn write_memory(file: Fd, mut bytes: &[u8]) -> Result<(), Errno> {
    while !bytes.is_empty() {
        match sys::write(file, bytes) {
            Ok(0) => return Err(Errno(sys::EPIPE)),
            Ok(written) => bytes = &bytes[written..],
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn accept(listener: Fd, queue: Fd, first: &mut *mut Session) -> Result<(), Errno> {
    loop {
        let control = match sys::accept(listener) {
            Err(Errno(sys::EINTR)) => continue,
            Err(Errno(sys::EAGAIN)) => return Ok(()),
            result => result?,
        };
        // SO_PEERCRED authenticates the sender, including before its first byte.
        if sys::peer_user(control)? != sys::effective_user() {
            sys::close(control);
            continue;
        }
        let session = match Session::new(control) {
            Ok(session) => session,
            Err(_) => {
                sys::close(control);
                continue;
            }
        };
        if sys::interest(
            queue,
            control,
            true,
            sys::EPOLL_IN,
            session.addr() as u64 | CONTROL,
        )
        .is_err()
        {
            sys::close(control);
            // SAFETY: admission has not published this mapping or any token;
            // a failed epoll registration installs no interest.
            unsafe {
                ptr::drop_in_place(session);
                sys::unmap(session.cast(), core::mem::size_of::<Session>());
            }
            continue;
        }
        // SAFETY: a fresh initialized mapping; list mutations are single-threaded.
        unsafe {
            (*session).next = *first;
            if !first.is_null() {
                (**first).previous = session;
            }
            *first = session;
        }
    }
}

pub(super) fn serve(listener: Fd) -> Result<(), Errno> {
    const ADMISSION: Fd = 4;
    const ADMISSION_EVENTS: Fd = 5;
    const RELEASED: u64 = u64::MAX;
    sys::nonblocking(listener)?;
    let queue = sys::epoll()?;
    sys::interest(queue, listener, true, sys::EPOLL_IN, 0)?;
    sys::interest(queue, ADMISSION_EVENTS, true, sys::EPOLL_IN, RELEASED)?;
    let mut first: *mut Session = ptr::null_mut();
    let mut events = [sys::EpollEvent {
        events: 0,
        token: 0,
    }; 64];
    loop {
        let count = sys::events(queue, &mut events)?;
        let mut garbage: *mut Session = ptr::null_mut();
        for event in &events[..count] {
            let token = event.token;
            if token == 0 {
                accept(listener, queue, &mut first)?;
                continue;
            }
            if token == RELEASED {
                // Drain event bytes, never infer lock ownership from them.
                // A coalesced or overflow event still prompts the same atomic
                // lock/accept/close cut below; no contender is enumerated.
                let _ = read(ADMISSION_EVENTS, &mut [0; 4096])?;
                continue;
            }
            let session = ptr::with_exposed_provenance_mut::<Session>((token & !3) as usize);
            // SAFETY: event tokens name live list entries. Retired entries stay
            // mapped until this entire event batch has surrendered its tokens.
            unsafe {
                let current = &mut *session;
                if !current.active {
                    continue;
                }
                let kind = token & 3;
                let result = if kind == OWNER
                    || (event.events & sys::EPOLL_HUP != 0
                        && !(kind == DEVICE && current.waiting_helper))
                {
                    Err(Errno(sys::EPIPE))
                } else if kind == CONTROL {
                    if current.owner < 0 {
                        current.admit(queue)
                    } else {
                        current.command(queue)
                    }
                } else if current.waiting_helper {
                    current.helper_ready(queue)
                } else {
                    current.progress().and_then(|()| current.interest(queue))
                };
                // An accepted stream that has not sent its first message waits
                // on the same readable event; other sessions keep progressing.
                if matches!(result, Err(Errno(sys::EAGAIN))) {
                    continue;
                }
                if result.is_err() {
                    current.close(queue);
                    if current.previous.is_null() {
                        first = current.next;
                    } else {
                        (*current.previous).next = current.next;
                    }
                    if !current.next.is_null() {
                        (*current.next).previous = current.previous;
                    }
                    current.garbage = garbage;
                    garbage = session;
                }
            }
        }
        let mut retire_host = false;
        if first.is_null() {
            // Contenders hold this lock only through connect, never through
            // a server reply. Do not block here: a full listen backlog must
            // continue accepting while a connector owns the lock. Its close
            // event also wakes us if it fails or dies before connecting.
            match sys::try_lock(ADMISSION) {
                Ok(()) => {
                    let accepted = accept(listener, queue, &mut first);
                    let retiring = accepted.is_ok() && first.is_null();
                    if retiring {
                        // Release the name while still excluding new binders.
                        sys::close(listener);
                    }
                    let unlocked = sys::unlock(ADMISSION);
                    accepted?;
                    unlocked?;
                    retire_host = retiring;
                }
                Err(Errno(sys::EAGAIN)) => {}
                Err(error) => return Err(error),
            }
        }
        while !garbage.is_null() {
            // SAFETY: all event borrowers have retired; unlink and unmap exactly once.
            unsafe {
                let next = (*garbage).garbage;
                let control = (*garbage).control;
                sys::unwatch(queue, control);
                ptr::drop_in_place(garbage);
                sys::unmap(garbage.cast(), core::mem::size_of::<Session>());
                // Receipt follows physical device closure and buffer destruction.
                let _ = send_receipt(control, Receipt::Retired);
                sys::close(control);
                garbage = next;
            }
        }
        if retire_host {
            return Ok(());
        }
    }
}

fn send_receipt(control: Fd, receipt: Receipt) -> Result<(), Errno> {
    let bytes: [u8; Receipt::SIZE] = receipt.try_into().map_err(|()| Errno(sys::EINVAL))?;
    write_control(control, &bytes)
}

fn write_control(control: Fd, bytes: &[u8]) -> Result<(), Errno> {
    // Publication state has already been consumed. EAGAIN cannot be treated
    // as an unfinished read by the event loop: losing this receipt must close
    // the session and wake its owner rather than wait forever for another one.
    let written = sys::write(control, bytes).map_err(|error| {
        if error.0 == sys::EAGAIN {
            Errno(sys::EPIPE)
        } else {
            error
        }
    })?;
    if written != bytes.len() {
        return Err(Errno(sys::EPIPE));
    }
    Ok(())
}

struct Options {
    bytes: [u8; 128],
    length: usize,
}

impl core::fmt::Write for Options {
    fn write_str(&mut self, value: &str) -> core::fmt::Result {
        let end = self
            .length
            .checked_add(value.len())
            .ok_or(core::fmt::Error)?;
        if end >= self.bytes.len() {
            return Err(core::fmt::Error);
        }
        self.bytes[self.length..end].copy_from_slice(value.as_bytes());
        self.length = end;
        Ok(())
    }
}
