//! One event-driven host for packet devices. The owner retains only a stream.
//! No callback, namespace lookup, or wait for an owner runs in this process.

use super::{receive_handles, sys};
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
}

impl Packet {
    fn new(capacity: usize) -> Result<Self, Errno> {
        Ok(Self {
            bytes: sys::map(capacity)?,
            capacity,
            filled: 0,
            sent: 0,
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
    control: Fd,
    owner: Fd,
    device: Fd,
    stream: Fd,
    input: Option<Packet>,
    output: Option<Packet>,
}

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
                control,
                owner: -1,
                device: -1,
                stream: -1,
                input: None,
                output: None,
            })
        };
        Ok(base)
    }

    fn interest(&self, queue: Fd, add: bool) -> Result<(), Errno> {
        let base = ptr::from_ref(self).addr() as u64;
        if add {
            sys::interest(queue, self.owner, true, sys::EPOLL_IN, base | OWNER)?;
        }
        let input = self.input.as_ref().ok_or(Errno(sys::EINVAL))?;
        let output = self.output.as_ref().ok_or(Errno(sys::EINVAL))?;
        let complete = output.filled >= 4 && output.filled == frame_length(output);
        let device_events = if input.filled == 0 { sys::EPOLL_IN } else { 0 }
            | if complete { sys::EPOLL_OUT } else { 0 };
        let stream_events = if complete { 0 } else { sys::EPOLL_IN }
            | if input.filled != 0 { sys::EPOLL_OUT } else { 0 };
        sys::interest(queue, self.device, add, device_events, base | DEVICE)?;
        sys::interest(queue, self.stream, add, stream_events, base | STREAM)
    }

    fn admit(&mut self, queue: Fd) -> Result<(), Errno> {
        let mut body = [0; 8];
        let request = receive_handles(self.control, &mut body)?.ok_or(Errno(sys::EPIPE))?;
        if request.length != 8 || request.count != 3 || body[4..] != 1_u32.to_ne_bytes() {
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
        self.input = Some(Packet::new(capacity)?);
        self.output = Some(Packet::new(capacity)?);
        sys::nonblocking(self.device)?;
        sys::nonblocking(self.stream)?;
        self.interest(queue, true)?;
        // Acceptance proves that this host owns all three original handles.
        if sys::write(self.control, &[1])? != 1 {
            return Err(Errno(sys::EPIPE));
        }
        Ok(())
    }

    fn progress(&mut self) -> Result<(), Errno> {
        let input = self.input.as_mut().ok_or(Errno(sys::EINVAL))?;
        if input.filled == 0 {
            if let Some(count) = read(self.device, input.slice(0, input.capacity))? {
                if count == 0 {
                    return Err(Errno(sys::EPIPE));
                }
                input.filled = count;
            }
        }
        if input.filled != 0 {
            if let Some(count) = write(self.stream, input.slice(input.sent, input.filled))? {
                if count == 0 {
                    return Err(Errno(sys::EPIPE));
                }
                input.sent += count;
                if input.sent == input.filled {
                    input.sent = 0;
                    input.filled = 0;
                }
            }
        }
        let output = self.output.as_mut().ok_or(Errno(sys::EINVAL))?;
        let needed = if output.filled < 4 {
            4
        } else {
            frame_length(output)
        };
        if needed < 4 || needed > output.capacity {
            return Err(Errno(sys::EINVAL));
        }
        if output.filled < needed {
            if let Some(count) = read(self.stream, output.slice(output.filled, needed))? {
                if count == 0 {
                    return Err(Errno(sys::EPIPE));
                }
                output.filled += count;
            }
        }
        if output.filled >= 4 {
            let needed = frame_length(output);
            if needed < 4 || needed > output.capacity {
                return Err(Errno(sys::EINVAL));
            }
            if output.filled == needed {
                if let Some(count) = write(self.device, output.slice(0, needed))? {
                    // A short device publication is uncertain: close, never replay.
                    if count != needed {
                        return Err(Errno(sys::EPIPE));
                    }
                    output.filled = 0;
                }
            }
        }
        Ok(())
    }

    fn close(&mut self, queue: Fd) {
        self.active = false;
        // Device closure must precede the control EOF acknowledging retirement.
        for file in [
            &mut self.device,
            &mut self.stream,
            &mut self.owner,
            &mut self.control,
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
            Err(error) => {
                sys::close(control);
                return Err(error);
            }
        };
        // SAFETY: a fresh initialized mapping; list mutations are single-threaded.
        unsafe {
            (*session).next = *first;
            if !first.is_null() {
                (**first).previous = session;
            }
            *first = session;
        }
        sys::interest(
            queue,
            control,
            true,
            sys::EPOLL_IN,
            session.addr() as u64 | CONTROL,
        )?;
    }
}

pub(super) fn serve(listener: Fd) -> Result<(), Errno> {
    sys::nonblocking(listener)?;
    let queue = sys::epoll()?;
    sys::interest(queue, listener, true, sys::EPOLL_IN, 0)?;
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
            let session = ptr::with_exposed_provenance_mut::<Session>((token & !3) as usize);
            // SAFETY: event tokens name live list entries. Retired entries stay
            // mapped until this entire event batch has surrendered its tokens.
            unsafe {
                let current = &mut *session;
                if !current.active {
                    continue;
                }
                let kind = token & 3;
                let result = if kind == OWNER || event.events & sys::EPOLL_HUP != 0 {
                    Err(Errno(sys::EPIPE))
                } else if kind == CONTROL {
                    if current.owner < 0 {
                        current.admit(queue)
                    } else {
                        Err(Errno(sys::EPIPE))
                    }
                } else {
                    current
                        .progress()
                        .and_then(|()| current.interest(queue, false))
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
        while !garbage.is_null() {
            // SAFETY: all event borrowers have retired; unlink and unmap exactly once.
            unsafe {
                let next = (*garbage).garbage;
                ptr::drop_in_place(garbage);
                sys::unmap(garbage.cast(), core::mem::size_of::<Session>());
                garbage = next;
            }
        }
        if first.is_null() {
            // Drain admissions already queued before retiring the sole listener.
            accept(listener, queue, &mut first)?;
            if first.is_null() {
                return Ok(());
            }
        }
    }
}
