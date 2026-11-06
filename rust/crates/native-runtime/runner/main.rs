//! The Linux process runner: a small static image, without the C library,
//! that the native runtime execs for each launch. It clones the custodied
//! tree's warden as the owner's own child (`CLONE_PARENT`).
//!
//! Exec'd, it holds nothing of the owner: no heap, locks, threads or fork
//! handlers, so the clone and the warden's fork copy only this image's few
//! pages, whatever host loaded the library. Its standard input is the
//! control socket; it moves that to descriptor 3, closes every other
//! inherited descriptor and serves until the owner closes the socket.
//!
//! A request carries the custody directory, a reply pipe, the owner's
//! working directory and the child's standard streams as descriptors, then
//! the owner's PID, the program, its arguments and its whole environment.
//! The cloned warden writes its PID to the reply pipe first, and an exec
//! failure after it; the reply ends once the program has exec'd and the
//! warden closed its copy, as a forked spawn's status pipe does.

#![no_std]
#![no_main]

#[path = "../src/execution/linux/entry.rs"]
mod entry;
#[path = "../src/process_tree/record.rs"]
mod record;
mod request;
mod sys;
use request::{DESCRIPTORS, Mapped, Received, Slot, Spec, array4, spec, word};
#[path = "../src/process_tree/warden.rs"]
mod warden;

use core::ffi::{CStr, c_char, c_void};
use sys::{Errno, Fd, Pid};

/// The control socket, above the standard streams.
const CONTROL: Fd = 3;
/// Reply frame tags, each followed by a native-endian 32-bit value.
const STARTED: u8 = b'P';
const FAILED: u8 = b'E';

#[cfg(target_arch = "x86_64")]
core::arch::global_asm!(
    ".globl _start",
    "_start:",
    "xor ebp, ebp",
    "and rsp, -16",
    "call {entry}",
    "ud2",
    entry = sym start,
);

#[cfg(target_arch = "aarch64")]
core::arch::global_asm!(
    ".globl _start",
    "_start:",
    "mov x29, xzr",
    "mov x30, xzr",
    "bl {entry}",
    "brk #0",
    entry = sym start,
);

extern "C" fn start() -> ! {
    let code = match prepare() {
        Ok(()) => {
            let mut body = Mapped::EMPTY;
            while let Ok(Some(received)) = receive(&mut body) {
                launch(received, &mut body);
            }
            0
        }
        Err(_) => 1,
    };
    sys::exit(code)
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    sys::exit(127)
}

/// Keeps the control socket, received on standard input, and the null
/// device as the standard streams; closes everything else inherited. A
/// write to a reply its owner abandoned must not end the runner.
fn prepare() -> Result<(), Errno> {
    sys::dup_onto(0, CONTROL, true)?;
    sys::close_range(CONTROL.cast_unsigned() + 1, u32::MAX)?;
    sys::dup_onto(1, 0, false)?;
    sys::set_signal(sys::SIGPIPE, true);
    Ok(())
}

// The C library's memory functions, which compiled code calls. Volatile
// accesses keep these loops from being compiled back into calls to
// themselves.

/// Copies `count` bytes forwards, or backwards.
///
/// # Safety
///
/// `count` bytes are writable at `to` and readable at `from`.
unsafe fn copy(to: *mut c_void, from: *const c_void, count: usize, backwards: bool) -> *mut c_void {
    let (bytes, source) = (to.cast::<u8>(), from.cast::<u8>());
    for step in 0..count {
        let index = if backwards { count - 1 - step } else { step };
        // SAFETY: the caller's contract.
        unsafe {
            bytes
                .add(index)
                .write_volatile(source.add(index).read_volatile())
        };
    }
    to
}

/// # Safety
///
/// `count` bytes are writable at `to` and readable at `from`.
#[unsafe(no_mangle)]
unsafe extern "C" fn memcpy(to: *mut c_void, from: *const c_void, count: usize) -> *mut c_void {
    // SAFETY: the caller's contract.
    unsafe { copy(to, from, count, false) }
}

/// # Safety
///
/// As for `memcpy`; the ranges may overlap.
#[unsafe(no_mangle)]
unsafe extern "C" fn memmove(to: *mut c_void, from: *const c_void, count: usize) -> *mut c_void {
    // SAFETY: the caller's contract; each byte is read before any write
    // reaches it, copying away from the overlap.
    unsafe { copy(to, from, count, to.cast_const() > from) }
}

/// # Safety
///
/// `count` bytes are writable at `to`.
#[unsafe(no_mangle)]
unsafe extern "C" fn memset(to: *mut c_void, value: i32, count: usize) -> *mut c_void {
    let bytes = to.cast::<u8>();
    // As the C function: the value's low byte.
    let [byte, ..] = value.to_le_bytes();
    for index in 0..count {
        // SAFETY: the caller's contract.
        unsafe { bytes.add(index).write_volatile(byte) };
    }
    to
}

/// # Safety
///
/// `string` is NUL-terminated.
#[unsafe(no_mangle)]
unsafe extern "C" fn strlen(string: *const c_char) -> usize {
    let mut length = 0;
    // SAFETY: the caller's contract: every byte up to the NUL is readable.
    while unsafe { string.add(length).read_volatile() } != 0 {
        length += 1;
    }
    length
}

/// The unwinding personality the precompiled core's unwind tables name. The
/// runner aborts on panic, so nothing unwinds and this never runs.
#[unsafe(no_mangle)]
extern "C" fn rust_eh_personality() {}

/// # Safety
///
/// `count` bytes are readable at `left` and `right`.
#[unsafe(no_mangle)]
unsafe extern "C" fn memcmp(left: *const c_void, right: *const c_void, count: usize) -> i32 {
    let (left, right) = (left.cast::<u8>(), right.cast::<u8>());
    for index in 0..count {
        // SAFETY: the caller's contract.
        let (a, b) = unsafe {
            (
                left.add(index).read_volatile(),
                right.add(index).read_volatile(),
            )
        };
        if a != b {
            return i32::from(a) - i32::from(b);
        }
    }
    0
}

/// # Safety
///
/// As for `memcmp`.
#[unsafe(no_mangle)]
unsafe extern "C" fn bcmp(left: *const c_void, right: *const c_void, count: usize) -> i32 {
    // SAFETY: the caller's contract.
    unsafe { memcmp(left, right, count) }
}

fn read_exact(mut bytes: &mut [u8]) -> Result<(), Errno> {
    while !bytes.is_empty() {
        match sys::read(CONTROL, bytes) {
            Ok(0) => return Err(Errno(sys::EPIPE)),
            Ok(read) => bytes = bytes.get_mut(read..).unwrap_or_default(),
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Reads one request into `body`. `None` once the owner has closed the
/// socket; an error leaves the stream unusable.
fn receive(body: &mut Mapped) -> Result<Option<Received>, Errno> {
    // A control message header, then the descriptors.
    const HEADER: usize = 16;
    let mut length = [0_u8; 8];
    let mut space = ControlSpace([0; 64]);
    let mut vector = sys::IoVector {
        base: length.as_mut_ptr(),
        length: length.len(),
    };
    let mut header = sys::MessageHeader {
        name: 0,
        name_length: 0,
        iov: &raw mut vector,
        iov_length: 1,
        control: space.0.as_mut_ptr(),
        control_length: space.0.len(),
        flags: 0,
    };
    let read = loop {
        match sys::receive(CONTROL, &mut header) {
            Ok(read) => break read,
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    };
    let mut received = Received {
        descriptors: [-1; DESCRIPTORS],
        count: 0,
        length: 0,
    };
    // Each control message: its length, level and type, then its data, the
    // next one aligned to eight bytes.
    let control = &space.0;
    let filled = header.control_length.min(control.len());
    let mut offset = 0;
    while let Some(message) = control
        .get(offset..filled)
        .filter(|rest| rest.len() >= HEADER)
    {
        let message_length = message.get(..8).map_or(0, word).min(message.len());
        let level = i32::from_ne_bytes(message.get(8..12).map(array4).unwrap_or_default());
        let kind = i32::from_ne_bytes(message.get(12..16).map(array4).unwrap_or_default());
        if message_length < HEADER {
            break;
        }
        if level == sys::SOL_SOCKET && kind == sys::SCM_RIGHTS {
            let data = message.get(HEADER..message_length).unwrap_or_default();
            for fd in data.chunks_exact(4) {
                let fd = Fd::from_ne_bytes(array4(fd));
                if let Some(slot) = received.descriptors.get_mut(received.count) {
                    *slot = fd;
                    received.count += 1;
                } else {
                    sys::close(fd);
                }
            }
        }
        offset += message_length.next_multiple_of(8);
    }
    if read == 0 {
        received.close();
        return Ok(None);
    }
    let read = read_exact(length.get_mut(read..).unwrap_or_default())
        .and_then(|()| usize::try_from(u64::from_ne_bytes(length)).map_err(|_| Errno(sys::EINVAL)))
        .and_then(|length| {
            body.reserve(length)?;
            read_exact(body.bytes().get_mut(..length).unwrap_or_default())?;
            Ok(length)
        });
    match read {
        Ok(length) if header.flags & sys::MSG_CTRUNC == 0 => {
            received.length = length;
            Ok(Some(received))
        }
        Ok(_) => {
            received.close();
            Err(Errno(sys::EINVAL))
        }
        Err(error) => {
            received.close();
            Err(error)
        }
    }
}

/// Space for control messages, aligned as their headers are.
#[repr(C, align(8))]
struct ControlSpace([u8; 64]);

impl Received {
    fn close(&self) {
        for fd in self.descriptors.iter().take(self.count) {
            sys::close(*fd);
        }
    }
}

/// Clones one warden as the owner's child. Every failure is reported on the
/// request's reply pipe, which closes when this returns.
fn launch(received: Received, body: &mut Mapped) {
    let reply = received.descriptors.get(1).copied().unwrap_or(-1);
    match spec(&received, body) {
        Ok(spec) => match sys::fork_sibling() {
            Ok(0) => warden(&spec),
            Ok(_) => {}
            Err(Errno(error)) => report(reply, FAILED, error),
        },
        Err(Errno(error)) => report(reply, FAILED, error),
    }
    received.close();
}

/// In the cloned warden: runs the warden, and in the program it forks, execs.
fn warden(spec: &Spec) -> ! {
    if let Some(activity) = spec.activity {
        released(spec, activity);
    }
    report(spec.reply, STARTED, sys::getpid());
    // Still the runner's group, which the warden returns to.
    let home = sys::getpgrp();
    let Errno(error) = match warden::enter(spec.directory, spec.owner, home) {
        Ok(()) => exec(spec),
        Err(error) => error,
    };
    report(spec.reply, FAILED, error);
    sys::exit(127)
}

/// An owner-exit action holds no process tree or volume device. The original
/// activity pipe also belongs to any admitted producer: EOF proves that no
/// such producer can perform a late effect after this action.
fn released(spec: &Spec, activity: Fd) -> ! {
    let result = (|| {
        sys::setpgid(0, 0)?;
        // Loss of the dispatcher is independent of this admitted action.
        sys::close(CONTROL);
        report(spec.reply, STARTED, sys::getpid());
        sys::close(spec.reply);
        let execute = released_byte(spec.directory)?;
        loop {
            match sys::read(activity, &mut [0; 256]) {
                Ok(0) => break,
                Ok(_) | Err(Errno(sys::EINTR)) => {}
                Err(error) => return Err(error),
            }
        }
        sys::close(spec.directory);
        sys::close(activity);
        if execute {
            return Err(exec(spec));
        }
        Ok(())
    })();
    sys::exit(if result.is_ok() { 0 } else { 127 })
}

/// EOF releases the action; an explicit byte cancels it. Interrupted reads
/// resume the same producer event, without probing or a timeout.
fn released_byte(fd: Fd) -> Result<bool, Errno> {
    loop {
        match sys::read(fd, &mut [0]) {
            Ok(count) => return Ok(count == 0),
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
}

/// In the program: sets up what the request asks for and execs. Returns the
/// error that stopped it.
fn exec(spec: &Spec) -> Errno {
    match prepare_program(spec) {
        Err(error) => error,
        // SAFETY: NUL-terminated program, argument and environment vectors.
        Ok(()) if !spec.search => unsafe { sys::execve(spec.program, spec.argv, spec.envp) },
        Ok(()) => search(spec),
    }
}

fn prepare_program(spec: &Spec) -> Result<(), Errno> {
    // What a fork of the owner would have inherited at this request, set
    // while the program still has the runner's privileges.
    sys::set_mask(spec.mask);
    for (resource, limit) in (0..).zip(&spec.limits) {
        sys::set_limit(resource, *limit)?;
    }
    for (target, slot) in (0..).zip(spec.slots) {
        // The runner's own descriptors are above the standard three.
        match slot {
            Slot::Null => {}
            Slot::Descriptor(fd) => sys::dup_onto(fd, target, false)?,
            Slot::Closed => sys::close(target),
        }
    }
    sys::fchdir(spec.here)?;
    if let Some(cwd) = spec.cwd {
        // SAFETY: a NUL-terminated string in the request.
        sys::chdir(unsafe { CStr::from_ptr(cwd) })?;
    }
    if let Some(gid) = spec.gid {
        sys::setgid(gid)?;
    }
    if let Some(uid) = spec.uid {
        match sys::clear_groups() {
            Ok(()) | Err(Errno(sys::EPERM)) => {}
            Err(error) => return Err(error),
        }
        sys::setuid(uid)?;
    }
    // Other ignored dispositions came from the current owner when this
    // runner was exec'd. Undo only the runner/warden's own changes: std
    // spawn defaults SIGPIPE; the warden needed SIGCHLD to reap its children.
    sys::set_signal_checked(sys::SIGPIPE, false)?;
    if spec.ignored & (1 << (sys::SIGCHLD - 1)) != 0 {
        sys::set_signal_checked(sys::SIGCHLD, true)?;
    }
    sys::set_signal_mask(spec.signals)?;
    if let Some(boundary) = &spec.boundary {
        // SAFETY: spec checked the complete Instruction range. Its start
        // FIXED+4 is eight-byte aligned in this mapping and remapping ended
        // before spec formed this pointer. The original request stays live.
        let filter = unsafe { core::slice::from_raw_parts(boundary.filter, boundary.count) };
        entry::enter(boundary.ruleset, filter).map_err(Errno)?;
    }
    Ok(())
}

/// Tries the program in each directory of the child's `PATH`, as `execvp`
/// does without running scripts through a shell.
fn search(spec: &Spec) -> Errno {
    // SAFETY: NUL-terminated strings in the request.
    let (program, path) = unsafe {
        (
            CStr::from_ptr(spec.program).to_bytes(),
            spec.path.map_or(b"/bin:/usr/bin".as_slice(), |path| {
                CStr::from_ptr(path).to_bytes()
            }),
        )
    };
    if program.is_empty() {
        return Errno(sys::ENOENT);
    }
    let mut candidate = Mapped::EMPTY;
    if let Err(error) = candidate.reserve(path.len() + program.len() + 2) {
        return error;
    }
    let mut denied = false;
    for directory in path.split(|byte| *byte == b':') {
        let bytes = candidate.bytes();
        let mut length = 0;
        let separator: &[u8] = if directory.is_empty() { b"" } else { b"/" };
        for part in [directory, separator, program, b"\0"] {
            if let Some(target) = bytes.get_mut(length..length + part.len()) {
                target.copy_from_slice(part);
            }
            length += part.len();
        }
        // SAFETY: a NUL-terminated candidate path and the request's vectors.
        match unsafe { sys::execve(bytes.as_ptr().cast(), spec.argv, spec.envp) } {
            Errno(sys::EACCES) => denied = true,
            Errno(sys::ENOENT | sys::ENOTDIR | sys::ESTALE | sys::ENODEV | sys::ETIMEDOUT) => {}
            error => return error,
        }
    }
    Errno(if denied { sys::EACCES } else { sys::ENOENT })
}

/// Writes one reply frame. A frame is far below the pipe's atomic size.
fn report(reply: Fd, tag: u8, value: i32) {
    let mut frame = [tag, 0, 0, 0, 0];
    if let Some(rest) = frame.get_mut(1..) {
        rest.copy_from_slice(&value.to_ne_bytes());
    }
    let _ = sys::write(reply, &frame);
}
