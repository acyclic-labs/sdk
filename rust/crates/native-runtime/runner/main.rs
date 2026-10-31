//! The Linux process runner: a small static image, without the C library,
//! that the native runtime execs once per owner. It clones each custodied
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

#[path = "../src/process_tree/record.rs"]
mod record;
mod sys;
#[path = "../src/process_tree/warden.rs"]
mod warden;

use core::ffi::{CStr, c_char, c_void};
use sys::{Errno, Fd, Pid};

/// Descriptors one request carries: custody directory, reply, the owner's
/// working directory and up to three streams.
const DESCRIPTORS: usize = 6;
/// Reply frame tags, each followed by a native-endian 32-bit value.
const STARTED: u8 = b'P';
const FAILED: u8 = b'E';
/// Stream slots in a request.
const NULL: u8 = 0;
const SENT: u8 = 1;
const CLOSED: u8 = 2;
/// Request flags.
const HAS_CWD: u8 = 1;
const HAS_UID: u8 = 2;
const HAS_GID: u8 = 4;
/// Resource limits a request carries, as soft and hard pairs.
const LIMITS: usize = 16;
/// The fixed fields before a request's strings: slots, flags, user, group,
/// owner, file-creation mask, limits, and the argument and environment
/// counts.
const FIXED: usize = 3 + 1 + 4 + 4 + 4 + 4 + 8 + LIMITS * 16 + 8 + 8;
/// The control socket, above the standard streams.
const CONTROL: Fd = 3;

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

/// Memory the runner maps for itself.
struct Mapped {
    base: *mut u8,
    length: usize,
}

impl Mapped {
    const EMPTY: Self = Self {
        base: core::ptr::null_mut(),
        length: 0,
    };

    /// Grows to at least `length` bytes, keeping the contents.
    fn reserve(&mut self, length: usize) -> Result<(), Errno> {
        if length <= self.length {
            return Ok(());
        }
        let length = length
            .checked_next_power_of_two()
            .ok_or(Errno(sys::ENOMEM))?
            .max(1 << 16);
        self.base = if self.base.is_null() {
            sys::map(length)?
        } else {
            // SAFETY: this whole mapping, which nothing else references.
            unsafe { sys::remap(self.base, self.length, length)? }
        };
        self.length = length;
        Ok(())
    }

    fn bytes(&mut self) -> &mut [u8] {
        if self.base.is_null() {
            return &mut [];
        }
        // SAFETY: this mapping is readable and writable for its length.
        unsafe { core::slice::from_raw_parts_mut(self.base, self.length) }
    }
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

/// One received request: its descriptors and body length.
struct Received {
    descriptors: [Fd; DESCRIPTORS],
    count: usize,
    length: usize,
}

impl Received {
    fn close(&self) {
        for fd in self.descriptors.iter().take(self.count) {
            sys::close(*fd);
        }
    }
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

fn word(bytes: &[u8]) -> usize {
    let mut word = [0; 8];
    if let Some(bytes) = bytes.get(..8) {
        word.copy_from_slice(bytes);
    }
    usize::from_ne_bytes(word)
}

fn array4(bytes: &[u8]) -> [u8; 4] {
    let mut array = [0; 4];
    if let Some(bytes) = bytes.get(..4) {
        array.copy_from_slice(bytes);
    }
    array
}

/// Where the program's standard stream comes from.
#[derive(Clone, Copy)]
enum Slot {
    /// The runner's null device, already in place.
    Null,
    Descriptor(Fd),
    Closed,
}

/// A request, ready to exec: pointers into the runner's request memory.
struct Spec {
    reply: Fd,
    directory: Fd,
    here: Fd,
    owner: Pid,
    slots: [Slot; 3],
    cwd: Option<*const c_char>,
    uid: Option<u32>,
    gid: Option<u32>,
    /// The owner's file-creation mask and resource limits at the request.
    mask: u32,
    signals: u64,
    limits: [[u64; 2]; LIMITS],
    program: *const c_char,
    /// The program has no slash, so the child's `PATH` finds it.
    search: bool,
    path: Option<*const c_char>,
    argv: *const *const c_char,
    envp: *const *const c_char,
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

/// Reads a request body and lays out its argument and environment vectors
/// after it.
fn spec(received: &Received, body: &mut Mapped) -> Result<Spec, Errno> {
    let invalid = Errno(sys::EINVAL);
    let descriptor = |index: usize| {
        received
            .descriptors
            .get(index)
            .copied()
            .filter(|_| index < received.count)
            .ok_or(invalid)
    };
    let length = received.length;
    let (slots, flags, uid, gid, owner, mask, signals, limits, argc, envc) = {
        let mut reader = Reader(body.bytes().get(..length).ok_or(invalid)?);
        let slots = reader.take(3)?;
        let flags = *reader.take(1)?.first().ok_or(invalid)?;
        let uid = reader.word()?;
        let gid = reader.word()?;
        let owner = Pid::try_from(reader.word()?).map_err(|_| invalid)?;
        let mask = reader.word()?;
        let signals = reader.wide()?;
        let mut limits = [[0; 2]; LIMITS];
        for limit in &mut limits {
            *limit = [reader.wide()?, reader.wide()?];
        }
        let argc = reader.count()?;
        let envc = reader.count()?;
        let mut kinds = [0; 3];
        kinds.copy_from_slice(slots);
        (
            kinds, flags, uid, gid, owner, mask, signals, limits, argc, envc,
        )
    };
    let mut next = 3;
    let mut streams = [Slot::Null; 3];
    for (stream, kind) in streams.iter_mut().zip(slots) {
        *stream = match kind {
            NULL => Slot::Null,
            SENT => {
                next += 1;
                Slot::Descriptor(descriptor(next - 1)?)
            }
            CLOSED => Slot::Closed,
            _ => return Err(invalid),
        };
    }
    // The vectors follow the body, aligned for pointers.
    let vectors = length
        .checked_next_multiple_of(8)
        .ok_or(Errno(sys::ENOMEM))?;
    let pointers = argc
        .checked_add(envc)
        .and_then(|count| count.checked_add(2))
        .and_then(|count| count.checked_mul(core::mem::size_of::<*const c_char>()))
        .ok_or(Errno(sys::ENOMEM))?;
    body.reserve(vectors.checked_add(pointers).ok_or(Errno(sys::ENOMEM))?)?;
    let base = body.base;
    // Shared, and only the body: the vectors after it are written below.
    let bytes: &[u8] = body.bytes().get(..length).ok_or(invalid)?;
    let mut offset = FIXED;
    let mut string = || -> Result<*const c_char, Errno> {
        let rest = bytes.get(offset..).ok_or(invalid)?;
        let end = rest.iter().position(|byte| *byte == 0).ok_or(invalid)?;
        let start = offset;
        offset += end + 1;
        Ok(base.wrapping_add(start).cast_const().cast())
    };
    let program = string()?;
    let cwd = if flags & HAS_CWD != 0 {
        Some(string()?)
    } else {
        None
    };
    let mut path = None;
    let vector = base.wrapping_add(vectors).cast::<*const c_char>();
    for index in 0..argc {
        let argument = string()?;
        // SAFETY: within the pointer area reserved above.
        unsafe { vector.add(index).write(argument) };
    }
    // SAFETY: within the pointer area reserved above.
    unsafe { vector.add(argc).write(core::ptr::null()) };
    let environment = vector.wrapping_add(argc + 1);
    for index in 0..envc {
        let variable = string()?;
        // SAFETY: a NUL-terminated string in this request.
        if unsafe { CStr::from_ptr(variable) }
            .to_bytes()
            .starts_with(b"PATH=")
        {
            path = Some(variable.wrapping_add(5));
        }
        // SAFETY: within the pointer area reserved above.
        unsafe { environment.add(index).write(variable) };
    }
    // SAFETY: within the pointer area reserved above.
    unsafe { environment.add(envc).write(core::ptr::null()) };
    // SAFETY: the program is a NUL-terminated string in this request.
    let search = !unsafe { CStr::from_ptr(program) }
        .to_bytes()
        .contains(&b'/');
    Ok(Spec {
        reply: descriptor(1)?,
        directory: descriptor(0)?,
        here: descriptor(2)?,
        owner,
        slots: streams,
        cwd,
        uid: (flags & HAS_UID != 0).then_some(uid),
        gid: (flags & HAS_GID != 0).then_some(gid),
        mask,
        signals,
        limits,
        program,
        search,
        path,
        argv: vector.cast_const(),
        envp: environment.cast_const(),
    })
}

/// In the cloned warden: runs the warden, and in the program it forks, execs.
fn warden(spec: &Spec) -> ! {
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
    // As a standard spawn does.
    sys::set_signal(sys::SIGPIPE, false);
    sys::set_signal_mask(spec.signals)?;
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

/// A cursor over a request body.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], Errno> {
        if length > self.0.len() {
            return Err(Errno(sys::EINVAL));
        }
        let (taken, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(taken)
    }

    fn word(&mut self) -> Result<u32, Errno> {
        Ok(u32::from_ne_bytes(array4(self.take(4)?)))
    }

    fn count(&mut self) -> Result<usize, Errno> {
        Ok(word(self.take(8)?))
    }

    fn wide(&mut self) -> Result<u64, Errno> {
        let mut wide = [0; 8];
        wide.copy_from_slice(self.take(8)?);
        Ok(u64::from_ne_bytes(wide))
    }
}
