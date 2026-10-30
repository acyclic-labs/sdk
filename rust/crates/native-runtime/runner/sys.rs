//! Raw Linux system calls for the runner, which links no C library. The
//! record and warden code calls these by the same names as the native
//! crate's `process_tree/sys.rs`.

use core::ffi::{CStr, c_char};

pub type Fd = i32;
pub type Pid = i32;

/// A system call's error number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Errno(pub i32);

// Error numbers, flags and signals shared by x86_64 and aarch64.
pub const EPERM: i32 = 1;
pub const ENOENT: i32 = 2;
pub const EINTR: i32 = 4;
pub const ENOMEM: i32 = 12;
pub const EACCES: i32 = 13;
pub const ENODEV: i32 = 19;
pub const ENOTDIR: i32 = 20;
pub const EINVAL: i32 = 22;
pub const EPIPE: i32 = 32;
pub const ETIMEDOUT: i32 = 110;
pub const ESTALE: i32 = 116;

pub const O_RDONLY: i32 = 0;
pub const O_RDWR: i32 = 2;
pub const O_CREAT: i32 = 0o100;
pub const O_EXCL: i32 = 0o200;
pub const O_CLOEXEC: i32 = 0o2_000_000;

pub const SIGHUP: i32 = 1;
pub const SIGINT: i32 = 2;
pub const SIGQUIT: i32 = 3;
pub const SIGKILL: i32 = 9;
pub const SIGPIPE: i32 = 13;
pub const SIGTERM: i32 = 15;
pub const SIGCHLD: i32 = 17;

const CLONE_PARENT: usize = 0x8000;
const AT_FDCWD: i32 = -100;
const LOCK_EX_NB: usize = 2 | 4;
const PR_SET_CHILD_SUBREAPER: usize = 36;
const RLIMIT_NOFILE: usize = 7;
const SIG_UNBLOCK: usize = 1;
const MSG_CMSG_CLOEXEC: usize = 0x4000_0000;
const PROT_READ_WRITE: usize = 1 | 2;
const MAP_PRIVATE_ANONYMOUS: usize = 0x02 | 0x20;
const MREMAP_MAYMOVE: usize = 1;

#[cfg(target_arch = "x86_64")]
mod number {
    pub const READ: usize = 0;
    pub const WRITE: usize = 1;
    pub const CLOSE: usize = 3;
    pub const MMAP: usize = 9;
    pub const RT_SIGACTION: usize = 13;
    pub const RT_SIGPROCMASK: usize = 14;
    pub const MREMAP: usize = 25;
    pub const GETPID: usize = 39;
    pub const RECVMSG: usize = 47;
    pub const CLONE: usize = 56;
    pub const EXECVE: usize = 59;
    pub const WAIT4: usize = 61;
    pub const KILL: usize = 62;
    pub const FLOCK: usize = 73;
    pub const CHDIR: usize = 80;
    pub const FCHDIR: usize = 81;
    pub const SETUID: usize = 105;
    pub const SETGID: usize = 106;
    pub const SETPGID: usize = 109;
    pub const GETPPID: usize = 110;
    pub const SETGROUPS: usize = 116;
    pub const GETPGID: usize = 121;
    pub const PRCTL: usize = 157;
    pub const EXIT_GROUP: usize = 231;
    pub const OPENAT: usize = 257;
    pub const UNLINKAT: usize = 263;
    pub const PPOLL: usize = 271;
    pub const DUP3: usize = 292;
    pub const PIPE2: usize = 293;
    pub const PRLIMIT64: usize = 302;
    pub const UMASK: usize = 95;
    pub const PIDFD_OPEN: usize = 434;
    pub const CLOSE_RANGE: usize = 436;
}

#[cfg(target_arch = "aarch64")]
mod number {
    pub const DUP3: usize = 24;
    pub const FLOCK: usize = 32;
    pub const UNLINKAT: usize = 35;
    pub const CHDIR: usize = 49;
    pub const FCHDIR: usize = 50;
    pub const OPENAT: usize = 56;
    pub const CLOSE: usize = 57;
    pub const PIPE2: usize = 59;
    pub const READ: usize = 63;
    pub const WRITE: usize = 64;
    pub const PPOLL: usize = 73;
    pub const EXIT_GROUP: usize = 94;
    pub const KILL: usize = 129;
    pub const RT_SIGACTION: usize = 134;
    pub const RT_SIGPROCMASK: usize = 135;
    pub const SETGID: usize = 144;
    pub const SETUID: usize = 146;
    pub const SETPGID: usize = 154;
    pub const GETPGID: usize = 155;
    pub const SETGROUPS: usize = 159;
    pub const PRCTL: usize = 167;
    pub const GETPID: usize = 172;
    pub const GETPPID: usize = 173;
    pub const RECVMSG: usize = 212;
    pub const MREMAP: usize = 216;
    pub const CLONE: usize = 220;
    pub const EXECVE: usize = 221;
    pub const MMAP: usize = 222;
    pub const WAIT4: usize = 260;
    pub const PRLIMIT64: usize = 261;
    pub const UMASK: usize = 166;
    pub const PIDFD_OPEN: usize = 434;
    pub const CLOSE_RANGE: usize = 436;
}

/// One system call with up to six arguments.
///
/// # Safety
///
/// The arguments must be valid for system call `number`.
#[cfg(target_arch = "x86_64")]
unsafe fn raw(number: usize, a: [usize; 6]) -> isize {
    let result: isize;
    // SAFETY: the caller's contract; `syscall` clobbers only rcx and r11.
    unsafe {
        core::arch::asm!(
            "syscall",
            inlateout("rax") number as isize => result,
            in("rdi") a[0], in("rsi") a[1], in("rdx") a[2],
            in("r10") a[3], in("r8") a[4], in("r9") a[5],
            lateout("rcx") _, lateout("r11") _,
            options(nostack),
        );
    }
    result
}

/// One system call with up to six arguments.
///
/// # Safety
///
/// The arguments must be valid for system call `number`.
#[cfg(target_arch = "aarch64")]
unsafe fn raw(number: usize, a: [usize; 6]) -> isize {
    let result: isize;
    // SAFETY: the caller's contract; `svc` clobbers only x0.
    unsafe {
        core::arch::asm!(
            "svc 0",
            in("x8") number,
            inlateout("x0") a[0] as isize => result,
            in("x1") a[1], in("x2") a[2], in("x3") a[3], in("x4") a[4], in("x5") a[5],
            options(nostack),
        );
    }
    result
}

/// A system call's result: its value, or the error it returned.
///
/// # Safety
///
/// As for [`raw`].
unsafe fn call(number: usize, a: [usize; 6]) -> Result<usize, Errno> {
    // SAFETY: the caller's contract.
    let result = unsafe { raw(number, a) };
    // The kernel returns -4095..=-1 for errors.
    if (-4095..0).contains(&result) {
        Err(Errno(i32::try_from(-result).unwrap_or(EINVAL)))
    } else {
        Ok(result.cast_unsigned())
    }
}

const fn arg(value: i32) -> usize {
    value as isize as usize
}

fn fd(value: usize) -> Fd {
    Fd::try_from(value).unwrap_or(-1)
}

pub fn open(path: &CStr, flags: i32) -> Result<Fd, Errno> {
    // SAFETY: a NUL-terminated path.
    unsafe {
        call(
            number::OPENAT,
            [arg(AT_FDCWD), path.as_ptr().addr(), arg(flags), 0, 0, 0],
        )
    }
    .map(fd)
}

pub fn openat(directory: Fd, name: &CStr, flags: i32, mode: u32) -> Result<Fd, Errno> {
    // SAFETY: a NUL-terminated name.
    unsafe {
        call(
            number::OPENAT,
            [
                arg(directory),
                name.as_ptr().addr(),
                arg(flags),
                mode as usize,
                0,
                0,
            ],
        )
    }
    .map(fd)
}

pub fn unlinkat(directory: Fd, name: &CStr) -> Result<(), Errno> {
    // SAFETY: a NUL-terminated name.
    unsafe {
        call(
            number::UNLINKAT,
            [arg(directory), name.as_ptr().addr(), 0, 0, 0, 0],
        )
    }
    .map(drop)
}

pub fn read(file: Fd, buffer: &mut [u8]) -> Result<usize, Errno> {
    // SAFETY: writable storage of the stated length.
    unsafe {
        call(
            number::READ,
            [arg(file), buffer.as_mut_ptr().addr(), buffer.len(), 0, 0, 0],
        )
    }
}

pub fn write(file: Fd, bytes: &[u8]) -> Result<usize, Errno> {
    // SAFETY: readable storage of the stated length.
    unsafe {
        call(
            number::WRITE,
            [arg(file), bytes.as_ptr().addr(), bytes.len(), 0, 0, 0],
        )
    }
}

pub fn close(file: Fd) {
    // SAFETY: closes a descriptor of this process.
    let _ = unsafe { call(number::CLOSE, [arg(file), 0, 0, 0, 0, 0]) };
}

/// A pipe whose ends a forked child keeps.
pub fn pipe() -> Result<[Fd; 2], Errno> {
    let mut ends = [0_i32; 2];
    // SAFETY: writable storage for two descriptors.
    unsafe { call(number::PIPE2, [ends.as_mut_ptr().addr(), 0, 0, 0, 0, 0]) }?;
    Ok(ends)
}

fn clone(flags: usize) -> Result<Pid, Errno> {
    // SAFETY: a fork-like clone with no new stack: the child continues here
    // with its own copy of this single-threaded process.
    unsafe { call(number::CLONE, [flags | arg(SIGCHLD), 0, 0, 0, 0, 0]) }.map(fd)
}

pub fn fork() -> Result<Pid, Errno> {
    clone(0)
}

/// Clones a child of this process's parent.
pub fn fork_sibling() -> Result<Pid, Errno> {
    clone(CLONE_PARENT)
}

pub fn getpid() -> Pid {
    // SAFETY: no arguments.
    unsafe { call(number::GETPID, [0; 6]) }.map_or(-1, fd)
}

pub fn getppid() -> Pid {
    // SAFETY: no arguments.
    unsafe { call(number::GETPPID, [0; 6]) }.map_or(-1, fd)
}

pub fn getpgrp() -> Pid {
    // SAFETY: this process's own group.
    unsafe { call(number::GETPGID, [0; 6]) }.map_or(-1, fd)
}

pub fn setpgid(pid: Pid, group: Pid) -> Result<(), Errno> {
    // SAFETY: changes the group of this process or its unexeced child.
    unsafe { call(number::SETPGID, [arg(pid), arg(group), 0, 0, 0, 0]) }.map(drop)
}

pub fn kill(pid: Pid, signal: i32) -> Result<(), Errno> {
    // SAFETY: sends a signal; no memory is involved.
    unsafe { call(number::KILL, [arg(pid), arg(signal), 0, 0, 0, 0]) }.map(drop)
}

/// Waits for `pid` and returns it with its raw status.
pub fn wait(pid: Pid, flags: i32) -> Result<(Pid, i32), Errno> {
    let mut status = 0_i32;
    // SAFETY: writable status storage and no usage output.
    let reaped = unsafe {
        call(
            number::WAIT4,
            [arg(pid), (&raw mut status).addr(), arg(flags), 0, 0, 0],
        )
    }?;
    Ok((fd(reaped), status))
}

/// Takes `file`'s exclusive lock without waiting.
pub fn try_lock(file: Fd) -> Result<(), Errno> {
    // SAFETY: locks an open file of this process.
    unsafe { call(number::FLOCK, [arg(file), LOCK_EX_NB, 0, 0, 0, 0]) }.map(drop)
}

/// Sets `signal`'s action to ignore it, or to its default.
pub fn set_signal(signal: i32, ignore: bool) {
    // The kernel's action: handler, flags, restorer and mask; at most four
    // words, and all-zero but the handler for these two actions.
    let action = [usize::from(ignore), 0, 0, 0];
    // SAFETY: a kernel-sized action and an eight-byte signal set size.
    let _ = unsafe {
        call(
            number::RT_SIGACTION,
            [arg(signal), action.as_ptr().addr(), 0, 8, 0, 0],
        )
    };
}

pub fn unblock(signal: i32) {
    let Ok(bit) = u32::try_from(signal - 1) else {
        return;
    };
    let set = 1_u64.checked_shl(bit).unwrap_or(0);
    // SAFETY: a kernel-sized signal set and no old-set output.
    let _ = unsafe {
        call(
            number::RT_SIGPROCMASK,
            [SIG_UNBLOCK, (&raw const set).addr(), 0, 8, 0, 0],
        )
    };
}

pub fn exit(code: i32) -> ! {
    loop {
        // SAFETY: ends this process.
        let _ = unsafe { call(number::EXIT_GROUP, [arg(code), 0, 0, 0, 0, 0]) };
    }
}

pub fn subreaper() -> Result<(), Errno> {
    // SAFETY: marks this process as the reaper of its orphaned descendants.
    unsafe { call(number::PRCTL, [PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0, 0]) }.map(drop)
}

pub fn pidfd(pid: Pid) -> Result<Fd, Errno> {
    // SAFETY: a PID and zero flags.
    unsafe { call(number::PIDFD_OPEN, [arg(pid), 0, 0, 0, 0, 0]) }.map(fd)
}

/// One descriptor `poll` waits on.
#[repr(C)]
pub struct PollFd {
    fd: Fd,
    events: i16,
    revents: i16,
}

impl PollFd {
    pub const fn readable(fd: Fd) -> Self {
        Self {
            fd,
            events: 1,
            revents: 0,
        }
    }

    pub const fn ready(&self) -> bool {
        self.revents != 0
    }
}

/// Waits without a timeout until one of `fds` is ready.
pub fn poll(fds: &mut [PollFd]) -> Result<usize, Errno> {
    // SAFETY: initialized entries of the stated count; no timeout or mask.
    unsafe {
        call(
            number::PPOLL,
            [fds.as_mut_ptr().addr(), fds.len(), 0, 0, 8, 0],
        )
    }
}

/// Closes the descriptors `first..=last`, or fails: an inherited
/// descriptor left open (a reply pipe's write end) would keep its reader
/// from ever seeing end of file.
pub fn close_range(first: u32, last: u32) -> Result<(), Errno> {
    // SAFETY: closes only descriptors of this process.
    if unsafe {
        call(
            number::CLOSE_RANGE,
            [first as usize, last as usize, 0, 0, 0, 0],
        )
    }
    .is_ok()
    {
        return Ok(());
    }
    // Without close_range (an old kernel, or a filter refusing it): close
    // each descriptor up to the limit.
    let mut limit = [0_u64; 2];
    // SAFETY: writable limit storage for this process.
    unsafe {
        call(
            number::PRLIMIT64,
            [0, RLIMIT_NOFILE, 0, limit.as_mut_ptr().addr(), 0, 0],
        )
    }?;
    let end = u32::try_from(limit[0]).unwrap_or(u32::MAX).min(last);
    for file in first..=end {
        if let Ok(file) = Fd::try_from(file) {
            close(file);
        }
    }
    Ok(())
}

// The runner's own calls.

/// Makes `target` a copy of `file`, closed on exec when `cloexec`.
pub fn dup_onto(file: Fd, target: Fd, cloexec: bool) -> Result<(), Errno> {
    let flags = if cloexec { arg(O_CLOEXEC) } else { 0 };
    // SAFETY: replaces a descriptor of this process.
    unsafe { call(number::DUP3, [arg(file), arg(target), flags, 0, 0, 0]) }.map(drop)
}

pub fn fchdir(directory: Fd) -> Result<(), Errno> {
    // SAFETY: changes this process's working directory.
    unsafe { call(number::FCHDIR, [arg(directory), 0, 0, 0, 0, 0]) }.map(drop)
}

pub fn chdir(path: &CStr) -> Result<(), Errno> {
    // SAFETY: a NUL-terminated path.
    unsafe { call(number::CHDIR, [path.as_ptr().addr(), 0, 0, 0, 0, 0]) }.map(drop)
}

/// Sets this process's file-creation mask.
pub fn set_mask(mask: u32) {
    // SAFETY: umask cannot fail; it returns the previous mask.
    let _ = unsafe { call(number::UMASK, [mask as usize, 0, 0, 0, 0, 0]) };
}

/// Sets this process's soft and hard limit for `resource`.
pub fn set_limit(resource: usize, limit: [u64; 2]) -> Result<(), Errno> {
    // SAFETY: a readable soft and hard pair for this process.
    unsafe {
        call(
            number::PRLIMIT64,
            [0, resource, limit.as_ptr().addr(), 0, 0, 0],
        )
    }
    .map(drop)
}

pub fn setgid(id: u32) -> Result<(), Errno> {
    // SAFETY: changes this single-threaded process's group.
    unsafe { call(number::SETGID, [id as usize, 0, 0, 0, 0, 0]) }.map(drop)
}

pub fn setuid(id: u32) -> Result<(), Errno> {
    // SAFETY: changes this single-threaded process's user.
    unsafe { call(number::SETUID, [id as usize, 0, 0, 0, 0, 0]) }.map(drop)
}

pub fn clear_groups() -> Result<(), Errno> {
    // SAFETY: an empty supplementary group list.
    unsafe { call(number::SETGROUPS, [0; 6]) }.map(drop)
}

/// Execs `program`; returns only its error.
///
/// # Safety
///
/// `program` is NUL-terminated, and `argv` and `envp` are null-terminated
/// arrays of NUL-terminated strings.
pub unsafe fn execve(
    program: *const c_char,
    argv: *const *const c_char,
    envp: *const *const c_char,
) -> Errno {
    // SAFETY: the caller's contract.
    match unsafe {
        call(
            number::EXECVE,
            [program.addr(), argv.addr(), envp.addr(), 0, 0, 0],
        )
    } {
        Err(error) => error,
        Ok(_) => Errno(EINVAL),
    }
}

/// Maps `length` new bytes of private memory.
pub fn map(length: usize) -> Result<*mut u8, Errno> {
    // SAFETY: a new private anonymous mapping.
    unsafe {
        call(
            number::MMAP,
            [
                0,
                length,
                PROT_READ_WRITE,
                MAP_PRIVATE_ANONYMOUS,
                usize::MAX,
                0,
            ],
        )
    }
    .map(|base| core::ptr::with_exposed_provenance_mut(base))
}

/// Grows a mapping from `map`, moving it if needed.
///
/// # Safety
///
/// `base` and `old` name a whole mapping this process owns.
pub unsafe fn remap(base: *mut u8, old: usize, length: usize) -> Result<*mut u8, Errno> {
    // SAFETY: the caller's contract.
    unsafe {
        call(
            number::MREMAP,
            [base.expose_provenance(), old, length, MREMAP_MAYMOVE, 0, 0],
        )
    }
    .map(|base| core::ptr::with_exposed_provenance_mut(base))
}

/// The socket message header, as the kernel lays it out on 64-bit targets.
#[repr(C)]
pub struct MessageHeader {
    pub name: usize,
    pub name_length: u32,
    pub iov: *mut IoVector,
    pub iov_length: usize,
    pub control: *mut u8,
    pub control_length: usize,
    pub flags: i32,
}

#[repr(C)]
pub struct IoVector {
    pub base: *mut u8,
    pub length: usize,
}

pub const MSG_CTRUNC: i32 = 8;
pub const SOL_SOCKET: i32 = 1;
pub const SCM_RIGHTS: i32 = 1;

/// Receives one message, closing received descriptors on exec.
pub fn receive(socket: Fd, header: &mut MessageHeader) -> Result<usize, Errno> {
    // SAFETY: a header naming writable buffers of its stated lengths.
    unsafe {
        call(
            number::RECVMSG,
            [
                arg(socket),
                core::ptr::from_mut(header).addr(),
                MSG_CMSG_CLOEXEC,
                0,
                0,
                0,
            ],
        )
    }
}
