//! Landlock binds filesystem objects; seccomp seals the escape mechanisms.
//! All allocation and ruleset construction happen before fork. The child hook
//! uses only native system calls and already-owned buffers.

#![allow(
    unsafe_code,
    reason = "checked Landlock and seccomp FFI in the native execution adapter"
)]

use super::{
    BoundPath, ExecutionAccess, ExecutionGrant, ExecutionNetwork, Grant, failure, refused,
    unavailable,
};
use crate::{
    NativeOperationKind::{Read, Write},
    NativeReadMeter,
    meter::native_control_attempt,
};
use std::{
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{ffi::OsStrExt, fs::MetadataExt, process::CommandExt},
    },
    path::Path,
    process::Command,
};

mod entry;
mod sys;
pub(super) use entry::Instruction;

const READ: u64 = (1 << 0) | (1 << 2) | (1 << 3);
const URANDOM_DEVICE: u64 = 0x109;
const WRITE: u64 = (1 << 1)
    | (1 << 4)
    | (1 << 5)
    | (1 << 7)
    | (1 << 8)
    | (1 << 9)
    | (1 << 10)
    | (1 << 12)
    | (1 << 13)
    | (1 << 14);
const FILE: u64 = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 14);
const CONNECT_TCP: u64 = 1 << 1;

pub(super) fn open_beneath(
    root: &File,
    relative: &Path,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<File> {
    open_beneath_with_flags(root, relative, 0, meter)
}

fn open_beneath_with_flags(
    root: &File,
    relative: &Path,
    flags: i32,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<File> {
    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }
    let bytes = relative.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return Err(refused("invalid relative grant path"));
    }
    let count = bytes
        .len()
        .checked_add(1)
        .ok_or_else(|| refused("relative grant path length overflow"))?;
    let mut operation = meter
        .map(|meter| crate::NativeOperationReservation::new(meter, Read, 1))
        .transpose()?;
    // Declare credit before storage: the buffer is physically freed before
    // its original ledger releases capacity, including every error return.
    let mut allocation = meter
        .map(|meter| crate::ReadAllocation::borrowed(meter, count))
        .transpose()?;
    let mut path = Vec::new();
    path.try_reserve_exact(count).map_err(|error| match meter {
        Some(meter) => meter.allocation_failed(error),
        None => io::Error::new(io::ErrorKind::OutOfMemory, error),
    })?;
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(path.capacity())?;
    }
    path.extend_from_slice(bytes);
    path.push(0);
    let how = OpenHow {
        flags: (libc::O_PATH | libc::O_CLOEXEC | flags) as u64,
        mode: 0,
        // BENEATH | NO_SYMLINKS: resolution stays under this native object,
        // even when an ancestor is renamed or replaced during the syscall.
        resolve: 0x08 | 0x04,
    };
    let open = || {
        // SAFETY: the borrowed root, terminated path and complete zero-reserved
        // open_how remain live for the kernel's checked relative resolution.
        let descriptor = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                root.as_raw_fd(),
                path.as_ptr(),
                &raw const how,
                std::mem::size_of::<OpenHow>(),
            )
        };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            return Err(if error.raw_os_error() == Some(libc::ENOSYS) {
                unavailable("rooted execution path resolution requires openat2")
            } else {
                failure(error)
            });
        }
        let descriptor =
            i32::try_from(descriptor).map_err(|_| refused("invalid grant root handle"))?;
        // SAFETY: successful openat2 transfers this unique descriptor to File,
        // before a possible late operation-recording failure.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    };
    match &mut operation {
        Some(operation) => operation.control(open),
        None => open(),
    }
}

#[repr(C)]
struct Ruleset {
    filesystem: u64,
    network: u64,
    scoped: u64,
}

#[repr(C, packed)]
struct PathRule {
    access: u64,
    parent: i32,
}

#[repr(C)]
struct PortRule {
    access: u64,
    port: u64,
}

/// Only the two fixed computation devices receive implicit path access. Use
/// the held object's metadata, so a substituted name cannot grant a subtree
/// or an unrelated device before the kernel takes its own rule reference.
fn computation_device(
    path: &Path,
    device: u64,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<File> {
    let root = native_control_attempt(meter, Read, || BoundPath::open_path(path))?;
    let metadata = native_control_attempt(meter, Read, || root.metadata().map_err(failure))?;
    if !entry::character_device(metadata.mode(), metadata.rdev(), device) {
        return Err(refused("execution computation device was replaced"));
    }
    Ok(root)
}

pub(super) struct Prepared {
    ruleset: OwnedFd,
    cwd: File,
}

impl Prepared {
    pub(super) fn working_directory(&self) -> &File {
        &self.cwd
    }

    pub(super) fn encode(
        &self,
        filter: &[Instruction],
        body: &mut Vec<u8>,
    ) -> io::Result<std::os::fd::RawFd> {
        // The original request carries one extra descriptor and its native
        // BPF bytes. Both images share Instruction; the child borrows these
        // bytes after decoding, with no policy allocation or translation.
        body.extend_from_slice(
            &u32::try_from(filter.len())
                .map_err(|_| refused("invalid execution filter length"))?
                .to_ne_bytes(),
        );
        for instruction in filter {
            body.extend_from_slice(&instruction.code.to_ne_bytes());
            body.extend_from_slice(&[instruction.jt, instruction.jf]);
            body.extend_from_slice(&instruction.k.to_ne_bytes());
        }
        Ok(self.ruleset.as_raw_fd())
    }

    pub(super) fn new(
        grant: &Grant,
        root: File,
        cwd: Option<&Path>,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        // ABI 6 supplies signal and abstract-UNIX scoping; earlier ABIs cannot
        // prove this contract. Never silently discard an unsupported right.
        let abi = native_control_attempt(meter, Read, || {
            // SAFETY: VERSION accepts a null attribute and zero attribute length.
            let abi = unsafe {
                libc::syscall(
                    libc::SYS_landlock_create_ruleset,
                    std::ptr::null::<()>(),
                    0,
                    1,
                )
            };
            if abi < 0 {
                return Err(unavailable(format!(
                    "Landlock unavailable: {}",
                    io::Error::last_os_error()
                )));
            }
            Ok(abi)
        })?;
        if abi < 6 {
            return Err(unavailable("Landlock ABI 6 or later is required"));
        }
        let rules = Ruleset {
            // All ABI 5 filesystem rights are handled, including device ioctl.
            filesystem: (1 << 16) - 1,
            network: if grant.network == ExecutionNetwork::On {
                0
            } else {
                3
            },
            scoped: 3,
        };
        let ruleset = native_control_attempt(meter, Write, || {
            // SAFETY: initialized ABI 6 attributes, correct length, zero flags.
            let descriptor = syscall(|| unsafe {
                libc::syscall(
                    libc::SYS_landlock_create_ruleset,
                    &raw const rules,
                    std::mem::size_of::<Ruleset>(),
                    0,
                )
            })?;
            let descriptor =
                i32::try_from(descriptor).map_err(|_| refused("invalid Landlock handle"))?;
            // SAFETY: landlock_create_ruleset returned this uniquely owned fd.
            // Acquire RAII ownership before checking a late recording failure.
            Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
        })?;
        // Resolve the selected directory beneath the original verified root,
        // rather than trusting the earlier canonical path at child launch.
        // The root itself needs no additional open or duplicate descriptor.
        let relative = cwd
            .unwrap_or(&grant.workspace)
            .strip_prefix(&grant.workspace.path)
            .map_err(|_| refused("execution working directory escapes the granted workspace"))?;
        let directory = if relative.as_os_str().is_empty() {
            None
        } else {
            Some(open_beneath_with_flags(
                &root,
                relative,
                libc::O_DIRECTORY,
                meter,
            )?)
        };
        let prepared = Self {
            ruleset,
            cwd: directory.unwrap_or(root),
        };
        let workspace = READ
            | if grant.access == ExecutionAccess::WorkspaceWrite {
                WRITE
            } else {
                0
            };
        // The pinned workspace handle is the exact object just verified by
        // binding; when cwd is nested, the reopened root can already close.
        prepared.path(
            &grant.workspace._object,
            workspace,
            grant.workspace.is_dir,
            meter,
        )?;
        // The kernel ruleset owns the inserted inode reference. The grant
        // independently pins every original object against identity reuse;
        // another live descriptor per rule buys no additional guarantee.
        for root in &grant.read {
            prepared.path(&root.bind_controlled(meter)?, READ, root.is_dir, meter)?;
        }
        // Null output and entropy are capabilities of computation. No arbitrary
        // device, host home, procfs or temporary tree is exposed.
        for (path, access, device) in [
            (
                Path::new("/dev/null"),
                (1 << 1) | (1 << 2),
                entry::NULL_DEVICE,
            ),
            (Path::new("/dev/urandom"), 1 << 2, URANDOM_DEVICE),
        ] {
            let root = computation_device(path, device, meter)?;
            prepared.path(&root, access, false, meter)?;
        }
        if let ExecutionNetwork::TcpPorts(ports) = &grant.network {
            for port in ports {
                let rule = PortRule {
                    access: CONNECT_TCP,
                    port: u64::from(*port),
                };
                native_control_attempt(meter, Write, || {
                    // SAFETY: live owned ruleset and initialized native port rule.
                    syscall(|| unsafe {
                        libc::syscall(
                            libc::SYS_landlock_add_rule,
                            prepared.ruleset.as_raw_fd(),
                            2,
                            &raw const rule,
                            0,
                        )
                    })
                })?;
            }
        }
        // std and the static runner install stdio before guest entry. Keep
        // both retained setup objects outside those three target slots even
        // when the owner originally had closed standard streams.
        Ok(Self {
            ruleset: crate::file::outside_standard_streams(prepared.ruleset, meter)?,
            cwd: crate::file::outside_standard_streams(prepared.cwd.into(), meter)?.into(),
        })
    }

    fn path(
        &self,
        file: &File,
        access: u64,
        is_dir: bool,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<()> {
        let rule = PathRule {
            // A pinned inode cannot change its file type. Binding already
            // verified its identity; another metadata query buys no guarantee.
            access: if is_dir { access } else { access & FILE },
            parent: file.as_raw_fd(),
        };
        native_control_attempt(meter, Write, || {
            // SAFETY: the live root fd and packed path rule remain borrowed until
            // the kernel copies this rule into the exclusively owned ruleset.
            syscall(|| unsafe {
                libc::syscall(
                    libc::SYS_landlock_add_rule,
                    self.ruleset.as_raw_fd(),
                    1,
                    &raw const rule,
                    0,
                )
            })
        })?;
        Ok(())
    }

    pub(super) fn install(self, command: &mut Command, grant: ExecutionGrant) {
        // SAFETY: only syscalls and owned preallocated storage in the child.
        // This hook is appended after the original custody/warden hook.
        unsafe {
            command.pre_exec(move || self.enter(&grant));
        }
    }

    fn enter(&self, grant: &ExecutionGrant) -> io::Result<()> {
        sys::working_directory(self.cwd.as_raw_fd()).map_err(io::Error::from_raw_os_error)?;
        entry::enter(self.ruleset.as_raw_fd(), &grant.0.filter)
            .map_err(io::Error::from_raw_os_error)
    }
}

// Preparation failures may allocate a descriptive refusal. Post-fork failures
// use errno only; the launch core formats them after returning to the host.
fn syscall(call: impl FnOnce() -> libc::c_long) -> io::Result<libc::c_long> {
    let result = call();
    if result < 0 {
        Err(failure(io::Error::last_os_error()))
    } else {
        Ok(result)
    }
}

const LOAD: u16 = 0x20;
const EQUAL: u16 = 0x15;
const AND: u16 = 0x54;
const RETURN: u16 = 0x06;
const DENY: u32 = 0x0005_0000 | libc::EPERM as u32;
const ALLOW: u32 = 0x7fff_0000;

const fn instruction(code: u16, jt: u8, jf: u8, k: u32) -> Instruction {
    Instruction { code, jt, jf, k }
}

pub(super) fn filter(network: &ExecutionNetwork) -> io::Result<Vec<Instruction>> {
    #[cfg(target_arch = "x86_64")]
    let arch = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    let arch = 0xc000_00b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err(unavailable(
        "seccomp execution boundary requires x86_64 or aarch64",
    ));
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let mut code = vec![
            instruction(LOAD, 0, 0, 4),
            instruction(EQUAL, 1, 0, arch),
            instruction(RETURN, 0, 0, 0x8000_0000), // kill on another syscall ABI
            instruction(LOAD, 0, 0, 0),
        ];
        // Reject the x32 ABI before comparing native syscall numbers.
        code.extend([
            instruction(0x35, 0, 1, 0x4000_0000),
            instruction(RETURN, 0, 0, DENY),
        ]);
        // The common data path returns before the less frequent control
        // checks. Every other admitted operation is classified below; a new
        // kernel syscall never acquires authority by being absent from a list.
        for call in [libc::SYS_read, libc::SYS_write] {
            verdict(&mut code, call, ALLOW)?;
        }
        // clone3 has an indirect flag pointer, so force libc's ordinary clone
        // fallback. It is never continued after a racy userspace inspection.
        verdict(
            &mut code,
            libc::SYS_clone3,
            0x0005_0000 | libc::ENOSYS as u32,
        )?;
        code.extend([
            instruction(EQUAL, 0, 4, number(libc::SYS_clone)?),
            instruction(LOAD, 0, 0, 16),
            instruction(AND, 0, 0, 0x7e02_8080), // namespace + CLONE_PARENT
            instruction(EQUAL, 1, 0, 0),
            instruction(RETURN, 0, 0, DENY),
            instruction(LOAD, 0, 0, 0),
        ]);
        control_filter(&mut code)?;
        socket_filter(&mut code, network)?;
        for call in allowed_syscalls() {
            verdict(&mut code, *call, ALLOW)?;
        }
        code.push(instruction(RETURN, 0, 0, DENY));
        Ok(code)
    }
}

fn control_filter(code: &mut Vec<Instruction>) -> io::Result<()> {
    // No file-owner signal routing, leases or new fcntl commands. Only
    // descriptor-local state and locks on already granted files are admitted.
    const COMMANDS: &[u32] = &[
        0, 1, 2, 3, 4, 5, 6, 7, // DUPFD, descriptor/status flags, POSIX locks
        36, 37, 38, // open-file-description locks
        1030, 1031, 1032, 1033, 1034, // DUPFD_CLOEXEC, pipe size, file seals
    ];
    code.push(instruction(
        EQUAL,
        0,
        u8::try_from(2 * COMMANDS.len() + 2)
            .map_err(|_| refused("invalid descriptor-control filter"))?,
        number(libc::SYS_fcntl)?,
    ));
    code.push(instruction(LOAD, 0, 0, 24));
    for command in COMMANDS {
        code.extend([
            instruction(EQUAL, 0, 1, *command),
            instruction(RETURN, 0, 0, ALLOW),
        ]);
    }
    code.push(instruction(RETURN, 0, 0, DENY));
    // prlimit64 can otherwise change another host process's resource limits.
    code.extend([
        instruction(EQUAL, 0, 4, number(libc::SYS_prlimit64)?),
        instruction(LOAD, 0, 0, 16),
        instruction(EQUAL, 0, 1, 0),
        instruction(RETURN, 0, 0, ALLOW),
        instruction(RETURN, 0, 0, DENY),
    ]);
    Ok(())
}

fn socket_filter(code: &mut Vec<Instruction>, network: &ExecutionNetwork) -> io::Result<()> {
    if matches!(network, ExecutionNetwork::Off)
        || matches!(network, ExecutionNetwork::TcpPorts(ports) if ports.is_empty())
    {
        for call in [libc::SYS_socket, libc::SYS_socketpair, libc::SYS_connect] {
            verdict(code, call, DENY)?;
        }
        return Ok(());
    }
    // Local sockets could delegate forbidden filesystem/network work to an
    // unconstrained host service. Only Internet address families are admitted.
    let mut socket = vec![
        instruction(LOAD, 0, 0, 16),
        instruction(EQUAL, 2, 0, libc::AF_INET as u32),
        instruction(EQUAL, 1, 0, libc::AF_INET6 as u32),
        instruction(RETURN, 0, 0, DENY),
        instruction(LOAD, 0, 0, 24),
        instruction(AND, 0, 0, 0xf),
    ];
    match network {
        ExecutionNetwork::TcpPorts(_) => {
            // Deferred Fast Open reaches that same implicit-connect path on
            // ordinary write, without MSG_FASTOPEN. No socket crosses exec,
            // and guests cannot create this deferred state through setsockopt.
            code.extend([
                instruction(EQUAL, 0, 6, number(libc::SYS_setsockopt)?),
                instruction(LOAD, 0, 0, 24),
                instruction(EQUAL, 0, 3, libc::IPPROTO_TCP as u32),
                instruction(LOAD, 0, 0, 32),
                instruction(EQUAL, 0, 1, libc::TCP_FASTOPEN_CONNECT as u32),
                instruction(RETURN, 0, 0, DENY),
                instruction(LOAD, 0, 0, 0),
            ]);
            // Older Landlock implementations do not mediate Fast Open's
            // implicit connect. Read only by-value flags, never a guest pointer.
            for (call, flags) in [
                (libc::SYS_sendto, 40),
                (libc::SYS_sendmsg, 32),
                (libc::SYS_sendmmsg, 40),
            ] {
                code.extend([
                    instruction(EQUAL, 0, 5, number(call)?),
                    instruction(LOAD, 0, 0, flags),
                    instruction(AND, 0, 0, libc::MSG_FASTOPEN as u32),
                    instruction(EQUAL, 1, 0, 0),
                    instruction(RETURN, 0, 0, DENY),
                    instruction(LOAD, 0, 0, 0),
                ]);
            }
            socket.extend([
                instruction(EQUAL, 1, 0, libc::SOCK_STREAM as u32),
                instruction(RETURN, 0, 0, DENY),
                instruction(LOAD, 0, 0, 32),
                instruction(EQUAL, 2, 0, 0),
                instruction(EQUAL, 1, 0, libc::IPPROTO_TCP as u32),
                instruction(RETURN, 0, 0, DENY),
            ]);
            for call in [
                libc::SYS_bind,
                libc::SYS_listen,
                libc::SYS_accept,
                libc::SYS_accept4,
            ] {
                verdict(code, call, DENY)?;
            }
        }
        ExecutionNetwork::On => socket.extend([
            instruction(EQUAL, 2, 0, libc::SOCK_STREAM as u32),
            instruction(EQUAL, 1, 0, libc::SOCK_DGRAM as u32),
            instruction(RETURN, 0, 0, DENY),
        ]),
        ExecutionNetwork::Off => return Err(refused("inconsistent network filter")),
    }
    code.push(instruction(
        EQUAL,
        0,
        u8::try_from(socket.len()).map_err(|_| refused("invalid socket filter"))?,
        number(libc::SYS_socket)?,
    ));
    code.extend(socket);
    code.push(instruction(LOAD, 0, 0, 0));
    verdict(code, libc::SYS_socketpair, DENY)?;
    Ok(())
}

fn number(value: libc::c_long) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| refused("invalid syscall number"))
}

fn verdict(code: &mut Vec<Instruction>, call: libc::c_long, result: u32) -> io::Result<()> {
    code.extend([
        instruction(EQUAL, 0, 1, number(call)?),
        instruction(RETURN, 0, 0, result),
    ]);
    Ok(())
}

fn allowed_syscalls() -> &'static [libc::c_long] {
    &[
        // Data and memory operate only on this address space or descriptors
        // acquired under Landlock. No external IPC object identifiers enter.
        libc::SYS_close,
        libc::SYS_mmap,
        libc::SYS_mprotect,
        libc::SYS_munmap,
        libc::SYS_brk,
        libc::SYS_mremap,
        libc::SYS_madvise,
        libc::SYS_msync,
        libc::SYS_mincore,
        libc::SYS_mlock,
        libc::SYS_munlock,
        libc::SYS_mlockall,
        libc::SYS_munlockall,
        libc::SYS_pread64,
        libc::SYS_pwrite64,
        libc::SYS_readv,
        libc::SYS_writev,
        libc::SYS_preadv,
        libc::SYS_pwritev,
        libc::SYS_preadv2,
        libc::SYS_pwritev2,
        libc::SYS_lseek,
        libc::SYS_fadvise64,
        libc::SYS_sendfile,
        libc::SYS_copy_file_range,
        libc::SYS_splice,
        libc::SYS_tee,
        libc::SYS_vmsplice,
        libc::SYS_dup,
        libc::SYS_dup3,
        libc::SYS_pipe2,
        libc::SYS_close_range,
        libc::SYS_memfd_create,
        // Paths and writable file handles are checked by Landlock. Metadata
        // mutation has no admitted syscall, even inside the workspace.
        libc::SYS_openat,
        libc::SYS_openat2,
        libc::SYS_fstat,
        libc::SYS_newfstatat,
        libc::SYS_statx,
        libc::SYS_faccessat,
        libc::SYS_faccessat2,
        libc::SYS_getdents64,
        libc::SYS_readlinkat,
        libc::SYS_getcwd,
        libc::SYS_chdir,
        libc::SYS_fchdir,
        libc::SYS_mkdirat,
        libc::SYS_mknodat,
        libc::SYS_unlinkat,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_renameat,
        libc::SYS_renameat2,
        libc::SYS_linkat,
        libc::SYS_symlinkat,
        libc::SYS_truncate,
        libc::SYS_ftruncate,
        libc::SYS_fallocate,
        libc::SYS_flock,
        libc::SYS_fsync,
        libc::SYS_fdatasync,
        libc::SYS_statfs,
        libc::SYS_fstatfs,
        libc::SYS_umask,
        // Threads, children and signals remain in the inherited domain.
        // clone flags and cross-domain signals have separate enforcement.
        libc::SYS_clone,
        libc::SYS_execve,
        libc::SYS_execveat,
        libc::SYS_exit,
        libc::SYS_exit_group,
        libc::SYS_wait4,
        libc::SYS_waitid,
        libc::SYS_kill,
        libc::SYS_tkill,
        libc::SYS_tgkill,
        libc::SYS_pidfd_open,
        libc::SYS_pidfd_send_signal,
        libc::SYS_rt_sigaction,
        libc::SYS_rt_sigprocmask,
        libc::SYS_rt_sigreturn,
        libc::SYS_rt_sigpending,
        libc::SYS_rt_sigtimedwait,
        libc::SYS_rt_sigqueueinfo,
        libc::SYS_rt_tgsigqueueinfo,
        libc::SYS_rt_sigsuspend,
        libc::SYS_sigaltstack,
        libc::SYS_set_tid_address,
        libc::SYS_set_robust_list,
        libc::SYS_futex,
        libc::SYS_rseq,
        libc::SYS_membarrier,
        libc::SYS_sched_yield,
        libc::SYS_sched_getaffinity,
        libc::SYS_restart_syscall,
        // Descriptor events and this process's clocks/identity expose no
        // independently named host mutation or external message channel.
        libc::SYS_eventfd2,
        libc::SYS_epoll_create1,
        libc::SYS_epoll_ctl,
        libc::SYS_epoll_pwait,
        libc::SYS_epoll_pwait2,
        libc::SYS_ppoll,
        libc::SYS_pselect6,
        libc::SYS_timerfd_create,
        libc::SYS_timerfd_settime,
        libc::SYS_timerfd_gettime,
        libc::SYS_nanosleep,
        libc::SYS_clock_nanosleep,
        libc::SYS_clock_gettime,
        libc::SYS_clock_getres,
        libc::SYS_gettimeofday,
        libc::SYS_getitimer,
        libc::SYS_setitimer,
        libc::SYS_getpid,
        libc::SYS_getppid,
        libc::SYS_gettid,
        libc::SYS_getuid,
        libc::SYS_geteuid,
        libc::SYS_getgid,
        libc::SYS_getegid,
        libc::SYS_getgroups,
        libc::SYS_getrusage,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_getrlimit,
        libc::SYS_getrandom,
        libc::SYS_getcpu,
        libc::SYS_uname,
        libc::SYS_prctl,
        // Socket creation is separately restricted to Internet families;
        // Landlock checks TCP connections and the port-only mode's listeners.
        libc::SYS_socket,
        libc::SYS_connect,
        libc::SYS_bind,
        libc::SYS_listen,
        libc::SYS_accept,
        libc::SYS_accept4,
        libc::SYS_shutdown,
        libc::SYS_sendto,
        libc::SYS_recvfrom,
        libc::SYS_sendmsg,
        libc::SYS_recvmsg,
        libc::SYS_sendmmsg,
        libc::SYS_recvmmsg,
        libc::SYS_getsockname,
        libc::SYS_getpeername,
        libc::SYS_setsockopt,
        libc::SYS_getsockopt,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_open,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_creat,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_stat,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_lstat,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_access,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_readlink,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_getdents,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_mkdir,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_mknod,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_rmdir,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_rename,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_link,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_unlink,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_symlink,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_fork,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_vfork,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_dup2,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_pipe,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_poll,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_select,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_epoll_create,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_epoll_wait,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_eventfd,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_alarm,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_time,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_arch_prctl,
    ]
}

#[cfg(all(test, any(target_arch = "x86_64", target_arch = "aarch64")))]
mod checks;
