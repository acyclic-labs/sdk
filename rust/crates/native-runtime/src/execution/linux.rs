//! Landlock binds filesystem objects; seccomp seals the escape mechanisms.
//! All allocation and ruleset construction happen before fork. The child hook
//! uses only native system calls and already-owned buffers.

#![allow(
    unsafe_code,
    reason = "checked Landlock and seccomp FFI in the native execution adapter"
)]

use super::{ExecutionAccess, ExecutionNetwork, Grant, failure, refused};
use std::{
    ffi::CString,
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{ffi::OsStrExt, process::CommandExt},
    },
    path::Path,
    process::Command,
};

const READ: u64 = (1 << 0) | (1 << 2) | (1 << 3);
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

pub(super) struct Prepared {
    ruleset: OwnedFd,
    // Keeping these objects open through spawn prevents inode reuse between
    // rule construction and enforcement. Nothing is retained for idle agents.
    _roots: Vec<File>,
    filter: Vec<libc::sock_filter>,
}

impl Prepared {
    pub(super) fn new(grant: &Grant) -> io::Result<Self> {
        // ABI 6 supplies signal and abstract-UNIX scoping; earlier ABIs cannot
        // prove this contract. Never silently discard an unsupported right.
        let abi = syscall(|| unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<()>(),
                0,
                1,
            )
        })?;
        if abi < 6 {
            return Err(refused("Landlock ABI 6 or later is required"));
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
        let ruleset = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let mut prepared = Self {
            ruleset,
            _roots: Vec::new(),
            filter: filter(&grant.network)?,
        };
        let workspace = READ
            | if grant.access == ExecutionAccess::WorkspaceWrite {
                WRITE
            } else {
                0
            };
        prepared.path(&grant.workspace, workspace)?;
        for path in &grant.read {
            prepared.path(path, READ)?;
        }
        // Null output and entropy are capabilities of computation. No arbitrary
        // device, host home, procfs or temporary tree is exposed.
        prepared.path(Path::new("/dev/null"), (1 << 1) | (1 << 2))?;
        prepared.path(Path::new("/dev/urandom"), 1 << 2)?;
        if let ExecutionNetwork::TcpPorts(ports) = &grant.network {
            for port in ports {
                let rule = PortRule {
                    access: CONNECT_TCP,
                    port: u64::from(*port),
                };
                syscall(|| unsafe {
                    libc::syscall(
                        libc::SYS_landlock_add_rule,
                        prepared.ruleset.as_raw_fd(),
                        2,
                        &raw const rule,
                        0,
                    )
                })?;
            }
        }
        Ok(prepared)
    }

    fn path(&mut self, path: &Path, access: u64) -> io::Result<()> {
        let name =
            CString::new(path.as_os_str().as_bytes()).map_err(|_| refused("invalid grant path"))?;
        // SAFETY: terminated path; O_PATH holds the object without reading it.
        let fd = unsafe { libc::open(name.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(failure(io::Error::last_os_error()));
        }
        // SAFETY: successful open transferred ownership of fd.
        let file = unsafe { File::from_raw_fd(fd) };
        let rule = PathRule {
            access: if file.metadata()?.is_dir() {
                access
            } else {
                access & FILE
            },
            parent: file.as_raw_fd(),
        };
        syscall(|| unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                self.ruleset.as_raw_fd(),
                1,
                &raw const rule,
                0,
            )
        })?;
        self._roots.push(file);
        Ok(())
    }

    pub(super) fn install(self, command: &mut Command) {
        // SAFETY: only syscalls and owned preallocated storage in the child.
        // This hook is appended after the original custody/warden hook.
        unsafe {
            command.pre_exec(move || self.enter());
        }
    }

    fn enter(&self) -> io::Result<()> {
        // Preserve Rust's exec-error pipe until exec while closing every
        // nonstdio host descriptor on exec, including inherited network fds.
        // SAFETY: close_range with CLOEXEC changes descriptor flags only.
        child_status(unsafe { libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, 4_u32) })?;
        for fd in 0..3 {
            // SAFETY: fstat initializes this plain ABI output on success.
            let mut status: libc::stat = unsafe { std::mem::zeroed() };
            if unsafe { libc::fstat(fd, &raw mut status) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let kind = status.st_mode & libc::S_IFMT;
            // Only pipes and /dev/null may cross the boundary as stdio.
            if kind != libc::S_IFIFO
                && !(kind == libc::S_IFCHR
                    && libc::major(status.st_rdev) == 1
                    && libc::minor(status.st_rdev) == 3)
            {
                return Err(io::Error::from_raw_os_error(libc::EPERM));
            }
        }
        // SAFETY: irreversible privilege and filesystem attenuation of this
        // child only. No other thread exists in the post-fork child.
        child_status(unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0).into() })?;
        child_status(unsafe {
            libc::syscall(
                libc::SYS_landlock_restrict_self,
                self.ruleset.as_raw_fd(),
                0,
            )
        })?;
        let program = libc::sock_fprog {
            len: u16::try_from(self.filter.len()).map_err(|_| io::ErrorKind::InvalidInput)?,
            filter: self.filter.as_ptr().cast_mut(),
        };
        // SAFETY: the kernel copies the complete immutable BPF program here.
        child_status(unsafe { libc::syscall(libc::SYS_seccomp, 1, 0, &raw const program) })?;
        Ok(())
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

fn child_status(result: libc::c_long) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

const LOAD: u16 = 0x20;
const EQUAL: u16 = 0x15;
const AND: u16 = 0x54;
const RETURN: u16 = 0x06;
const DENY: u32 = 0x0005_0000 | libc::EPERM as u32;
const ALLOW: u32 = 0x7fff_0000;

const fn instruction(code: u16, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

fn filter(network: &ExecutionNetwork) -> io::Result<Vec<libc::sock_filter>> {
    #[cfg(target_arch = "x86_64")]
    let arch = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    let arch = 0xc000_00b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err(refused(
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
        for number in denied_syscalls() {
            deny(&mut code, number, DENY)?;
        }
        // clone3 has an indirect flag pointer, so force libc's ordinary clone
        // fallback. It is never continued after a racy userspace inspection.
        deny(
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
        socket_filter(&mut code, network)?;
        code.push(instruction(RETURN, 0, 0, ALLOW));
        Ok(code)
    }
}

fn socket_filter(code: &mut Vec<libc::sock_filter>, network: &ExecutionNetwork) -> io::Result<()> {
    if matches!(network, ExecutionNetwork::Off)
        || matches!(network, ExecutionNetwork::TcpPorts(ports) if ports.is_empty())
    {
        for call in [libc::SYS_socket, libc::SYS_socketpair, libc::SYS_connect] {
            deny(code, call, DENY)?;
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
                deny(code, call, DENY)?;
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
    deny(code, libc::SYS_socketpair, DENY)?;
    Ok(())
}

fn number(value: libc::c_long) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| refused("invalid syscall number"))
}

fn deny(code: &mut Vec<libc::sock_filter>, call: libc::c_long, result: u32) -> io::Result<()> {
    code.extend([
        instruction(EQUAL, 0, 1, number(call)?),
        instruction(RETURN, 0, 0, result),
    ]);
    Ok(())
}

fn denied_syscalls() -> Vec<libc::c_long> {
    let mut calls = vec![
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_getfd,
        libc::SYS_kcmp,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_open_by_handle_at,
        libc::SYS_name_to_handle_at,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_io_uring_setup,
        libc::SYS_setsid,
        libc::SYS_setpgid,
        libc::SYS_fchmod,
        libc::SYS_fchmodat,
        libc::SYS_fchown,
        libc::SYS_fchownat,
        libc::SYS_utimensat,
        libc::SYS_setxattr,
        libc::SYS_lsetxattr,
        libc::SYS_fsetxattr,
        libc::SYS_removexattr,
        libc::SYS_lremovexattr,
        libc::SYS_fremovexattr,
        libc::SYS_reboot,
        libc::SYS_kexec_load,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_acct,
        libc::SYS_quotactl,
        // fchmodat2 is not exposed by every libc release; its number is shared
        // by both supported native ABIs. It must not bypass metadata denial.
        452,
    ];
    #[cfg(target_arch = "x86_64")]
    calls.extend([
        libc::SYS_chmod,
        libc::SYS_chown,
        libc::SYS_lchown,
        libc::SYS_utime,
        libc::SYS_utimes,
        libc::SYS_futimesat,
    ]);
    calls
}
