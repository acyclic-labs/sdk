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
    sync::Arc,
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
        flags: u64::from((libc::O_PATH | libc::O_CLOEXEC | flags).cast_unsigned()),
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

#[derive(Debug)]
pub(super) struct Policy {
    ruleset: OwnedFd,
    access: ExecutionAccess,
}

impl Policy {
    pub(super) fn access(&self) -> ExecutionAccess {
        self.access
    }

    pub(super) fn new(
        grant: &Grant,
        access: ExecutionAccess,
        root: &File,
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
        let prepared = Self { ruleset, access };
        let workspace = READ
            | if access == ExecutionAccess::WorkspaceWrite {
                WRITE
            } else {
                0
            };
        // The caller has just bound and verified this original workspace.
        prepared.path(root, workspace, grant.workspace.is_dir, meter)?;
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
        // Keep this setup object outside the child stdio target slots.
        Ok(Self {
            ruleset: crate::file::outside_standard_streams(prepared.ruleset, meter)?,
            access,
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
}

// Cold preparation owns its policy directly without another allocation. A
// retained policy shares the exact immutable descriptor across active launches.
enum PolicyOwner {
    Owned(Policy),
    Shared(Arc<Policy>),
}

impl std::ops::Deref for PolicyOwner {
    type Target = Policy;

    fn deref(&self) -> &Policy {
        match self {
            Self::Owned(policy) => policy,
            Self::Shared(policy) => policy,
        }
    }
}

pub(super) struct Prepared {
    policy: PolicyOwner,
    cwd: File,
}

impl Prepared {
    pub(super) fn new(
        grant: &Grant,
        access: ExecutionAccess,
        compiled: Option<&Arc<Policy>>,
        root: File,
        cwd: Option<&Path>,
        meter: Option<&dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        let policy = match compiled.filter(|policy| policy.access == access) {
            Some(policy) => {
                // Retention saves compilation, never the original path fences.
                // A replaced read root refuses before any child can launch.
                for root in &grant.read {
                    drop(root.bind_controlled(meter)?);
                }
                PolicyOwner::Shared(Arc::clone(policy))
            }
            None => PolicyOwner::Owned(Policy::new(grant, access, &root, meter)?),
        };
        // Resolve once beneath the original verified workspace. Both cold and
        // retained policies use this same cwd owner and final-child handoff.
        let relative = cwd
            .unwrap_or(&grant.workspace)
            .strip_prefix(&grant.workspace.path)
            .map_err(|_| refused("execution working directory escapes the granted workspace"))?;
        let cwd = if relative.as_os_str().is_empty() {
            root
        } else {
            open_beneath_with_flags(&root, relative, libc::O_DIRECTORY, meter)?
        };
        Ok(Self {
            policy,
            cwd: crate::file::outside_standard_streams(cwd.into(), meter)?.into(),
        })
    }

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
        // SAFETY: Instruction is repr(C), and its eight-byte size assertion
        // equals the sum of its integer fields, so it has no padding. Every
        // byte is initialized; this immutable native-endian view borrows the
        // same live slice and cannot outlive it. The decoder uses that same
        // native layout, so one copy replaces field-by-field serialization.
        let bytes = unsafe {
            std::slice::from_raw_parts(filter.as_ptr().cast::<u8>(), std::mem::size_of_val(filter))
        };
        body.extend_from_slice(bytes);
        Ok(self.policy.ruleset.as_raw_fd())
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
        entry::enter(self.policy.ruleset.as_raw_fd(), grant.policy.filter)
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
const GREATER: u16 = 0x25;
const GREATER_EQUAL: u16 = 0x35;
const JUMP: u16 = 0x05;
const AND: u16 = 0x54;
const RETURN: u16 = 0x06;
const DENY: u32 = 0x0005_0000 | libc::EPERM as u32;
const ALLOW: u32 = 0x7fff_0000;

const fn instruction(code: u16, jt: u8, jf: u8, k: u32) -> Instruction {
    Instruction { code, jt, jf, k }
}

/// Counting and emission use the same generator during constant evaluation.
/// The zero-capacity pass computes storage; every emitted array is exact-sized.
struct Filter<const N: usize> {
    instructions: [Instruction; N],
    len: usize,
}

const fn element<T>(items: &[T], index: usize) -> &T {
    match items.split_at_checked(index) {
        Some((_, [item, ..])) => item,
        _ => unreachable!("execution filter index is within its counted constant capacity"),
    }
}

const fn element_mut<T>(items: &mut [T], index: usize) -> &mut T {
    match items.split_at_mut_checked(index) {
        Some((_, [item, ..])) => item,
        _ => unreachable!("execution filter index is within its counted constant capacity"),
    }
}

impl<const N: usize> Filter<N> {
    const fn new() -> Self {
        Self {
            instructions: [instruction(0, 0, 0, 0); N],
            len: 0,
        }
    }

    const fn push(&mut self, instruction: Instruction) {
        if N != 0 {
            *element_mut(&mut self.instructions, self.len) = instruction;
        }
        self.len += 1;
    }

    const fn extend(&mut self, instructions: &[Instruction]) {
        let mut remaining = instructions;
        while let Some((instruction, rest)) = remaining.split_first() {
            self.push(*instruction);
            remaining = rest;
        }
    }
}

#[derive(Clone, Copy)]
enum FilterNetwork {
    Off,
    On,
    Tcp,
}

pub(super) fn filter(network: &ExecutionNetwork) -> io::Result<&'static [Instruction]> {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        const OFF_LEN: usize = compile_filter::<0>(FilterNetwork::Off).len;
        const ON_LEN: usize = compile_filter::<0>(FilterNetwork::On).len;
        const TCP_LEN: usize = compile_filter::<0>(FilterNetwork::Tcp).len;
        static OFF: [Instruction; OFF_LEN] =
            compile_filter::<OFF_LEN>(FilterNetwork::Off).instructions;
        static ON: [Instruction; ON_LEN] = compile_filter::<ON_LEN>(FilterNetwork::On).instructions;
        static TCP: [Instruction; TCP_LEN] =
            compile_filter::<TCP_LEN>(FilterNetwork::Tcp).instructions;
        let code: &'static [Instruction] = match network {
            ExecutionNetwork::Off => &OFF,
            ExecutionNetwork::On => &ON,
            ExecutionNetwork::TcpPorts(ports) if ports.is_empty() => &OFF,
            ExecutionNetwork::TcpPorts(_) => &TCP,
        };
        Ok(code)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = network;
        Err(unavailable(
            "seccomp execution boundary requires x86_64 or aarch64",
        ))
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const fn compile_filter<const N: usize>(network: FilterNetwork) -> Filter<N> {
    #[cfg(target_arch = "x86_64")]
    let arch = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    let arch = 0xc000_00b7;
    let mut code = Filter::new();
    code.extend(&[
        instruction(LOAD, 0, 0, 4),
        instruction(EQUAL, 1, 0, arch),
        instruction(RETURN, 0, 0, 0x8000_0000), // kill on another syscall ABI
        instruction(LOAD, 0, 0, 0),
    ]);
    // Reject the x32 ABI before comparing native syscall numbers.
    code.extend(&[
        instruction(GREATER_EQUAL, 0, 1, 0x4000_0000),
        instruction(RETURN, 0, 0, DENY),
    ]);
    // The common data path returns before the less frequent control
    // checks. Every other admitted operation is classified below; a new
    // kernel syscall never acquires authority by being absent from a list.
    verdict(&mut code, libc::SYS_read, ALLOW);
    verdict(&mut code, libc::SYS_write, ALLOW);
    // clone3 has an indirect flag pointer, so force libc's ordinary clone
    // fallback. It is never continued after a racy userspace inspection.
    verdict(
        &mut code,
        libc::SYS_clone3,
        0x0005_0000 | libc::ENOSYS as u32,
    );
    code.extend(&[
        instruction(EQUAL, 0, 4, number(libc::SYS_clone)),
        instruction(LOAD, 0, 0, 16),
        instruction(AND, 0, 0, 0x7e02_8080), // namespace + CLONE_PARENT
        instruction(EQUAL, 1, 0, 0),
        instruction(RETURN, 0, 0, DENY),
        instruction(LOAD, 0, 0, 0),
    ]);
    control_filter(&mut code);
    socket_filter(&mut code, network);
    const RANGES: SyscallRanges = syscall_ranges();
    syscall_dispatch(&mut code, RANGES.ranges.split_at(RANGES.len).0);
    assert!(N == 0 || code.len == N, "invalid execution filter length");
    assert!(
        code.len <= u16::MAX as usize,
        "execution filter exceeds native length"
    );
    code
}

const fn control_filter<const N: usize>(code: &mut Filter<N>) {
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
        jump(2 * COMMANDS.len() + 2),
        number(libc::SYS_fcntl),
    ));
    code.push(instruction(LOAD, 0, 0, 24));
    let mut remaining = COMMANDS;
    while let Some((command, rest)) = remaining.split_first() {
        code.extend(&[
            instruction(EQUAL, 0, 1, *command),
            instruction(RETURN, 0, 0, ALLOW),
        ]);
        remaining = rest;
    }
    code.push(instruction(RETURN, 0, 0, DENY));
    // Process-selected operations stay local to the caller. Group/session
    // identity reads are needed by ordinary shells; neither those queries
    // nor resource limits acquire authority over another named process.
    const SELF_PROCESS: &[libc::c_long] =
        &[libc::SYS_prlimit64, libc::SYS_getpgid, libc::SYS_getsid];
    let mut remaining = SELF_PROCESS;
    while let Some((call, rest)) = remaining.split_first() {
        code.extend(&[
            instruction(EQUAL, 0, 4, number(*call)),
            instruction(LOAD, 0, 0, 16),
            instruction(EQUAL, 0, 1, 0),
            instruction(RETURN, 0, 0, ALLOW),
            instruction(RETURN, 0, 0, DENY),
        ]);
        remaining = rest;
    }
}

const fn socket_filter<const N: usize>(code: &mut Filter<N>, network: FilterNetwork) {
    if matches!(network, FilterNetwork::Off) {
        verdict(code, libc::SYS_socket, DENY);
        verdict(code, libc::SYS_socketpair, DENY);
        verdict(code, libc::SYS_connect, DENY);
        return;
    }
    // Local sockets could delegate forbidden filesystem/network work to an
    // unconstrained host service. Only Internet address families are admitted.
    let socket = [
        instruction(LOAD, 0, 0, 16),
        instruction(EQUAL, 2, 0, libc::AF_INET as u32),
        instruction(EQUAL, 1, 0, libc::AF_INET6 as u32),
        instruction(RETURN, 0, 0, DENY),
        instruction(LOAD, 0, 0, 24),
        instruction(AND, 0, 0, 0xf),
    ];
    let protocol: &[Instruction] = if matches!(network, FilterNetwork::Tcp) {
        // Deferred Fast Open reaches that same implicit-connect path on
        // ordinary write, without MSG_FASTOPEN. No socket crosses exec,
        // and guests cannot create this deferred state through setsockopt.
        code.extend(&[
            instruction(EQUAL, 0, 6, number(libc::SYS_setsockopt)),
            instruction(LOAD, 0, 0, 24),
            instruction(EQUAL, 0, 3, libc::IPPROTO_TCP as u32),
            instruction(LOAD, 0, 0, 32),
            instruction(EQUAL, 0, 1, libc::TCP_FASTOPEN_CONNECT as u32),
            instruction(RETURN, 0, 0, DENY),
            instruction(LOAD, 0, 0, 0),
        ]);
        // Older Landlock implementations do not mediate Fast Open's
        // implicit connect. Read only by-value flags, never a guest pointer.
        let sends = [
            (libc::SYS_sendto, 40),
            (libc::SYS_sendmsg, 32),
            (libc::SYS_sendmmsg, 40),
        ];
        let mut remaining = sends.as_slice();
        while let Some((&(call, flags), rest)) = remaining.split_first() {
            code.extend(&[
                instruction(EQUAL, 0, 5, number(call)),
                instruction(LOAD, 0, 0, flags),
                instruction(AND, 0, 0, libc::MSG_FASTOPEN as u32),
                instruction(EQUAL, 1, 0, 0),
                instruction(RETURN, 0, 0, DENY),
                instruction(LOAD, 0, 0, 0),
            ]);
            remaining = rest;
        }
        verdict(code, libc::SYS_bind, DENY);
        verdict(code, libc::SYS_listen, DENY);
        verdict(code, libc::SYS_accept, DENY);
        verdict(code, libc::SYS_accept4, DENY);
        &[
            instruction(EQUAL, 1, 0, libc::SOCK_STREAM as u32),
            instruction(RETURN, 0, 0, DENY),
            instruction(LOAD, 0, 0, 32),
            instruction(EQUAL, 2, 0, 0),
            instruction(EQUAL, 1, 0, libc::IPPROTO_TCP as u32),
            instruction(RETURN, 0, 0, DENY),
        ]
    } else {
        &[
            instruction(EQUAL, 2, 0, libc::SOCK_STREAM as u32),
            instruction(EQUAL, 1, 0, libc::SOCK_DGRAM as u32),
            instruction(RETURN, 0, 0, DENY),
        ]
    };
    code.push(instruction(
        EQUAL,
        0,
        jump(socket.len() + protocol.len()),
        number(libc::SYS_socket),
    ));
    code.extend(&socket);
    code.extend(protocol);
    code.push(instruction(LOAD, 0, 0, 0));
    verdict(code, libc::SYS_socketpair, DENY);
}

const fn number(value: libc::c_long) -> u32 {
    assert!(
        value >= 0 && value.cast_unsigned() <= u32::MAX as libc::c_ulong,
        "invalid syscall number"
    );
    let [a, b, c, d, ..] = value.to_le_bytes();
    u32::from_le_bytes([a, b, c, d])
}

const fn jump(value: usize) -> u8 {
    assert!(value <= u8::MAX as usize, "invalid execution filter jump");
    let [low, ..] = value.to_le_bytes();
    low
}

const fn verdict<const N: usize>(code: &mut Filter<N>, call: libc::c_long, result: u32) {
    code.extend(&[
        instruction(EQUAL, 0, 1, number(call)),
        instruction(RETURN, 0, 0, result),
    ]);
}

#[derive(Clone, Copy)]
struct SyscallRange {
    first: u32,
    last: u32,
}

struct SyscallRanges {
    ranges: [SyscallRange; ALLOWED_SYSCALLS.len()],
    len: usize,
}

/// Sort and coalesce only during constant evaluation. No runtime table,
/// sorting, allocation or additional policy representation is retained.
const fn syscall_ranges() -> SyscallRanges {
    let calls = ALLOWED_SYSCALLS;
    let mut sorted = [0; ALLOWED_SYSCALLS.len()];
    let mut at = 0;
    while at < calls.len() {
        let call = number(*element(calls, at));
        let mut insert = at;
        while insert != 0 && *element(&sorted, insert - 1) > call {
            *element_mut(&mut sorted, insert) = *element(&sorted, insert - 1);
            insert -= 1;
        }
        *element_mut(&mut sorted, insert) = call;
        at += 1;
    }
    let mut result = SyscallRanges {
        ranges: [SyscallRange { first: 0, last: 0 }; ALLOWED_SYSCALLS.len()],
        len: 0,
    };
    at = 0;
    while at < sorted.len() {
        let call = *element(&sorted, at);
        if result.len != 0 && call <= element(&result.ranges, result.len - 1).last + 1 {
            element_mut(&mut result.ranges, result.len - 1).last = call;
        } else {
            *element_mut(&mut result.ranges, result.len) = SyscallRange {
                first: call,
                last: call,
            };
            result.len += 1;
        }
        at += 1;
    }
    result
}

/// Every branch moves forward and halves the remaining ranges. Leaves check
/// both bounds, so holes and future syscall numbers retain the same denial.
const fn syscall_dispatch<const N: usize>(code: &mut Filter<N>, ranges: &[SyscallRange]) {
    assert!(!ranges.is_empty(), "empty execution syscall declaration");
    if ranges.len() == 1 {
        let range = *element(ranges, 0);
        if range.first == range.last {
            code.push(instruction(EQUAL, 0, 1, range.first));
        } else {
            code.push(instruction(GREATER_EQUAL, 0, 2, range.first));
            code.push(instruction(GREATER, 1, 0, range.last));
        }
        code.push(instruction(RETURN, 0, 0, ALLOW));
        code.push(instruction(RETURN, 0, 0, DENY));
        return;
    }
    let (left, right) = ranges.split_at(ranges.len() / 2);
    code.push(instruction(GREATER_EQUAL, 0, 1, element(right, 0).first));
    let branch = code.len;
    code.push(instruction(JUMP, 0, 0, 0));
    syscall_dispatch(code, left);
    if N != 0 {
        // Classic BPF's unconditional jump carries a full 32-bit offset;
        // it cannot truncate a large subtree to an eight-bit branch.
        let distance = code.len - branch - 1;
        assert!(
            distance <= u32::MAX as usize,
            "invalid execution filter branch"
        );
        let [a, b, c, d, ..] = distance.to_le_bytes();
        element_mut(&mut code.instructions, branch).k = u32::from_le_bytes([a, b, c, d]);
    }
    syscall_dispatch(code, right);
}

const ALLOWED_SYSCALLS: &[libc::c_long] = &[
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
    #[cfg(target_arch = "x86_64")]
    libc::SYS_getpgrp,
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
];

#[cfg(all(test, any(target_arch = "x86_64", target_arch = "aarch64")))]
mod checks;
