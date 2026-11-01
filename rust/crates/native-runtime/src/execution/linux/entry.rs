//! One allocation-free final-child boundary, also usable by a static image.
//! The parent prepares the policy; the launch owner calls this only after
//! custody, identity and standard-stream installation, immediately before exec.

use super::sys;

#[repr(C)]
pub(super) struct CapabilityHeader {
    pub(super) version: u32,
    pub(super) pid: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct Capabilities {
    pub(super) effective: u32,
    pub(super) permitted: u32,
    pub(super) inheritable: u32,
}

/// Native classic-BPF instruction. Preparation and application borrow the
/// same buffer; entering a child never translates or copies its policy.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Instruction {
    pub(super) code: u16,
    pub(super) jt: u8,
    pub(super) jf: u8,
    pub(super) k: u32,
}

const _: () = assert!(core::mem::size_of::<Instruction>() == 8);

#[repr(C)]
pub(super) struct Program {
    pub(super) len: u16,
    pub(super) filter: *const Instruction,
}

pub(super) const NULL_DEVICE: u64 = 0x103;

/// Linux device numbers and file types identify the held object, not its name.
pub(super) fn character_device(mode: u32, device: u64, expected: u64) -> bool {
    mode & 0o170_000 == 0o020_000 && device == expected
}

/// Irreversibly confines only the final guest child. Every failure stops the
/// launch before exec; there is no fallback, allocation, wait or host callback.
pub(super) fn enter(ruleset: i32, filter: &[Instruction]) -> Result<(), i32> {
    let program = Program {
        len: u16::try_from(filter.len()).map_err(|_| sys::EINVAL)?,
        filter: filter.as_ptr(),
    };
    // Keep the launch owner's error pipe until exec while preventing every
    // nonstdio host capability, including sockets, from crossing exec.
    sys::seal_descriptors()?;
    for fd in 0..3 {
        let (mode, device) = sys::stdio_metadata(fd)?;
        let kind = mode & 0o170_000;
        // Pipes or Linux's null character device (major 1, minor 3).
        if kind != 0o010_000 && !character_device(mode, device, NULL_DEVICE) {
            return Err(sys::EPERM);
        }
    }
    // Drop ambient, effective, permitted and inheritable capabilities before
    // making the no-new-privileges promise and admitting guest code.
    sys::clear_ambient_capabilities()?;
    sys::capset(
        &CapabilityHeader {
            version: 0x2008_0522,
            pid: 0,
        },
        &[Capabilities {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        }; 2],
    )?;
    sys::no_new_privileges()?;
    sys::restrict_filesystem(ruleset)?;
    sys::restrict_syscalls(&program)
}
