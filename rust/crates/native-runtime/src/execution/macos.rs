//! Compile Seatbelt policy in the host; apply the compiled policy in the
//! existing launch core's final child, after its custody hook.

#![allow(
    unsafe_code,
    reason = "Seatbelt FFI owns and applies a compiled native sandbox profile"
)]

use super::{ExecutionAccess, ExecutionNetwork, Grant, refused};
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    io,
    os::unix::process::CommandExt,
    path::Path,
    process::Command,
    ptr::NonNull,
};

#[repr(C)]
struct Profile {
    builtin: *mut c_char,
    data: *const u8,
    size: usize,
}

#[link(name = "sandbox")]
unsafe extern "C" {
    fn sandbox_compile_string(
        source: *const c_char,
        parameters: *mut c_void,
        error: *mut *mut c_char,
    ) -> *mut Profile;
    fn sandbox_apply(profile: *mut Profile) -> c_int;
    fn sandbox_free_profile(profile: *mut Profile);
    fn sandbox_free_error(error: *mut c_char);
}

pub(super) struct Prepared(NonNull<Profile>);

// SAFETY: the compiled profile has no thread affinity. Exclusive ownership
// moves it to the spawn thread; application only reads its immutable bytes.
unsafe impl Send for Prepared {}
// SAFETY: a shared borrow can only apply the same immutable compiled profile
// in an isolated post-fork child; destruction requires exclusive ownership.
unsafe impl Sync for Prepared {}

impl Prepared {
    pub(super) fn new(grant: &Grant) -> io::Result<Self> {
        let source = profile(grant)?;
        let source = CString::new(source).map_err(|_| refused("invalid Seatbelt profile"))?;
        let mut error = std::ptr::null_mut();
        // SAFETY: owned terminated profile and writable native error output.
        let compiled = unsafe {
            sandbox_compile_string(source.as_ptr(), std::ptr::null_mut(), &raw mut error)
        };
        let reason = if error.is_null() {
            None
        } else {
            // SAFETY: Seatbelt returns a terminated, library-owned error.
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: this error is released exactly once by its own library.
            unsafe { sandbox_free_error(error) };
            Some(message)
        };
        NonNull::new(compiled).map(Self).ok_or_else(|| {
            refused(
                reason
                    .as_deref()
                    .unwrap_or("Seatbelt policy compilation failed"),
            )
        })
    }

    pub(super) fn install(self, command: &mut Command) {
        // SAFETY: the child applies already-compiled native data; policy
        // parsing, allocation and error formatting stay in the host.
        unsafe {
            command.pre_exec(move || {
                if sandbox_apply(self.0.as_ptr()) == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        // SAFETY: unique library-owned profile; the child's copy exits at exec.
        unsafe { sandbox_free_profile(self.0.as_ptr()) };
    }
}

fn profile(grant: &Grant) -> io::Result<String> {
    let mut policy = String::from(
        "(version 1)\n(deny default)\n\
         (allow process-exec process-fork)\n\
         (allow sysctl-read)\n\
         (allow file-read-metadata)\n\
         (allow file-read-data file-write-data (literal \"/dev/null\"))\n\
         (allow file-read-data (literal \"/dev/urandom\"))\n",
    );
    let root = quote(&grant.workspace)?;
    policy.push_str(&format!("(allow file-read* (subpath {root}))\n"));
    if grant.access == ExecutionAccess::WorkspaceWrite {
        policy.push_str(&format!("(allow file-write* (subpath {root}))\n"));
    }
    for path in &grant.read {
        let selector = if path.is_dir() { "subpath" } else { "literal" };
        policy.push_str(&format!(
            "(allow file-read* ({selector} {}))\n",
            quote(path)?
        ));
    }
    match &grant.network {
        ExecutionNetwork::Off => {}
        ExecutionNetwork::On => {
            policy.push_str("(allow network-outbound network-inbound (remote tcp) (remote udp))\n")
        }
        ExecutionNetwork::TcpPorts(ports) => {
            for port in ports {
                policy.push_str(&format!(
                    "(allow network-outbound (remote tcp \"*:{port}\"))\n"
                ));
            }
        }
    }
    Ok(policy)
}

fn quote(path: &Path) -> io::Result<String> {
    let path = path
        .to_str()
        .ok_or_else(|| refused("Seatbelt grant path must be UTF-8"))?;
    let mut quoted = String::from("\"");
    for character in path.chars() {
        match character {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(character);
            }
            character if character.is_control() => {
                return Err(refused("invalid Seatbelt grant path"));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    Ok(quoted)
}
