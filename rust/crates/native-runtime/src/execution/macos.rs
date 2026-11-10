//! Seatbelt translates the shared grant; the process-tree owner still owns
//! custody, streams and retirement. Apple's launcher applies the profile before
//! exec, avoiding sandbox compiler allocation in a multithreaded fork child.
#![allow(
    unsafe_code,
    reason = "rooted openat and fchdir in the native Seatbelt adapter"
)]
use super::{ExecutionAccess, ExecutionNetwork, Grant, refused};
use std::{fs::File, io, os::fd::AsRawFd, path::Path};

pub(super) struct Prepared {
    pub(super) profile: String,
    cwd: File,
}

impl Prepared {
    pub(super) fn new(
        grant: &Grant,
        access: ExecutionAccess,
        cwd: Option<&Path>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        let mut profile = String::from(
            "(version 1)(deny default)(allow process-exec process-fork sysctl-read file-read-metadata)(allow file-write-data (literal \"/dev/null\"))",
        );
        for root in std::iter::once(&grant.workspace)
            .chain(grant.write.entries())
            .chain(grant.read.entries())
        {
            drop(root.bind_controlled(meter)?);
            rule(&mut profile, "file-read* file-map-executable", root)?;
        }
        if access == ExecutionAccess::WorkspaceWrite {
            for root in std::iter::once(&grant.workspace).chain(grant.write.entries()) {
                rule(&mut profile, "file-write*", root)?;
            }
        }
        match &grant.network {
            ExecutionNetwork::On => profile.push_str("(allow network*)"),
            ExecutionNetwork::Off => {}
            ExecutionNetwork::TcpPorts(ports) => {
                use std::fmt::Write;
                for port in ports {
                    write!(
                        profile,
                        "(allow network-outbound (remote tcp \"*:{port}\"))"
                    )
                    .expect("writing to String");
                }
            }
        }
        let cwd = cwd.unwrap_or(&grant.workspace);
        let selected = grant.cwd_root(cwd)?;
        let root = selected.bind_controlled(meter)?;
        let relative = cwd.strip_prefix(&selected.path).map_err(refused)?;
        let cwd = if relative.as_os_str().is_empty() {
            root
        } else {
            open_beneath(root, relative, meter)?
        };
        Ok(Self { profile, cwd })
    }

    pub(super) fn install(self, command: &mut std::process::Command) {
        use std::os::unix::process::CommandExt;
        // SAFETY: the child only calls fchdir on a retained descriptor. The
        // profile is applied by sandbox-exec after its own exec, before guest
        // code. No allocator or sandbox compiler runs after our fork.
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(self.cwd.as_raw_fd()) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
}

fn rule(profile: &mut String, operations: &str, root: &super::BoundPath) -> io::Result<()> {
    let path = root
        .path
        .to_str()
        .ok_or_else(|| refused("Seatbelt path is not UTF-8"))?;
    // Escape every Scheme string delimiter; reject control bytes rather than
    // interpreting model-controlled filenames as profile source.
    if path.chars().any(char::is_control) {
        return Err(refused("Seatbelt path contains a control character"));
    }
    let path = path.replace('\\', "\\\\").replace('"', "\\\"");
    let filter = if root.is_dir { "subpath" } else { "literal" };
    use std::fmt::Write;
    write!(profile, "(allow {operations} ({filter} \"{path}\"))").expect("writing to String");
    Ok(())
}

pub(super) fn open_beneath(
    mut root: File,
    relative: &Path,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<File> {
    use std::{ffi::CString, os::fd::FromRawFd, os::unix::ffi::OsStrExt};
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(refused("execution cwd is not a rooted descendant"));
        };
        let name = CString::new(name.as_bytes()).map_err(refused)?;
        root =
            crate::meter::native_control_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: the parent descriptor and terminated relative component
                // are live. NOFOLLOW rejects every symlink component, not just the
                // final path, and DIRECTORY prevents traversal through files.
                let fd = unsafe {
                    libc::openat(
                        root.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
                    )
                };
                if fd == -1 {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: openat returned a fresh owned descriptor.
                Ok(unsafe { File::from_raw_fd(fd) })
            })?;
    }
    Ok(root)
}
