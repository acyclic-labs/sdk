//! Seatbelt translates the shared grant; the process-tree owner still owns
//! custody, streams and retirement. Apple's launcher applies the profile before
//! exec, avoiding sandbox compiler allocation in a multithreaded fork child.
#![allow(
    unsafe_code,
    reason = "rooted openat and fchdir in the native Seatbelt adapter"
)]
use super::{ExecutionAccess, ExecutionNetwork, Grant, refused};
use std::{fs::File, io, os::fd::AsRawFd, path::Path};

// Apple's installed spawn.h declares this extension; libc exposes the flags
// but not the file action. Keep this one native declaration in the adapter.
unsafe extern "C" {
    fn posix_spawn_file_actions_addinherit_np(
        actions: *mut libc::posix_spawn_file_actions_t,
        descriptor: libc::c_int,
    ) -> libc::c_int;
}

pub(super) fn validate_stdio(descriptors: [Option<std::os::fd::RawFd>; 3]) -> io::Result<()> {
    let null = File::open("/dev/null")?.metadata()?;
    use std::os::unix::fs::MetadataExt;
    for descriptor in descriptors.into_iter().flatten() {
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fstat initializes the complete live output on success.
        if unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstat succeeded.
        let metadata = unsafe { metadata.assume_init() };
        let kind = metadata.st_mode & libc::S_IFMT;
        if kind != libc::S_IFIFO
            && !(kind == libc::S_IFCHR && u64::try_from(metadata.st_rdev).ok() == Some(null.rdev()))
        {
            return Err(refused(
                "Seatbelt standard streams must be pipes or the null device",
            ));
        }
    }
    Ok(())
}

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
            for root in grant
                .workspace_write
                .then_some(&grant.workspace)
                .into_iter()
                .chain(grant.write.entries())
            {
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
            open_beneath(root, relative, true, meter)?
        };
        Ok(Self {
            profile,
            cwd: crate::file::outside_standard_streams(cwd.into(), meter)?.into(),
        })
    }

    pub(super) fn install(self, command: &mut std::process::Command) -> io::Result<()> {
        use std::os::unix::process::CommandExt;
        let exec = SealedExec::new(command)?;
        // SAFETY: the child only calls fchdir on a retained descriptor. The
        // profile is applied by sandbox-exec after its own exec, before guest
        // code. No allocator or sandbox compiler runs after our fork.
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(self.cwd.as_raw_fd()) == -1 {
                    return Err(io::Error::last_os_error());
                }
                exec.enter()
            });
        }
        Ok(())
    }
}

// SETEXEC replaces the already-custodied child, rather than starting another
// process owner. CLOEXEC_DEFAULT seals *all* ambient descriptors in the kernel
// without scanning the host's descriptor table. std has already installed the
// three admitted standard streams before calling this final exec hook.
struct SealedExec {
    attributes: libc::posix_spawnattr_t,
    actions: Option<libc::posix_spawn_file_actions_t>,
    program: std::ffi::CString,
    _arguments: Vec<std::ffi::CString>,
    _environment: Vec<std::ffi::CString>,
    argv: Vec<*mut libc::c_char>,
    envp: Vec<*mut libc::c_char>,
}

// SAFETY: all native objects and pointer targets are exclusively owned,
// immutable after preparation, and borrowed only by the fork child's exec.
unsafe impl Send for SealedExec {}
unsafe impl Sync for SealedExec {}

impl SealedExec {
    fn new(command: &std::process::Command) -> io::Result<Self> {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};
        let cstring = |text: &std::ffi::OsStr| CString::new(text.as_bytes()).map_err(refused);
        let program = cstring(command.get_program())?;
        let arguments = std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(cstring)
            .collect::<io::Result<Vec<_>>>()?;
        let environment = command
            .get_envs()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .map(|(key, value)| {
                let mut bytes = key.as_bytes().to_vec();
                bytes.push(b'=');
                bytes.extend_from_slice(value.as_bytes());
                CString::new(bytes).map_err(refused)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let pointers = |values: &[CString]| {
            values
                .iter()
                .map(|value| value.as_ptr().cast_mut())
                .chain(std::iter::once(std::ptr::null_mut()))
                .collect()
        };
        let mut attributes = std::mem::MaybeUninit::uninit();
        // SAFETY: initialization receives writable storage of the native type.
        status(unsafe { libc::posix_spawnattr_init(attributes.as_mut_ptr()) })?;
        let mut result = Self {
            // SAFETY: initialization succeeded.
            attributes: unsafe { attributes.assume_init() },
            actions: None,
            program,
            argv: pointers(&arguments),
            envp: pointers(&environment),
            _arguments: arguments,
            _environment: environment,
        };
        let mut actions = std::mem::MaybeUninit::uninit();
        // SAFETY: writable native action storage.
        status(unsafe { libc::posix_spawn_file_actions_init(actions.as_mut_ptr()) })?;
        // SAFETY: initialization succeeded; RAII now owns the actions.
        result.actions = Some(unsafe { actions.assume_init() });
        let flags = libc::POSIX_SPAWN_SETEXEC | libc::POSIX_SPAWN_CLOEXEC_DEFAULT;
        // SAFETY: initialized, exclusively held attribute object.
        status(unsafe {
            libc::posix_spawnattr_setflags(&raw mut result.attributes, flags as libc::c_short)
        })?;
        for descriptor in [0, 1, 2] {
            // SAFETY: live action object and standard descriptors, which std
            // establishes before the exec hook; no ambient FD is inherited.
            status(unsafe {
                posix_spawn_file_actions_addinherit_np(result.actions.as_mut().unwrap(), descriptor)
            })?;
        }
        Ok(result)
    }

    fn enter(&self) -> io::Result<()> {
        // SAFETY: immutable prebuilt spawn objects, strings and null-terminated
        // pointer vectors remain live. SETEXEC never returns on success.
        let code = unsafe {
            libc::posix_spawn(
                std::ptr::null_mut(),
                self.program.as_ptr(),
                self.actions.as_ref().unwrap(),
                &raw const self.attributes,
                self.argv.as_ptr(),
                self.envp.as_ptr(),
            )
        };
        Err(io::Error::from_raw_os_error(code))
    }
}

impl Drop for SealedExec {
    fn drop(&mut self) {
        // SAFETY: each initialized native object is destroyed exactly once.
        unsafe {
            if let Some(actions) = &mut self.actions {
                libc::posix_spawn_file_actions_destroy(actions);
            }
            libc::posix_spawnattr_destroy(&raw mut self.attributes);
        }
    }
}

fn status(code: libc::c_int) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code))
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
    directory: bool,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<File> {
    use std::{ffi::CString, os::fd::FromRawFd, os::unix::ffi::OsStrExt};
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(refused("execution cwd is not a rooted descendant"));
        };
        let name = CString::new(name.as_bytes()).map_err(refused)?;
        let directory = if directory || components.peek().is_some() {
            libc::O_DIRECTORY
        } else {
            0
        };
        root =
            crate::meter::native_control_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: the parent descriptor and terminated relative component
                // are live. NOFOLLOW rejects every symlink component, not just the
                // final path, and DIRECTORY prevents traversal through files.
                let fd = unsafe {
                    libc::openat(
                        root.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY
                            | libc::O_CLOEXEC
                            | libc::O_NOFOLLOW
                            | libc::O_NONBLOCK
                            | directory,
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
