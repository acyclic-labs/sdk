//! The one description of a process a [`super::ProcessTree`] starts.
//!
//! It mirrors the part of `std::process::Command` that trees use, so every
//! platform starts the same description: Unix turns it into a standard
//! command at its spawn boundary, and Windows creates the process itself
//! (see `windows_create.rs`) with exactly the attributes a tree needs.
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// A program, its arguments, environment changes, working directory and
/// standard streams, as started by [`super::ProcessTree::spawn`].
///
/// Unset standard streams are inherited from this process, as with
/// `std::process::Command::spawn`.
#[derive(Debug)]
pub struct ProcessCommand {
    // Set only by the consuming, host-authorized launch entry. It is carried
    // through the original process owner, never exposed as a model argument.
    pub(super) boundary: Option<crate::execution::PreparedExecution>,
    pub(super) program: OsString,
    pub(super) args: Vec<Arg>,
    pub(super) env: Environment,
    pub(super) cwd: Option<PathBuf>,
    pub(super) stdin: Option<ProcessStdio>,
    pub(super) stdout: Option<ProcessStdio>,
    pub(super) stderr: Option<ProcessStdio>,
    #[cfg(unix)]
    pub(super) unix: UnixIdentity,
}

/// What a Unix child is started as, beyond its program and streams.
#[cfg(unix)]
#[derive(Debug, Default)]
pub(super) struct UnixIdentity {
    /// The process group to join; `Some(0)` starts a new one.
    pub(super) process_group: Option<i32>,
    pub(super) uid: Option<u32>,
    pub(super) gid: Option<u32>,
    /// `argv[0]`, when it differs from the program.
    pub(super) arg0: Option<OsString>,
}

/// One argument: quoted for the child's parser, or (on Windows) appended to
/// the command line as given.
#[derive(Debug)]
pub(super) enum Arg {
    Regular(OsString),
    #[cfg(windows)]
    Raw(OsString),
}

impl Arg {
    pub(super) fn as_os_str(&self) -> &OsStr {
        match self {
            Self::Regular(argument) => argument,
            #[cfg(windows)]
            Self::Raw(argument) => argument,
        }
    }
}

/// Changes to the inherited environment.
#[derive(Debug, Default)]
pub(super) struct Environment {
    pub(super) clear: bool,
    pub(super) saw_path: bool,
    pub(super) vars: BTreeMap<EnvKey, Option<OsString>>,
}

/// An environment variable name. Windows compares names as the OS does
/// (ordinal, ignoring case); Unix compares their bytes.
#[derive(Debug, Clone)]
pub(super) struct EnvKey {
    pub(super) name: OsString,
    #[cfg(windows)]
    pub(super) wide: Vec<u16>,
}

impl EnvKey {
    pub(super) fn new(name: &OsStr) -> Self {
        Self {
            boundary: None,
            #[cfg(windows)]
            wide: std::os::windows::ffi::OsStrExt::encode_wide(name).collect(),
            name: name.to_owned(),
        }
    }
}

#[cfg(unix)]
impl Ord for EnvKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

#[cfg(windows)]
impl Ord for EnvKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use windows_sys::Win32::Globalization::{
            CSTR_EQUAL, CSTR_GREATER_THAN, CSTR_LESS_THAN, CompareStringOrdinal,
        };
        let length = |wide: &[u16]| i32::try_from(wide.len()).unwrap_or(i32::MAX);
        // SAFETY: both slices are live for the call and their lengths are
        // passed with them (clamped, so a longer name compares its prefix).
        let result = unsafe {
            CompareStringOrdinal(
                self.wide.as_ptr(),
                length(&self.wide),
                other.wide.as_ptr(),
                length(&other.wide),
                1,
            )
        };
        match result {
            CSTR_LESS_THAN => std::cmp::Ordering::Less,
            CSTR_EQUAL => self.wide.len().cmp(&other.wide.len()),
            CSTR_GREATER_THAN => std::cmp::Ordering::Greater,
            // Valid arguments cannot fail; fall back to the code units.
            _ => self.wide.cmp(&other.wide),
        }
    }
}

impl PartialOrd for EnvKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for EnvKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for EnvKey {}

impl Environment {
    fn saw(&mut self, key: &EnvKey) {
        if !self.saw_path && *key == EnvKey::new(OsStr::new("PATH")) {
            self.saw_path = true;
        }
    }

    /// Whether the child's `PATH` may differ from this process's.
    #[cfg(windows)]
    pub(super) const fn changed_path(&self) -> bool {
        self.saw_path || self.clear
    }

    /// The child's whole environment, or `None` when it is inherited unchanged.
    #[cfg(windows)]
    pub(super) fn capture_if_changed(&self) -> Option<BTreeMap<EnvKey, OsString>> {
        if !self.clear && self.vars.is_empty() {
            return None;
        }
        let mut result = BTreeMap::new();
        if !self.clear {
            for (key, value) in std::env::vars_os() {
                result.insert(EnvKey::new(&key), value);
            }
        }
        for (key, value) in &self.vars {
            match value {
                Some(value) => {
                    result.insert(key.clone(), value.clone());
                }
                None => {
                    result.remove(key);
                }
            }
        }
        Some(result)
    }
}

impl ProcessCommand {
    /// Describes `program`, with no arguments, the inherited environment and
    /// working directory, and inherited standard streams.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            args: Vec::new(),
            env: Environment::default(),
            cwd: None,
            stdin: None,
            stdout: None,
            stderr: None,
            #[cfg(unix)]
            unix: UnixIdentity::default(),
        }
    }

    /// Appends one argument, quoted as the child's parser needs.
    pub fn arg(&mut self, argument: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(Arg::Regular(argument.as_ref().to_owned()));
        self
    }

    /// Appends arguments, each quoted as the child's parser needs.
    pub fn args<I, S>(&mut self, arguments: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for argument in arguments {
            self.arg(argument);
        }
        self
    }

    /// Appends text to the command line unquoted, as
    /// `std::os::windows::process::CommandExt::raw_arg` does.
    #[cfg(windows)]
    pub fn raw_arg(&mut self, text: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(Arg::Raw(text.as_ref().to_owned()));
        self
    }

    /// Sets one environment variable for the child.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        let key = EnvKey::new(key.as_ref());
        self.env.saw(&key);
        self.env.vars.insert(key, Some(value.as_ref().to_owned()));
        self
    }

    /// Sets several environment variables for the child.
    pub fn envs<I, K, V>(&mut self, variables: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in variables {
            self.env(key, value);
        }
        self
    }

    /// Removes one variable from the child's environment.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        let key = EnvKey::new(key.as_ref());
        self.env.saw(&key);
        if self.env.clear {
            self.env.vars.remove(&key);
        } else {
            self.env.vars.insert(key, None);
        }
        self
    }

    /// Starts the child with only the variables set after this call.
    pub fn env_clear(&mut self) -> &mut Self {
        self.env.clear = true;
        self.env.vars.clear();
        self
    }

    /// Sets the child's working directory.
    pub fn current_dir(&mut self, directory: impl AsRef<Path>) -> &mut Self {
        self.cwd = Some(directory.as_ref().to_owned());
        self
    }

    /// Sets the child's standard input.
    pub fn stdin(&mut self, stdio: impl Into<ProcessStdio>) -> &mut Self {
        self.stdin = Some(stdio.into());
        self
    }

    /// Sets the child's standard output.
    pub fn stdout(&mut self, stdio: impl Into<ProcessStdio>) -> &mut Self {
        self.stdout = Some(stdio.into());
        self
    }

    /// Sets the child's standard error.
    pub fn stderr(&mut self, stdio: impl Into<ProcessStdio>) -> &mut Self {
        self.stderr = Some(stdio.into());
        self
    }

    /// Sets the child's process group, as
    /// `std::os::unix::process::CommandExt::process_group` does.
    #[cfg(unix)]
    pub fn process_group(&mut self, group: i32) -> &mut Self {
        self.unix.process_group = Some(group);
        self
    }

    /// Sets the child's user ID, as `CommandExt::uid` does.
    #[cfg(unix)]
    pub fn uid(&mut self, id: u32) -> &mut Self {
        self.unix.uid = Some(id);
        self
    }

    /// Sets the child's group ID, as `CommandExt::gid` does.
    #[cfg(unix)]
    pub fn gid(&mut self, id: u32) -> &mut Self {
        self.unix.gid = Some(id);
        self
    }

    /// Sets the child's `argv[0]`, as `CommandExt::arg0` does.
    #[cfg(unix)]
    pub fn arg0(&mut self, arg0: impl AsRef<OsStr>) -> &mut Self {
        self.unix.arg0 = Some(arg0.as_ref().to_owned());
        self
    }

    /// The program as given.
    #[must_use]
    pub fn get_program(&self) -> &OsStr {
        &self.program
    }

    /// The arguments as given.
    pub fn get_args(&self) -> impl ExactSizeIterator<Item = &OsStr> {
        self.args.iter().map(Arg::as_os_str)
    }

    /// The environment changes: a value sets a variable, `None` removes it.
    pub fn get_envs(&self) -> impl ExactSizeIterator<Item = (&OsStr, Option<&OsStr>)> {
        self.env
            .vars
            .iter()
            .map(|(key, value)| (key.name.as_os_str(), value.as_deref()))
    }

    /// Whether the child starts from an empty environment.
    #[must_use]
    pub const fn get_env_clear(&self) -> bool {
        self.env.clear
    }

    /// The working directory, if set.
    #[must_use]
    pub fn get_current_dir(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// The same description as a standard command. Owned streams are
    /// duplicated, so this description stays reusable.
    #[cfg(any(unix, test))]
    pub(crate) fn to_std(&self) -> std::io::Result<std::process::Command> {
        if self.boundary.is_some() {
            return Err(crate::execution::failure(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "native command conversion cannot discard its execution boundary",
            )));
        }
        let mut command = std::process::Command::new(&self.program);
        for argument in &self.args {
            match argument {
                Arg::Regular(argument) => {
                    command.arg(argument);
                }
                #[cfg(windows)]
                Arg::Raw(text) => {
                    std::os::windows::process::CommandExt::raw_arg(&mut command, text);
                }
            }
        }
        if self.env.clear {
            command.env_clear();
        }
        for (key, value) in &self.env.vars {
            match value {
                Some(value) => command.env(&key.name, value),
                None => command.env_remove(&key.name),
            };
        }
        if let Some(directory) = &self.cwd {
            command.current_dir(directory);
        }
        if let Some(stdio) = &self.stdin {
            command.stdin(stdio.to_std()?);
        }
        if let Some(stdio) = &self.stdout {
            command.stdout(stdio.to_std()?);
        }
        if let Some(stdio) = &self.stderr {
            command.stderr(stdio.to_std()?);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            let identity = &self.unix;
            if let Some(group) = identity.process_group {
                command.process_group(group);
            }
            if let Some(id) = identity.uid {
                command.uid(id);
            }
            if let Some(id) = identity.gid {
                command.gid(id);
            }
            if let Some(arg0) = &identity.arg0 {
                command.arg0(arg0);
            }
        }
        Ok(command)
    }
}

/// Where one of a child's standard streams goes.
#[derive(Debug)]
pub struct ProcessStdio(pub(super) Stdio);

#[derive(Debug)]
pub(super) enum Stdio {
    Inherit,
    Null,
    Piped,
    #[cfg(unix)]
    Owned(std::os::fd::OwnedFd),
    #[cfg(windows)]
    Owned(std::os::windows::io::OwnedHandle),
}

impl ProcessStdio {
    /// The child uses this process's stream.
    #[must_use]
    pub const fn inherit() -> Self {
        Self(Stdio::Inherit)
    }

    /// The child's stream is the null device.
    #[must_use]
    pub const fn null() -> Self {
        Self(Stdio::Null)
    }

    /// A new pipe whose other end the tree keeps.
    #[must_use]
    pub const fn piped() -> Self {
        Self(Stdio::Piped)
    }

    #[cfg(any(unix, test))]
    fn to_std(&self) -> std::io::Result<std::process::Stdio> {
        Ok(match &self.0 {
            Stdio::Inherit => std::process::Stdio::inherit(),
            Stdio::Null => std::process::Stdio::null(),
            Stdio::Piped => std::process::Stdio::piped(),
            Stdio::Owned(owned) => std::process::Stdio::from(owned.try_clone()?),
        })
    }
}

#[cfg(unix)]
impl From<std::os::fd::OwnedFd> for ProcessStdio {
    fn from(owned: std::os::fd::OwnedFd) -> Self {
        Self(Stdio::Owned(owned))
    }
}

#[cfg(windows)]
impl From<std::os::windows::io::OwnedHandle> for ProcessStdio {
    fn from(owned: std::os::windows::io::OwnedHandle) -> Self {
        Self(Stdio::Owned(owned))
    }
}

macro_rules! owned_stdio {
    ($($source:ty),*) => {$(
        impl From<$source> for ProcessStdio {
            fn from(source: $source) -> Self {
                #[cfg(unix)]
                return Self(Stdio::Owned(std::os::fd::OwnedFd::from(source)));
                #[cfg(windows)]
                return Self(Stdio::Owned(std::os::windows::io::OwnedHandle::from(source)));
            }
        }
    )*};
}

owned_stdio!(
    std::fs::File,
    std::io::PipeReader,
    std::io::PipeWriter,
    std::process::ChildStdin,
    std::process::ChildStdout,
    std::process::ChildStderr
);
