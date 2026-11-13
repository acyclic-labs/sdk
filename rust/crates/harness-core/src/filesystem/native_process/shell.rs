use super::Invocation;
#[cfg(all(test, any(unix, windows)))]
use super::{NativeProcessObserver, NativeProcessResult, capture};
use crate::{Error, Result, tools::ShellInput, workspace::MountTable};
use acyclic_fs::access::Action;
use std::path::Path;
#[cfg(all(test, any(unix, windows)))]
use std::sync::Arc;

/// Validates the already-composed task namespace and uses the native provider's
/// capture path. Its caller retains the view and publishes each writable mount.
#[cfg(all(test, any(unix, windows)))]
pub(crate) async fn run<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    invocation: Invocation,
    now: u64,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
) -> Result<Option<NativeProcessResult>> {
    let invocation = prepare(input, mounts, invocation, now, std::time::Instant::now())?;
    capture::capture_with_custody(
        invocation,
        move |_: &mut (), chunk| {
            if let Some(observer) = &observer {
                observer(chunk)?;
            }
            Ok(())
        },
        check,
        (),
        None,
        capture::OutputRetention::Collect,
    )
    .await
    .and_then(|(output, ())| output)
}

#[doc(hidden)]
pub fn prepare<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    mut invocation: Invocation,
    now: u64,
    admitted: std::time::Instant,
) -> Result<Invocation> {
    let cwd_error = |cause: std::io::Error| {
        let path = input.cwd.as_deref().unwrap_or(".");
        let message = match cause.kind() {
            std::io::ErrorKind::NotFound => format!("cwd {path} does not exist"),
            std::io::ErrorKind::NotADirectory => format!("cwd {path} is not a directory"),
            _ => format!("cannot use cwd {path}: {cause}"),
        };
        Error::process_io(std::io::Error::new(cause.kind(), message))
    };
    input.validate()?;
    let relative = Path::new(input.cwd.as_deref().unwrap_or(""));
    let (mount, path) = mounts.resolve(relative)?;
    mount.check(Action::List, &path, now)?;
    if matches!(mount.native_root, crate::workspace::NativeRoot::Logical) {
        return Err(Error::Unsupported(
            "logical volume has no native shell directory".into(),
        ));
    }
    let io = |cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause));
    // A volume's root is its binding, which may link to the view projecting
    // it, so the canonical cwd is checked against the canonical root.
    let exposed = mounts
        .native_root_for(mount)
        .ok_or_else(|| Error::Unsupported("shell cwd has no native projection".into()))?;
    let native_root = mount
        .native_root
        .override_path()
        .map(|_| std::fs::canonicalize(exposed))
        .transpose()
        .map_err(io)?;
    let root = native_root.as_deref().unwrap_or(&mounts.root);
    let requested = root.join(&path);
    let cwd = std::fs::canonicalize(&requested).map_err(cwd_error)?;
    if !std::fs::metadata(&cwd).map_err(cwd_error)?.is_dir() {
        return Err(cwd_error(std::io::Error::from(
            std::io::ErrorKind::NotADirectory,
        )));
    }
    let selected = cwd
        .strip_prefix(root)
        .map_err(|_| Error::Invalid("shell cwd escapes the task view".into()))?;
    if mount.native_root.override_path().is_some() {
        let selected = selected
            .to_str()
            .ok_or_else(|| Error::Invalid("shell cwd is not UTF-8".into()))?;
        mount.check(Action::List, &selected.replace('\\', "/"), now)?;
    } else if selected != Path::new(&path) {
        let (mount, path) = mounts.resolve(selected)?;
        mount.check(Action::List, &path, now)?;
    }
    invocation.cwd = cwd;
    for (name, value) in mounts.environment()? {
        if invocation
            .environment
            .keys()
            .any(|key| super::invocation::environment_key_matches(key, &name))
        {
            return Err(Error::Invalid(
                "volume discovery hint collides with the explicit process environment".into(),
            ));
        }
        invocation.environment.insert(name, value);
    }
    // Shell commands receive only the host's explicit ordinary variables and
    // discovery hints. Ambient credentials and startup hooks stay host-owned,
    // even when another native process caller requested inheritance.
    invocation.inherit_environment = false;
    // The caller admits native execution after checkout preparation. Queueing
    // for the checkout consumes scope lifetime, not this process lifetime.
    invocation.schedule = input.timeout_ms()?.map_or(
        acyclic_native_runtime::CaptureSchedule::Unbounded,
        |timeout| {
            admitted
                .checked_add(std::time::Duration::from_millis(u64::from(timeout)))
                .map_or(
                    acyclic_native_runtime::CaptureSchedule::Overflow,
                    acyclic_native_runtime::CaptureSchedule::until,
                )
        },
    );
    let shell = &crate::tools::shell::PLATFORM;
    let (program, arguments, standard_input) = input.command.prepare(shell)?;
    if let Some(program) = program {
        let search =
            acyclic_native_runtime::ExecutableSearch::from_environment(&invocation.cwd, |name| {
                invocation
                    .environment
                    .iter()
                    .find(|(key, _)| super::invocation::environment_key_matches(key, name))
                    .map(|(_, value)| value.into())
            });
        invocation.executable = search.resolve(program).map_err(Error::process_io)?;
    }
    invocation.argv = arguments;
    invocation.standard_input =
        standard_input.map(acyclic_native_runtime::OwnedCaptureInput::Literal);
    if invocation.execution_grant.is_none() {
        invocation.execution_grant = workspace_grant(mounts, &mut invocation.environment)?;
    } else {
        workspace_environment(mounts, &mut invocation.environment)?;
    }
    super::invocation::validate(
        &invocation.executable,
        &invocation.cwd,
        root,
        &invocation.environment,
        &invocation.argv,
    )?;
    Ok(invocation)
}

/// Selects process-local directories independently of execution-grant selection.
/// The private home projection owns temporary files; host scratch is reserved
/// for process output custody when no private home is projected.
#[doc(hidden)]
pub fn workspace_environment<T>(
    mounts: &MountTable<T>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let home = mounts
        .native_mounts()
        .find(|(relative, _, mount)| *relative == "volume:///home" && !mount.mode.read_only());
    let temporary = home
        .map(|(_, root, _)| {
            // The admitted binding may be an owner-maintained alias. Resolve
            // that root before the no-follow walk of authored descendants.
            let canonical = std::fs::canonicalize(root).map_err(Error::process_io)?;
            acyclic_fs::hold_root(&canonical)
                .and_then(|root| {
                    // The ordinary volume publication owns persistence after
                    // the process completes, not this disposable projection.
                    root.create_directories(
                        Path::new("tmp"),
                        acyclic_fs::NativeDurability::Visibility,
                    )
                })
                .map_err(|error| Error::Storage(error.to_string()))?;
            Ok::<_, Error>(root.join("tmp"))
        })
        .transpose()?;
    let home_root = home.map_or(mounts.root.as_path(), |(_, root, _)| root);
    for name in [
        "HOME",
        #[cfg(windows)]
        "USERPROFILE",
    ] {
        if !environment
            .keys()
            .any(|key| super::invocation::environment_key_matches(key, name))
        {
            environment.insert(
                name.into(),
                home_root
                    .to_str()
                    .ok_or_else(|| Error::Invalid("sandbox home directory is not UTF-8".into()))?
                    .into(),
            );
        }
    }
    let temporary = temporary.as_ref().map_or_else(
        || {
            mounts.scratch_root.as_deref().unwrap_or_else(|| {
                mounts
                    .native_mounts()
                    .find(|(_, _, mount)| !mount.mode.read_only())
                    .map_or(home_root, |(_, root, _)| root)
            })
        },
        |temporary| temporary.as_path(),
    );
    let temporary = temporary
        .to_str()
        .ok_or_else(|| Error::Invalid("sandbox temporary directory is not UTF-8".into()))?;
    for name in [
        "TMPDIR",
        #[cfg(windows)]
        "TEMP",
        #[cfg(windows)]
        "TMP",
    ] {
        environment.retain(|key, _| !super::invocation::environment_key_matches(key, name));
        environment.insert(name.into(), temporary.into());
    }
    Ok(())
}

/// Host-owned workspace boundary shared by shell and lifecycle hooks. Explicit
/// execution grants retain their existing, separately authenticated authority.
/// Native enforcement failures refuse launch, never retry unconfined. Linux
/// macOS and Windows consume the same default policy.
#[doc(hidden)]
pub fn workspace_grant<T>(
    mounts: &MountTable<T>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<Option<acyclic_native_runtime::execution::ExecutionGrant>> {
    workspace_environment(mounts, environment)?;
    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    {
        use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant};
        let mut read = Vec::new();
        let mut write = Vec::new();
        let mut access = ExecutionAccess::ReadOnly;
        let io = Error::process_io;
        let mut primary = mounts.root.clone();
        for (relative, root, mount) in mounts.native_mounts() {
            let root = root.to_path_buf();
            if relative.is_empty() {
                primary = root;
                if !mount.mode.read_only() {
                    access = ExecutionAccess::WorkspaceWrite;
                }
            } else if mount.mode.read_only() {
                read.push(root);
            } else {
                write.push(root);
            }
        }
        if let Some(scratch) = &mounts.scratch_root {
            write.push(scratch.clone());
        }
        let mut scoped_read = Vec::new();
        for (root, ceiling) in &mounts.native_read_roots {
            if root.try_exists().map_err(io)? {
                scoped_read.push((root.clone(), ceiling.clone()));
            }
        }
        ExecutionGrant::workspace_default_with_optional_read_scopes(
            &primary,
            access,
            write,
            read,
            scoped_read,
        )
        .map(Some)
        .map_err(io)
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    {
        let _ = (mounts, environment);
        Ok(None)
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests;
