use super::Invocation;
#[cfg(test)]
#[cfg(unix)]
use super::{NativeProcessObserver, NativeProcessResult, capture};
use crate::{Error, Result, tools::ShellInput, workspace::MountTable};
use acyclic_fs::access::Action;
use std::path::Path;
#[cfg(test)]
#[cfg(unix)]
use std::sync::Arc;

/// Validates the already-composed task namespace and uses the native provider's
/// capture path. Its caller retains the view and publishes each writable mount.
#[cfg(test)]
#[cfg(unix)]
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
        if invocation.environment.insert(name, value).is_some() {
            return Err(Error::Invalid(
                "volume discovery hint collides with the explicit process environment".into(),
            ));
        }
    }
    // Shell commands receive only the host's explicit ordinary variables and
    // discovery hints. Ambient credentials and startup hooks stay host-owned,
    // even when another native process caller requested inheritance.
    invocation.inherit_environment = false;
    // timeout_ms bounds the process's own run: its clock starts at launch, so
    // admission, volume forks and every other pre-spawn wait never count.
    invocation.schedule = input.timeout_ms()?.map_or(
        acyclic_native_runtime::CaptureSchedule::Unbounded,
        |timeout| {
            acyclic_native_runtime::CaptureSchedule::FromLaunch(std::time::Duration::from_millis(
                u64::from(timeout),
            ))
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
                    .find(|(key, _)| {
                        if cfg!(windows) {
                            key.eq_ignore_ascii_case(name)
                        } else {
                            key.as_str() == name
                        }
                    })
                    .map(|(_, value)| value.into())
            });
        invocation.executable = search.resolve(program).map_err(Error::process_io)?;
    }
    invocation.argv = arguments;
    invocation.standard_input =
        standard_input.map(acyclic_native_runtime::OwnedCaptureInput::Literal);
    if invocation.execution_grant.is_none() {
        invocation.execution_grant = workspace_grant(mounts, &mut invocation.environment)?;
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

/// Host-owned workspace boundary shared by shell and lifecycle hooks. Explicit
/// execution grants retain their existing, separately authenticated authority.
/// Native enforcement failures refuse launch, never retry unconfined. Linux
/// macOS and Windows consume the same default policy.
#[doc(hidden)]
pub fn workspace_grant<T>(
    mounts: &MountTable<T>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<Option<acyclic_native_runtime::execution::ExecutionGrant>> {
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
        // A closed environment must not rediscover the owner's home through
        // host NSS IPC. Ordinary tools use the same workspace as their home.
        #[cfg(unix)]
        if let std::collections::btree_map::Entry::Vacant(home) = environment.entry("HOME".into()) {
            home.insert(
                primary
                    .to_str()
                    .ok_or_else(|| Error::Invalid("sandbox home directory is not UTF-8".into()))?
                    .into(),
            );
        }
        // Harness-bound processes put temporary files in host scratch.
        let temporary = if let Some(scratch) = &mounts.scratch_root {
            scratch
        } else if access == ExecutionAccess::ReadOnly {
            write.first().unwrap_or(&primary)
        } else {
            &primary
        };
        environment.insert(
            "TMPDIR".into(),
            temporary
                .to_str()
                .ok_or_else(|| Error::Invalid("sandbox temporary directory is not UTF-8".into()))?
                .into(),
        );
        #[cfg(windows)]
        {
            let temporary = environment["TMPDIR"].clone();
            environment.insert("TEMP".into(), temporary.clone());
            environment.insert("TMP".into(), temporary);
        }
        ExecutionGrant::workspace_default(&primary, access, write, read)
            .map(Some)
            .map_err(io)
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    {
        let _ = (mounts, environment);
        Ok(None)
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
