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
    let invocation = prepare(input, mounts, invocation, now)?;
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
    mount.check_relative(Action::List, &path, now)?;
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
        mount.check_relative(Action::List, &selected.replace('\\', "/"), now)?;
    } else if selected != Path::new(&path) {
        let (mount, path) = mounts.resolve(selected)?;
        mount.check_relative(Action::List, &path, now)?;
    }
    invocation.cwd = cwd;
    for (name, value) in mounts.environment()? {
        if invocation
            .environment
            .keys()
            .any(|key| super::invocation::environment_key_matches(key, &name))
        {
            return Err(Error::Invalid(
                "workspace discovery collides with the explicit process environment".into(),
            ));
        }
        invocation.environment.insert(name, value);
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
        let search = acyclic_executable_search::ExecutableSearch::from_environment(
            &invocation.cwd,
            |name| {
                invocation
                    .environment
                    .iter()
                    .find(|(key, _)| super::invocation::environment_key_matches(key, name))
                    .map(|(_, value)| value.into())
            },
        );
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
///
/// `HOME` and `TMPDIR` are the agent's private writable scratch, never the
/// project: the projected private home when one is mounted writable, and
/// otherwise a per-agent directory in host-owned scratch, which physical
/// views use because they project no auxiliary volume. Scratch work therefore
/// cannot edit, pollute or publish into authored project files.
#[doc(hidden)]
pub fn workspace_environment<T>(
    mounts: &MountTable<T>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let projected = mounts
        .native_mounts()
        .find(|(relative, _, mount)| *relative == "volume:///home" && !mount.mode.read_only())
        .map(|(_, root, _)| root);
    // `held` is the root the no-follow walk creates `tmp` under; `home` is
    // the value the process sees.
    let (held, relative, home) = if let Some(home) = projected {
        // The admitted binding may be an owner-maintained alias. Resolve
        // that root before the no-follow walk of authored descendants.
        (
            std::fs::canonicalize(home).map_err(|error| {
                Error::process_io(std::io::Error::new(
                    error.kind(),
                    format!("cannot resolve the admitted home directory: {error}"),
                ))
            })?,
            std::path::PathBuf::new(),
            home.to_path_buf(),
        )
    } else {
        let scratch = mounts.scratch_root.as_deref().ok_or_else(|| {
            Error::Invalid(
                "shell requires a private home or host scratch outside the project".into(),
            )
        })?;
        // Only the owning agent may write an agent-owned volume, so the
        // owner of any writable one is this table's agent. Principal
        // labels are not identities: one host may label every agent alike.
        let agent = mounts
            .mounts()
            .filter(|(_, mount)| !mount.mode.read_only())
            .find_map(|(_, mount)| match mount.view.volume.owner() {
                crate::conversation::VolumeOwner::Agent(agent) => Some(agent.to_string()),
                _ => None,
            })
            .ok_or_else(|| {
                Error::Invalid("shell requires the agent's own writable volume".into())
            })?;
        let relative = Path::new("homes").join(blake3::hash(agent.as_bytes()).to_hex().as_str());
        (
            scratch.to_path_buf(),
            relative.clone(),
            scratch.join(relative),
        )
    };
    acyclic_fs::hold_root(&held)
        .and_then(|root| {
            // The ordinary volume publication owns persistence after the
            // process completes, not this disposable projection.
            root.create_directories(
                &relative.join("tmp"),
                acyclic_fs::NativeDurability::Visibility,
            )
        })
        .map_err(|error| Error::Storage(error.to_string()))?;
    // The process sees its temporary directory under the home it is given,
    // alias included, as `$HOME/tmp`.
    let temporary = home.join("tmp");
    let utf8 = |path: &Path, what: &str| {
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Invalid(format!("sandbox {what} directory is not UTF-8")))
    };
    let (home, temporary) = (utf8(&home, "home")?, utf8(&temporary, "temporary")?);
    for name in [
        "HOME",
        #[cfg(windows)]
        "USERPROFILE",
    ] {
        if !environment
            .keys()
            .any(|key| super::invocation::environment_key_matches(key, name))
        {
            environment.insert(name.into(), home.clone());
        }
    }
    for name in [
        "TMPDIR",
        #[cfg(windows)]
        "TEMP",
        #[cfg(windows)]
        "TMP",
    ] {
        environment.retain(|key, _| !super::invocation::environment_key_matches(key, name));
        environment.insert(name.into(), temporary.clone());
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
#[cfg(any(unix, windows))]
mod tests;
