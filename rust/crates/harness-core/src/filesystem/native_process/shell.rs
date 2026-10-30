use super::Invocation;
#[cfg(test)]
#[cfg(unix)]
use super::{NativeProcessObserver, NativeProcessResult, capture};
use crate::{Error, Result, tools::ShellInput, workspace::MountTable};
use acyclic_fs::access::Action;
use acyclic_native_runtime::{NativeCommand, NativeHost};
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
    host: NativeHost,
    now: u64,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
) -> Result<Option<NativeProcessResult>> {
    let invocation = prepare(input, mounts, host, Path::new("/bin/bash"), 32_768, now)?;
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
    )
    .await
    .map(|(output, ())| output)
}

#[doc(hidden)]
pub fn prepare<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    mut host: NativeHost,
    shell: &Path,
    maximum_output_bytes: usize,
    now: u64,
) -> Result<Invocation> {
    input.validate()?;
    let relative = Path::new(input.cwd.as_deref().unwrap_or(""));
    let (mount, path) = mounts.resolve(relative)?;
    mount.check(Action::List, &path, now)?;
    let io = |cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause));
    // A volume's root is its binding, which may link to the view projecting
    // it, so the canonical cwd is checked against the canonical root.
    let native_root = mount
        .native_root
        .as_deref()
        .map(std::fs::canonicalize)
        .transpose()
        .map_err(io)?;
    let root = native_root.as_deref().unwrap_or(&mounts.root);
    let requested = if native_root.is_some() {
        root.join(&path)
    } else {
        mounts.root.join(relative)
    };
    let cwd = std::fs::canonicalize(requested).map_err(io)?;
    let selected = cwd
        .strip_prefix(root)
        .map_err(|_| Error::Invalid("shell cwd escapes the task view".into()))?;
    if mount.native_root.is_some() {
        let selected = selected
            .to_str()
            .ok_or_else(|| Error::Invalid("shell cwd is not UTF-8".into()))?;
        mount.check(Action::List, &selected.replace('\\', "/"), now)?;
    } else if selected != relative {
        let (mount, path) = mounts.resolve(selected)?;
        mount.check(Action::List, &path, now)?;
    }
    host.cwd = cwd;
    for (name, value) in mounts.environment()? {
        if host.environment.insert(name, value).is_some() {
            return Err(Error::Invalid(
                "volume discovery hint collides with the explicit process environment".into(),
            ));
        }
    }
    // Shell commands receive only the host's explicit ordinary variables and
    // discovery hints. Ambient credentials and startup hooks stay host-owned,
    // even when another native process caller requested inheritance.
    host.inherit_environment = false;
    let deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(u64::from(
            input.timeout_ms()?,
        )))
        .ok_or_else(|| Error::Invalid("shell timeout is outside the native clock range".into()))?;
    let mut argv = vec![shell.as_os_str().to_owned()];
    let stdin = crate::tools::shell::PLATFORM.compose(&mut argv, input.command);
    let invocation = Invocation {
        host,
        command: NativeCommand {
            argv,
            cwd: None,
            stdin,
            deadline,
            maximum_output_bytes,
        },
    };
    invocation.validate(root)?;
    Ok(invocation)
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
