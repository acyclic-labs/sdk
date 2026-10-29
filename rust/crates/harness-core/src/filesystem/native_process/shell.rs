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
    )
    .await
    .map(|(output, ())| output)
}

#[doc(hidden)]
pub fn prepare<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    mut invocation: Invocation,
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
    invocation.timeout_ms = input.timeout_ms()?;
    let (arguments, standard_input) = crate::tools::shell::PLATFORM.prepare(input.command);
    invocation.argv = arguments.collect();
    invocation.standard_input = standard_input;
    super::invocation::validate(
        &invocation.executable,
        &invocation.cwd,
        root,
        &invocation.environment,
        &invocation.argv,
    )?;
    Ok(invocation)
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
