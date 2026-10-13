use super::*;

pub(in crate::native_mount::materialize) fn rename_entry(
    from: &HostRoot,
    name: &Path,
    to: &HostRoot,
    target: &Path,
    work: &mut impl SourceDemandReadWork,
) -> Result<(), DemandError> {
    #[cfg(unix)]
    {
        crate::native_host::rename_held(
            &from.directory,
            name,
            &to.directory,
            target,
            acyclic_native_runtime::RenameMode::Replace,
            None,
            work,
        )
    }
    #[cfg(windows)]
    {
        crate::native_host::with_wide_path(name, work, |name, work| {
            crate::native_host::with_wide_path(target, work, |target, work| {
                crate::native_host::rename_held(
                    &from.directory,
                    name,
                    &to.directory,
                    target,
                    acyclic_native_runtime::RenameMode::Replace,
                    None,
                    work,
                )
            })
        })
    }
}

pub(in crate::native_mount::materialize) fn remove_entry(
    parent: &HostRoot,
    name: &Path,
    work: &mut impl SourceDemandReadWork,
) -> Result<(), DemandError> {
    #[cfg(unix)]
    {
        parent.remove(name, None, work)
    }
    #[cfg(windows)]
    {
        crate::native_host::with_wide_path(name, work, |name, work| parent.remove(name, None, work))
    }
}
