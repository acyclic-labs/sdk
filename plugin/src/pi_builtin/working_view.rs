use super::*;

pub(super) fn validate_declared_working_roles(
    binding: &crate::PiTaskBinding,
    manifest: &acyclic_harness::filesystem::NativeViewManifest,
) -> Result<()> {
    binding.validate(
        None,
        binding.project_volumes.keys().map(String::as_str),
        false,
    )?;
    if manifest.volumes.len() != binding.project_volumes.len() + 1
        || !manifest
            .volumes
            .iter()
            .any(|mapping| mapping.volume == binding.scratch.volume)
        || binding.project_volumes.values().any(|volume| {
            !manifest
                .volumes
                .iter()
                .any(|mapping| &mapping.volume == volume)
        })
    {
        return Err(denied(
            "prepared working view differs from its original declared PROJECT/SCRATCH roles",
        ));
    }
    Ok(())
}

pub(crate) fn mounted_relative_path(
    manifest: &acyclic_harness::filesystem::NativeViewManifest,
    mapping: &acyclic_harness::filesystem::NativeVolumeMapping,
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<(std::path::PathBuf, NativeAllocation)> {
    let prefix = mapping
        .path
        .strip_prefix(&manifest.options.root)
        .map_err(|_| denied("mounted volume is outside its original namespace root"))?;
    if prefix.as_os_str().is_empty()
        || prefix
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        || leaf.as_os_str().is_empty()
        || prefix
            .components()
            .chain(leaf.components())
            .any(|part| match part {
                Component::Normal(value) => value
                    .to_str()
                    .is_none_or(|value| value.contains(['\\', '\0'])),
                _ => true,
            })
    {
        return Err(denied(
            "mounted path is not a regular original namespace leaf",
        ));
    }
    let capacity = prefix
        .as_os_str()
        .len()
        .checked_add(leaf.as_os_str().len())
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| invalid("mounted path size overflow"))?;
    // Covers either native byte or UTF-16 backing before PathBuf growth.
    let bytes = (capacity as u64)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::path::PathBuf>() as u64))
        .ok_or_else(|| invalid("mounted path ownership overflow"))?;
    let custody = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut path = std::path::PathBuf::with_capacity(capacity);
    path.push(prefix);
    path.push(leaf);
    Ok((path, custody))
}

pub(super) fn select_native_path<'a, 'p>(
    path: &'p str,
    binding: &crate::PiTaskBinding,
    manifest: &'a acyclic_harness::filesystem::NativeViewManifest,
) -> Result<(
    &'a acyclic_harness::filesystem::NativeVolumeMapping,
    &'p Path,
)> {
    validate_path(path)?;
    manifest.validate(None)?;
    let allowed = |mapping: &&acyclic_harness::filesystem::NativeVolumeMapping| {
        mapping.volume == binding.scratch.volume
            || binding
                .project_volumes
                .values()
                .any(|volume| volume == &mapping.volume)
    };
    let mut selected = None;
    for mapping in manifest.volumes.iter().filter(allowed) {
        if !Path::new(path).is_absolute()
            && mapping.volume == binding.scratch.volume
            && !binding.project_volumes.is_empty()
        {
            continue;
        }
        if let Ok(relative) = relative_path(path, &mapping.path) {
            if selected.is_some() {
                return Err(denied("builtin native path selection is ambiguous"));
            }
            selected = Some((mapping, relative));
        }
    }
    selected.ok_or_else(|| {
        denied("builtin path is outside its actual PROJECT/SCRATCH view; RESULT is unmounted")
    })
}
