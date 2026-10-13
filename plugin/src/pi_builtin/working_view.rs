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
