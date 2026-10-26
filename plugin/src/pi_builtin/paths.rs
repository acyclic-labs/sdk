use super::*;

pub(super) fn validate_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 4096
        || value.as_bytes().contains(&0)
        || (value.starts_with("\\\\") && !windows_local_verbatim(value))
        || value.starts_with("//")
        || value.split(['/', '\\']).any(|part| part == "..")
        || (value.as_bytes().get(1) == Some(&b':') && !Path::new(value).is_absolute())
        || Path::new(value)
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(denied("builtin path escapes its declared native volume"));
    }
    Ok(())
}

/// Lexical selection only.
pub(crate) fn relative_path<'a>(path: &'a str, root: &Path) -> Result<&'a Path> {
    validate_path(path)?;
    if !root.is_absolute() {
        return Err(denied(
            "builtin volume has no original absolute native mapping",
        ));
    }
    let selected = Path::new(path);
    let relative = if selected.is_absolute() {
        strip_native_prefix(selected, root)
            .ok_or_else(|| denied("builtin path is outside its original native view"))?
    } else {
        selected
    };
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(denied(
            "builtin path is not a regular leaf below its original native volume",
        ));
    }
    Ok(relative)
}

pub(super) fn windows_local_verbatim(value: &str) -> bool {
    acyclic_native_runtime::paths::is_local_verbatim(Path::new(value))
}

pub(super) use acyclic_native_runtime::paths::strip_native_prefix;

pub(super) fn scoped_variants<'a>(
    variants: &'a [String],
    root: &Path,
) -> Result<[Option<&'a Path>; 4]> {
    let mut selected = [None; 4];
    for (index, path) in variants.iter().enumerate() {
        let entry = selected
            .get_mut(index)
            .ok_or_else(|| invalid("builtin variant bound exceeded"))?;
        *entry = Some(relative_path(path, root)?);
    }
    Ok(selected)
}

pub(super) fn logical_path(
    path: &Path,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    let maximum = path.as_os_str().len();
    let custody = work.claim(maximum as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut value = String::with_capacity(maximum);
    for component in path.components() {
        let part = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| invalid("builtin leaf has no wire representation"))?;
        if !value.is_empty() {
            value.push('/');
        }
        value.push_str(part);
    }
    work.charge(WorkCounters {
        bytes_copied: value.len() as u64,
        ..WorkCounters::default()
    })?;
    acyclic_harness::conversation::validate_content_path(
        &value,
        &mut Some(&mut acyclic_harness::store::HistoryWork::new(
            acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
        )),
    )?;
    Ok((value, custody))
}
