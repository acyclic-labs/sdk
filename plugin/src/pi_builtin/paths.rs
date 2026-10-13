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

#[cfg(not(windows))]
pub(super) fn windows_local_verbatim(_: &str) -> bool {
    false
}

#[cfg(windows)]
pub(super) fn windows_local_verbatim(value: &str) -> bool {
    matches!(Path::new(value).components().next(),
        Some(Component::Prefix(prefix)) if matches!(prefix.kind(), std::path::Prefix::VerbatimDisk(_)))
}

#[cfg(not(windows))]
pub(super) fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    path.strip_prefix(root).ok()
}

#[cfg(windows)]
pub(super) fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    use std::path::Prefix;
    let mut selected = path.components();
    for expected in root.components() {
        let actual = selected.next()?;
        let equivalent = match (actual, expected) {
            (Component::Prefix(actual), Component::Prefix(expected)) => {
                let disk = |prefix| match prefix {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        Some(drive.to_ascii_uppercase())
                    }
                    _ => None,
                };
                disk(actual.kind()).is_some() && disk(actual.kind()) == disk(expected.kind())
            }
            _ => actual == expected,
        };
        if !equivalent {
            return None;
        }
    }
    Some(selected.as_path())
}

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
    acyclic_harness::conversation::validate_content_path(&value)?;
    Ok((value, custody))
}
