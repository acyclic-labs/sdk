use crate::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(crate) fn separated_directory(path: &Path, checkout: Option<&Path>) -> Result<PathBuf> {
    let Some(checkout) = checkout else {
        return directory(path);
    };
    let checkout = fs::canonicalize(checkout).map_err(|error| Error::Storage(error.to_string()))?;
    let separate = |directory: &Path| {
        if checkout.starts_with(directory) || directory.starts_with(&checkout) {
            return Err(Error::Invalid(
                "local state directory must be separate from the checkout".into(),
            ));
        }
        Ok(())
    };
    // Reject before creating anything: a refused root must leave the checkout untouched.
    let planned = planned_directory(path).ok_or_else(|| {
        Error::Invalid("local state directory has an unresolved path suffix".into())
    })?;
    separate(&planned)?;
    let directory = directory(path)?;
    separate(&directory)?;
    Ok(directory)
}

/// Canonical form of a not-yet-created directory: its nearest existing ancestor plus plain
/// components. `None` when the missing suffix is not lexically resolvable.
fn planned_directory(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        let existing = if ancestor.as_os_str().is_empty() {
            Path::new(".")
        } else {
            ancestor
        };
        let Ok(canonical) = fs::canonicalize(existing) else {
            continue;
        };
        let missing = path.strip_prefix(ancestor).ok()?;
        return missing
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
            .then(|| canonical.join(missing));
    }
    None
}

fn directory(path: &Path) -> Result<PathBuf> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::Storage(error.to_string()))?;
    fs::canonicalize(path).map_err(|error| Error::Storage(error.to_string()))
}
