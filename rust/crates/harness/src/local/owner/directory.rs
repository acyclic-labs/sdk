use crate::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(crate) fn separated_directory(path: &Path, checkout: Option<&Path>) -> Result<PathBuf> {
    planned_separated_directory(path, checkout)?;
    let directory = directory(path)?;
    planned_separated_directory(&directory, checkout)?;
    Ok(directory)
}

/// The same separation check without creating directories or capturing a root.
pub(in crate::local) fn planned_separated_directory(
    path: &Path,
    checkout: Option<&Path>,
) -> Result<()> {
    let Some(checkout) = checkout else {
        return Ok(());
    };
    let planned = planned_directory(path).ok_or_else(|| {
        Error::Invalid("local state directory has an unresolved path suffix".into())
    })?;
    let checkout = fs::canonicalize(checkout).map_err(|error| Error::Storage(error.to_string()))?;
    if checkout.starts_with(&planned) || planned.starts_with(&checkout) {
        return Err(Error::Invalid(format!(
            "local state directory must be separate from the checkout: state {} and checkout {} nest; recovery: run from the project directory (never /) or set XDG_DATA_HOME outside it",
            planned.display(),
            checkout.display()
        )));
    }
    Ok(())
}

/// Canonical form of a not-yet-created directory: its nearest existing ancestor plus plain
/// components. `None` when the missing suffix is not lexically resolvable.
pub(super) fn planned_directory(path: &Path) -> Option<PathBuf> {
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
    acyclic_native_runtime::private_file::create_directory_all(path)
        .map_err(|error| Error::Storage(error.to_string()))?;
    fs::canonicalize(path).map_err(|error| Error::Storage(error.to_string()))
}
