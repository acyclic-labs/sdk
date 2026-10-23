use crate::Error;
use std::path::{Component, Path};
/// Resolves a path argument within the managed workspace.
pub(crate) fn workspace_path_argument(mount: &Path, argument: &str) -> Result<String, Error> {
    if argument.contains('\0') {
        return Err(Error::Invalid("path contains NUL".to_owned()));
    }
    let argument = Path::new(argument);
    let relative = if argument.is_absolute() {
        argument
            .strip_prefix(mount)
            .map_err(|_| Error::Invalid("path is outside the managed workspace".to_owned()))?
    } else {
        argument
    };
    let mut result = String::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name.to_str().ok_or_else(|| {
                    Error::Invalid("compatibility path is not valid Unicode".to_owned())
                })?;
                result.push('/');
                result.push_str(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::Invalid(
                    "path escapes the managed workspace".to_owned(),
                ));
            }
        }
    }
    if result.is_empty() {
        return Err(Error::Invalid(
            "path must name a file inside the managed workspace".to_owned(),
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: canonical relative paths and their absolute spellings
        // resolve identically, and neither NUL nor parent traversal is accepted.
        #[test]
        fn paths_round_trip_inside_the_workspace(
            names in prop::collection::vec("[a-zA-Z0-9_-]{1,20}", 1..8),
        ) {
            let mount = std::env::temp_dir();
            let path = names.join("/");
            let canonical = workspace_path_argument(&mount, &path).unwrap();
            prop_assert_eq!(&canonical, &format!("/{path}"));
            let absolute = mount.join(&path);
            prop_assert_eq!(workspace_path_argument(&mount, absolute.to_str().unwrap()).unwrap(), canonical);
            prop_assert!(workspace_path_argument(&mount, &format!("{path}/../escape")).is_err(),
                "parent traversal accepted");
            prop_assert!(workspace_path_argument(&mount, &format!("{path}\0suffix")).is_err(),
                "NUL accepted");
        }
    }
}
