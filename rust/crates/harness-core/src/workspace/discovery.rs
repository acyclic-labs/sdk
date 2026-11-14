use super::MountTable;
use crate::{Error, Result};
use std::collections::BTreeMap;

impl<T> MountTable<T> {
    /// One JSON map of visible native workspace names to paths for this target.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let mut workspaces = BTreeMap::new();
        for (relative, path, mount) in self.native_mounts() {
            let name = if relative.is_empty() {
                mount.view.name.as_str()
            } else {
                relative
                    .strip_prefix("volume:///")
                    .or_else(|| relative.strip_prefix("volume://"))
                    .ok_or_else(|| Error::Invalid("invalid volume address".into()))?
            };
            let path = path
                .to_str()
                .ok_or_else(|| Error::Invalid("task mount path is not UTF-8".into()))?;
            if workspaces
                .insert(name.to_owned(), path.to_owned())
                .is_some()
            {
                return Err(Error::Conflict("workspace discovery names collide".into()));
            }
        }
        Ok(BTreeMap::from([(
            "ACYCLIC_WORKSPACES".into(),
            serde_json::to_string(&workspaces)
                .map_err(|error| Error::Storage(error.to_string()))?,
        )]))
    }

    /// Task-local addresses and modes, independent of host projection paths.
    pub fn context_line(&self) -> String {
        let mut line = String::from("Volumes: ");
        for (index, (path, mount)) in self.mounts().enumerate() {
            if index != 0 {
                line.push_str(", ");
            }
            line.push_str(if path.is_empty() { "." } else { path });
            line.push_str(" (");
            if matches!(mount.native_root, super::NativeRoot::Logical) {
                line.push_str("logical, ");
            }
            line.push_str(mode_name(mount.mode));
            line.push(')');
        }
        line
    }
}

fn mode_name(mode: super::MountMode) -> &'static str {
    if mode.create_only() {
        "create"
    } else if mode.read_only() {
        "ro"
    } else {
        "rw"
    }
}
