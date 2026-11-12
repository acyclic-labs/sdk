use super::MountTable;
use crate::{Error, Result};
use std::collections::BTreeMap;
use std::fmt::Write as _;

impl<T> MountTable<T> {
    /// Explicit native process hints, including each projected path's access
    /// mode. A native command's table names its own views of the volumes.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let mut environment = BTreeMap::new();
        for (relative, path, mount) in self.native_mounts() {
            let key = variable(relative, &mount.view.name)?;
            let path = path
                .to_str()
                .ok_or_else(|| Error::Invalid("task mount path is not UTF-8".into()))?;
            if environment.insert(key.clone(), path.into()).is_some()
                || environment
                    .insert(format!("{key}_MODE"), mode_name(mount.mode).into())
                    .is_some()
            {
                return Err(Error::Conflict("volume discovery hints collide".into()));
            }
        }
        Ok(environment)
    }

    /// One line describing the namespace, for the task's context projection.
    pub fn context_line(&self) -> String {
        let mut line = String::from("Volumes: ");
        for (index, (path, mount)) in self.mounts().enumerate() {
            if index != 0 {
                line.push_str(", ");
            }
            line.push_str(if path.is_empty() { "." } else { path });
            // Each native command runs in its own view of a volume, so the
            // stable name of its path is the discovery variable, never a path.
            if mount.native_root.override_path().is_some()
                && let Ok(key) = variable(path, &mount.view.name)
            {
                line.push_str(" at $");
                line.push_str(&key);
            }
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

/// `ACYCLIC_VOLUME_<NAME>` for one mount: its address components, uppercased,
/// with every other byte hex-escaped.
fn variable(relative: &str, name: &str) -> Result<String> {
    let mut key = String::from("ACYCLIC_VOLUME_");
    let name = if relative.is_empty() {
        name
    } else {
        relative
            .strip_prefix("volume:///")
            .or_else(|| relative.strip_prefix("volume://"))
            .ok_or_else(|| Error::Invalid("invalid volume address".into()))?
    };
    for component in name.split('/') {
        if !key.ends_with('_') {
            key.push('_');
        }
        for byte in component.bytes() {
            if byte.is_ascii_alphanumeric() {
                key.push(char::from(byte.to_ascii_uppercase()));
            } else {
                write!(key, "_{byte:02X}").map_err(|error| Error::Storage(error.to_string()))?;
            }
        }
    }
    Ok(key)
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
