use super::MountTable;
use crate::{Error, Result};
use std::collections::BTreeMap;
use std::fmt::Write as _;

impl<T> MountTable<T> {
    /// Explicit process discovery hints, including a separate mode for each path.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let mut environment = BTreeMap::new();
        for (relative, mount) in self.mounts() {
            let mut key = String::from("ACYCLIC_VOLUME_");
            let name = if relative.is_empty() {
                mount.view.name.as_str()
            } else {
                relative
            };
            for component in name.split('/').filter(|part| *part != ".volumes") {
                if !key.ends_with('_') {
                    key.push('_');
                }
                for byte in component.bytes() {
                    if byte.is_ascii_alphanumeric() {
                        key.push(char::from(byte.to_ascii_uppercase()));
                    } else {
                        write!(key, "_{byte:02X}")
                            .map_err(|error| Error::Storage(error.to_string()))?;
                    }
                }
            }
            let path = mount
                .native_root
                .clone()
                .unwrap_or_else(|| self.root.join(relative));
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
            line.push_str(" (");
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
