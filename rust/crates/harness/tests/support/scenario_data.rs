use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn discover(root: &Path, paths: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            discover(&path, paths)?;
        } else if path.extension().is_some_and(|ext| ext == "json")
            && path
                .file_name()
                .is_some_and(|name| name != "_defaults.json")
        {
            paths.push(path);
        }
    }
    Ok(())
}

pub fn load_value(root: &Path, path: &Path) -> Result<Value, String> {
    let relative = path.strip_prefix(root).map_err(|e| e.to_string())?;
    let mut merged = Value::Object(serde_json::Map::new());
    let mut directory = root.to_path_buf();
    let parents = relative.parent().ok_or("scenario has no parent")?;
    for component in std::iter::once(None).chain(parents.components().map(Some)) {
        if let Some(component) = component {
            directory.push(component);
        }
        let defaults = directory.join("_defaults.json");
        match std::fs::read(&defaults) {
            Ok(bytes) => merge(
                &mut merged,
                serde_json::from_slice(&bytes)
                    .map_err(|e| format!("{}: {e}", defaults.display()))?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("{}: {error}", defaults.display())),
        }
    }
    merge(
        &mut merged,
        serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?,
    );
    Ok(merged)
}

fn merge(base: &mut Value, delta: Value) {
    match (base, delta) {
        (Value::Object(base), Value::Object(delta)) => {
            for (key, value) in delta {
                merge(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (base, delta) => *base = delta,
    }
}
