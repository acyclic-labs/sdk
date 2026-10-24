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
                platform_case(
                    serde_json::from_slice(&bytes)
                        .map_err(|e| format!("{}: {e}", defaults.display()))?,
                    cfg!(windows),
                )?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("{}: {error}", defaults.display())),
        }
    }
    merge(
        &mut merged,
        platform_case(
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?,
            cfg!(windows),
        )?,
    );
    Ok(merged)
}

// Native shells have different languages. Fixtures author both programs and keep
// the same file, output, lifecycle and wire oracles; execution never translates
// an arbitrary model command or adds a shell alias to the production toolset.
fn platform_case(mut value: Value, windows: bool) -> Result<Value, String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Platforms {
        windows: Option<serde_json::Map<String, Value>>,
        unix: Option<serde_json::Map<String, Value>>,
    }
    let Some(overrides) = value.as_object_mut().and_then(|v| v.remove("platforms")) else {
        return Ok(value);
    };
    let overrides: Platforms = serde_json::from_value(overrides).map_err(|e| e.to_string())?;
    if let Some(selected) = if windows {
        overrides.windows
    } else {
        overrides.unix
    } {
        merge(&mut value, Value::Object(selected));
    }
    Ok(value)
}

#[test]
fn authored_platform_programs_keep_their_oracles_and_case_precedence() {
    use serde_json::json;
    let fixture = json!({
        "turns":["unix program"], "expect":{"stdout":"same bytes","exit":7},
        "platforms":{"windows":{"turns":["windows program"]}}
    });
    for windows in [false, true] {
        let mut actual = platform_case(fixture.clone(), windows).unwrap();
        assert_eq!(
            actual.get("turns"),
            Some(&json!([if windows {
                "windows program"
            } else {
                "unix program"
            }]))
        );
        assert_eq!(
            actual.get("expect"),
            Some(&json!({"stdout":"same bytes","exit":7}))
        );
        assert!(actual.get("platforms").is_none());
        // A case's common field still overrides its parent's platform default.
        merge(
            &mut actual,
            platform_case(json!({"turns":[]}), windows).unwrap(),
        );
        assert_eq!(actual.get("turns"), Some(&json!([])));
    }
    for bad in [
        json!({"windwos":{}}),
        json!({"windows":[]}),
        json!({"unix":"program"}),
    ] {
        assert!(platform_case(json!({"platforms":bad}), true).is_err());
    }
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
