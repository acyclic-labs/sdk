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
    load_tiers(root, path).map(|(value, _)| value)
}

pub fn load_tiers(root: &Path, path: &Path) -> Result<(Value, bool), String> {
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
    split_tiers(merged)
}

pub fn split_tiers(mut merged: Value) -> Result<(Value, bool), String> {
    let binary = match merged
        .as_object_mut()
        .and_then(|value| value.remove("binary"))
    {
        None | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(_) => return Err("binary must be a boolean".into()),
    };
    Ok((merged, binary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // The envelope changes tier selection and preserves every scenario byte
        // value, including Unicode and arrays. Serialization is lossless.
        #[test]
        fn tier_envelope_round_trips_without_changing_the_scenario(
            binary in any::<bool>(), text in ".{0,64}", values in prop::collection::vec(any::<i64>(), 0..16),
        ) {
            let original = serde_json::json!({"prompt":text,"values":values});
            let mut envelope = original.clone();
            envelope["binary"] = binary.into();
            let encoded = serde_json::to_vec(&envelope)?;
            let (decoded, selected) = split_tiers(serde_json::from_slice(&encoded)?).unwrap();
            prop_assert_eq!(decoded, original);
            prop_assert_eq!(selected, binary);
        }
    }

    #[test]
    fn tier_selection_inherits_overrides_and_rejects_non_booleans()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("case.json");
        for inherited in [false, true] {
            std::fs::write(
                root.path().join("_defaults.json"),
                format!("{{\"binary\":{inherited}}}"),
            )?;
            for selected in [None, Some(false), Some(true)] {
                let mut value = serde_json::json!({"prompt":"unchanged"});
                if let Some(selected) = selected {
                    value["binary"] = selected.into();
                }
                std::fs::write(&path, serde_json::to_vec(&value)?)?;
                let (case, binary) = load_tiers(root.path(), &path)?;
                assert_eq!(binary, selected.unwrap_or(inherited));
                assert_eq!(case, serde_json::json!({"prompt":"unchanged"}));
                assert_eq!(load_value(root.path(), &path)?, case);
            }
        }
        for invalid in ["null", "0", "\"true\"", "[]", "{}"] {
            std::fs::write(&path, format!("{{\"binary\":{invalid}}}"))?;
            assert!(load_tiers(root.path(), &path).is_err());
        }
        Ok(())
    }
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

pub fn seed_settings(
    root: &Path,
    files: &std::collections::BTreeMap<String, String>,
) -> std::io::Result<()> {
    for (name, text) in files {
        let path = root.join(name);
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| std::io::Error::other("settings parent missing"))?,
        )?;
        let text = text
            .replace("{{settings}}", &root.to_string_lossy().replace('\\', "/"))
            .replace(
                "{{python}}",
                if cfg!(windows) { "python" } else { "python3" },
            );
        std::fs::write(path, text)?;
    }
    Ok(())
}
