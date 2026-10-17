use acyclic_harness_tui::config::{ConfigFile, DiagnosticKind, Scope, parse_bytes};

/// A successful parse must round-trip through the public parser. The independent
/// semantic check requires every format to reject an unknown top-level key.
pub fn exercise(file: ConfigFile, bytes: &[u8]) {
    if let Ok(value) = parse_bytes(file, Scope::User, bytes) {
        let encoded = value.to_string();
        assert_eq!(
            parse_bytes(file, Scope::User, encoded.as_bytes()),
            Ok(value.clone())
        );
        if file == ConfigFile::Keymap {
            let parsed = acyclic_harness_tui::InputKeymap::parse(&encoded);
            assert!(parsed.is_ok(), "{parsed:?}");
            if let Ok(map) = parsed {
                let canonical = map.encode();
                assert_eq!(acyclic_harness_tui::InputKeymap::parse(&canonical), Ok(map));
                assert_eq!(
                    parse_bytes(file, Scope::User, canonical.as_bytes()),
                    Ok(value)
                );
            }
        }
    }
    unknown_key_control(file, false);
}

pub fn unknown_key_control(file: ConfigFile, broken: bool) {
    let mut object: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(file.defaults()).unwrap_or_default();
    object.insert("unknown-ui-setting".into(), true.into());
    let value = serde_json::Value::Object(object);
    // Deliberately broken parser accepts unknown keys, modelling a missing schema check.
    let parsed = if broken {
        Ok(value)
    } else {
        parse_bytes(file, Scope::User, value.to_string().as_bytes())
    };
    assert!(
        parsed.is_err_and(|errors| errors.iter().any(|d| d.kind == DiagnosticKind::UnknownKey)),
        "schema must reject unknown keys"
    );
}
