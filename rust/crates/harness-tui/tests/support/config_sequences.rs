use acyclic_harness_tui::config::{ConfigFile, DiagnosticKind, Scope, parse_bytes};

/// A successful parse must round-trip through the public parser. The independent
/// semantic check requires every format to reject an unknown top-level key.
pub fn exercise(file: ConfigFile, bytes: &[u8]) {
    if let Ok(value) = parse_bytes(file, Scope::User, bytes) {
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(parse_bytes(file, Scope::User, &encoded).unwrap(), value);
        if file == ConfigFile::Keymap {
            let map =
                acyclic_harness_tui::InputKeymap::parse(std::str::from_utf8(&encoded).unwrap())
                    .unwrap();
            let canonical = map.encode();
            assert_eq!(
                acyclic_harness_tui::InputKeymap::parse(&canonical).unwrap(),
                map
            );
            assert_eq!(
                parse_bytes(file, Scope::User, canonical.as_bytes()).unwrap(),
                value
            );
        }
    }
    unknown_key_control(file, false);
}

pub fn unknown_key_control(file: ConfigFile, broken: bool) {
    let mut value: serde_json::Value = serde_json::from_str(file.defaults()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unknown-ui-setting".into(), true.into());
    // Deliberately broken parser accepts unknown keys, modelling a missing schema check.
    let parsed = if broken {
        Ok(value.clone())
    } else {
        parse_bytes(file, Scope::User, value.to_string().as_bytes())
    };
    assert!(
        parsed.is_err_and(|errors| errors.iter().any(|d| d.kind == DiagnosticKind::UnknownKey)),
        "schema must reject unknown keys"
    );
}
