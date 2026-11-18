use super::*;
use crate::input_keymap::canonical_chord;

pub(super) fn parse(file: ConfigFile, scope: Scope, text: &str) -> Result<Value, Vec<Diagnostic>> {
    let error = |path: String, kind, detail: String| Diagnostic {
        file,
        scope,
        path,
        kind,
        detail,
    };
    let mut value = super::json::parse(text).map_err(|e| {
        let kind = if e.to_string().starts_with("duplicate object key") {
            DiagnosticKind::Conflict
        } else {
            DiagnosticKind::Syntax
        };
        vec![error("$".into(), kind, e.to_string())]
    })?;
    let schema: Value = serde_json::from_str(file.schema())
        .map_err(|e| vec![error("$".into(), DiagnosticKind::Syntax, e.to_string())])?;
    let mut errors = vec![];
    check(
        &value,
        &schema,
        schema.get("$defs").unwrap_or(&Value::Null),
        "$",
        &mut |path, kind, detail| errors.push(error(path, kind, detail)),
    );
    if !errors.is_empty() {
        return Err(errors);
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("$schema");
    }
    if file == ConfigFile::Keymap {
        let mut normalized = serde_json::Map::new();
        if let Some(bindings) = value.get("bindings").and_then(Value::as_object) {
            for (chord, action) in bindings {
                let path = format!("$.bindings.{chord}");
                let Some(canonical) = canonical_chord(chord) else {
                    errors.push(error(
                        path,
                        DiagnosticKind::InvalidChord,
                        "invalid chord".into(),
                    ));
                    continue;
                };
                if !action.is_null()
                    && !action
                        .as_str()
                        .is_some_and(|name| crate::named_action(name).is_some())
                {
                    errors.push(error(
                        path.clone(),
                        DiagnosticKind::UnknownAction,
                        "unknown action name".into(),
                    ));
                    continue;
                }
                if normalized
                    .insert(canonical, action.clone())
                    .is_some_and(|old| old != *action)
                {
                    errors.push(error(
                        path,
                        DiagnosticKind::Conflict,
                        "conflicting canonical key chord".into(),
                    ));
                }
            }
        }
        value = serde_json::json!({"bindings":normalized});
    }
    if errors.is_empty() {
        Ok(value)
    } else {
        Err(errors)
    }
}

fn check(
    value: &Value,
    schema: &Value,
    definitions: &Value,
    path: &str,
    emit: &mut impl FnMut(String, DiagnosticKind, String),
) {
    if let Some(reference) = schema["$ref"]
        .as_str()
        .and_then(|s| s.strip_prefix("#/$defs/"))
    {
        if let Some(schema) = definitions.get(reference) {
            check(value, schema, definitions, path, emit);
        } else {
            emit(
                path.into(),
                DiagnosticKind::InvalidValue,
                "unknown schema reference".into(),
            );
        }
        return;
    }
    let matches = |ty: &str| match ty {
        "object" => value.is_object(),
        "string" => value.is_string(),
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "integer" => value.as_u64().is_some(),
        _ => false,
    };
    let valid = schema["type"]
        .as_str()
        .map(&matches)
        .or_else(|| {
            schema["type"]
                .as_array()
                .map(|types| types.iter().filter_map(Value::as_str).any(&matches))
        })
        .unwrap_or(true);
    if !valid {
        emit(
            path.into(),
            DiagnosticKind::InvalidType,
            "unexpected JSON type".into(),
        );
        return;
    }
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            let next = format!("{path}.{key}");
            if let Some(field) = schema.get("properties").and_then(|p| p.get(key)) {
                check(value, field, definitions, &next, emit);
            } else if schema["additionalProperties"] == false {
                emit(
                    next,
                    DiagnosticKind::UnknownKey,
                    "unknown configuration key".into(),
                );
            } else if schema["additionalProperties"].is_object() {
                check(
                    value,
                    &schema["additionalProperties"],
                    definitions,
                    &next,
                    emit,
                );
            }
        }
    }
    if let Some(number) = value.as_u64()
        && (schema["minimum"].as_u64().is_some_and(|min| number < min)
            || schema["maximum"].as_u64().is_some_and(|max| number > max))
    {
        emit(
            path.into(),
            DiagnosticKind::OutOfRange,
            "value outside schema bounds".into(),
        );
    }
    if let Some(allowed) = schema["enum"].as_array()
        && !allowed.contains(value)
    {
        emit(
            path.into(),
            DiagnosticKind::InvalidValue,
            "unknown enum value".into(),
        );
    }
    if schema["format"] == "ui-color" && !value.as_str().is_some_and(super::theme::valid_color) {
        emit(
            path.into(),
            DiagnosticKind::InvalidValue,
            "expected #RRGGBB, default, or an ANSI color name".into(),
        );
    }
}
