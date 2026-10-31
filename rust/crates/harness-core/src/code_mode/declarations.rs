use crate::{Error, Result, tool::ToolDefinition};
use serde_json::Value;
use std::{collections::BTreeMap, fmt::Write};

/// Generates the model's TypeScript declarations from the same admitted tool
/// schemas that the ordinary dispatcher validates. JSON constraints remain
/// enforced by that dispatcher; the declarations confer no authority.
pub fn declarations(tools: &[ToolDefinition]) -> Result<String> {
    let mut output = String::from("// Host calls use the normal approval and tool journal.\n");
    output.push_str("declare const input: unknown;\n");
    for (index, tool) in tools.iter().enumerate() {
        for (suffix, schema) in [
            ("Input", &tool.input_schema),
            ("Output", &tool.output_schema),
        ] {
            let root_name = format!("Tool{index}{suffix}");
            let mut aliases = BTreeMap::new();
            aliases.insert("#".to_owned(), root_name.clone());
            for container in ["$defs", "definitions"] {
                if let Some(definitions) = schema.get(container).and_then(Value::as_object) {
                    for name in definitions.keys() {
                        let pointer_name = name.replace('~', "~0").replace('/', "~1");
                        aliases.insert(
                            format!("#/{container}/{pointer_name}"),
                            format!("{root_name}Def{}", aliases.len()),
                        );
                    }
                }
            }
            writeln!(output, "type {root_name} = {};", render(schema, &aliases)?)
                .map_err(|_| invalid())?;
            for (pointer, alias) in &aliases {
                if pointer == "#" {
                    continue;
                }
                let definition = schema.pointer(&pointer[1..]).ok_or_else(invalid)?;
                writeln!(output, "type {alias} = {};", render(definition, &aliases)?)
                    .map_err(|_| invalid())?;
            }
        }
    }
    output.push_str("declare const tools: {\n");
    for (index, tool) in tools.iter().enumerate() {
        let name = serde_json::to_string(&tool.name).map_err(|_| invalid())?;
        writeln!(
            output,
            "  readonly {name}: (args: Tool{index}Input) => Promise<Tool{index}Output>;"
        )
        .map_err(|_| invalid())?;
    }
    output.push_str("};\n");
    // Valid identifiers also have direct host bindings (`shell`, `spawn`, etc.).
    for tool in tools {
        if identifier(&tool.name) {
            let quoted = serde_json::to_string(&tool.name).map_err(|_| invalid())?;
            writeln!(
                output,
                "declare const {}: typeof tools[{quoted}];",
                tool.name
            )
            .map_err(|_| invalid())?;
        }
    }
    Ok(output)
}

fn render(schema: &Value, aliases: &BTreeMap<String, String>) -> Result<String> {
    if schema == &Value::Bool(true) {
        return Ok("unknown".into());
    }
    if schema == &Value::Bool(false) {
        return Ok("never".into());
    }
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return aliases.get(reference).cloned().ok_or_else(invalid);
    }
    if let Some(value) = schema.get("const") {
        return literal(value);
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return join(values.iter().map(literal), " | ");
    }
    for (key, separator) in [("anyOf", " | "), ("oneOf", " | "), ("allOf", " & ")] {
        if let Some(values) = schema.get(key).and_then(Value::as_array) {
            return join(values.iter().map(|value| render(value, aliases)), separator);
        }
    }
    if let Some(types) = schema.get("type").and_then(Value::as_array) {
        return join(
            types.iter().map(|kind| {
                let mut schema = schema.clone();
                schema["type"] = kind.clone();
                render(&schema, aliases)
            }),
            " | ",
        );
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => Ok("string".into()),
        Some("number" | "integer") => Ok("number".into()),
        Some("boolean") => Ok("boolean".into()),
        Some("null") => Ok("null".into()),
        Some("array") => {
            let items = schema
                .get("items")
                .map(|items| render(items, aliases))
                .transpose()?
                .unwrap_or_else(|| "unknown".into());
            Ok(format!("Array<{items}>"))
        }
        Some("object") | None
            if schema.get("properties").is_some()
                || schema.get("type") == Some(&Value::String("object".into())) =>
        {
            let mut output = String::from("{ ");
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, property) in properties {
                    let required = schema
                        .get("required")
                        .and_then(Value::as_array)
                        .is_some_and(|names| {
                            names.iter().any(|value| value.as_str() == Some(name))
                        });
                    write!(
                        output,
                        "{}{}: {}; ",
                        serde_json::to_string(name).map_err(|_| invalid())?,
                        if required { "" } else { "?" },
                        render(property, aliases)?
                    )
                    .map_err(|_| invalid())?;
                }
            }
            if schema.get("additionalProperties") != Some(&Value::Bool(false)) {
                output.push_str("[key: string]: unknown; ");
            }
            output.push('}');
            Ok(output)
        }
        _ => Ok("unknown".into()),
    }
}

fn join(values: impl Iterator<Item = Result<String>>, separator: &str) -> Result<String> {
    let values: Vec<_> = values.collect::<Result<_>>()?;
    if values.is_empty() {
        return Ok("never".into());
    }
    Ok(format!("({})", values.join(separator)))
}

fn literal(value: &Value) -> Result<String> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}

fn identifier(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_' || ch == '$')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$')
        && !matches!(
            value,
            "await"
                | "break"
                | "case"
                | "catch"
                | "class"
                | "const"
                | "continue"
                | "debugger"
                | "default"
                | "delete"
                | "do"
                | "else"
                | "enum"
                | "export"
                | "extends"
                | "false"
                | "finally"
                | "for"
                | "function"
                | "if"
                | "implements"
                | "import"
                | "in"
                | "instanceof"
                | "interface"
                | "let"
                | "new"
                | "null"
                | "package"
                | "private"
                | "protected"
                | "public"
                | "return"
                | "static"
                | "super"
                | "switch"
                | "this"
                | "throw"
                | "true"
                | "try"
                | "typeof"
                | "var"
                | "void"
                | "while"
                | "with"
                | "yield"
        )
}

fn invalid() -> Error {
    Error::ToolRejected {
        code: crate::wire::ErrorCode::Invalid,
        reason: "tool schema cannot be projected to TypeScript",
    }
}
