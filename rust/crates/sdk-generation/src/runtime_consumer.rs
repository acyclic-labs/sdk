//! Rust-owned runtime consumer rendering for the additional language lane.
//!
//! The authority manifest is the only input that contains RPC identity, shape,
//! and type information.  The language launchers deliberately know nothing
//! about protobuf syntax or target naming rules; they invoke this renderer.

use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub enum Language {
    Elixir,
    Erlang,
    CommonLisp,
}

#[derive(Debug, Clone)]
struct Call {
    package: String,
    service: String,
    method: String,
    request: String,
    response: String,
    shape: String,
    rpc: String,
    source: String,
    request_base64: String,
}

#[derive(Debug, Clone)]
struct TypedRecord {
    request_base64: String,
    request_frames: Vec<String>,
}

fn typed_records(path: Option<&Path>) -> Result<std::collections::BTreeMap<String, TypedRecord>, String> {
    let Some(path) = path else {
        return Ok(std::collections::BTreeMap::new());
    };
    let bytes = fs::read(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    let records = document
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} is missing records", path.display()))?;
    let mut output = std::collections::BTreeMap::new();
    for record in records {
        let rpc = required_string(record, "rpc")?.to_string();
        let request_base64 = record
            .get("request_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("typed request record {rpc} is missing request_base64"))?
            .to_string();
        let request_frames = record
            .get("request_frames")
            .and_then(Value::as_array)
            .map(|frames| {
                frames
                    .iter()
                    .map(|frame| {
                        frame
                            .get("request_base64")
                            .or_else(|| frame.get("bytes_base64"))
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .ok_or_else(|| "typed request frame is missing bytes_base64".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        if output.insert(rpc.clone(), TypedRecord { request_base64, request_frames }).is_some() {
            return Err(format!("typed request manifest contains duplicate RPC {rpc}"));
        }
    }
    Ok(output)
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("authority RPC is missing non-empty {key}"))
}

fn pascal(value: &str) -> String {
    value
        .trim_start_matches('.')
        .split('.')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn snake(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 4);
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            output.push('_');
        }
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    output
}

fn lisp_name(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 4);
    let chars: Vec<char> = value.chars().collect();
    for (index, character) in chars.iter().copied().enumerate() {
        if index > 0
            && chars[index - 1].is_ascii_lowercase()
            && character.is_ascii_uppercase()
        {
            output.push('-');
        }
        if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('-');
        }
    }
    output.trim_matches('-').to_string()
}

fn method_parts(rpc: &str) -> Result<(String, String, String), String> {
    let (service_path, method) = rpc
        .split_once('/')
        .ok_or_else(|| format!("authority RPC has no method separator: {rpc}"))?;
    let split = service_path.rsplitn(2, '.').collect::<Vec<_>>();
    if split.len() != 2 || split[0].is_empty() || split[1].is_empty() || method.is_empty() {
        return Err(format!("authority RPC has invalid service identity: {rpc}"));
    }
    Ok((split[1].to_string(), split[0].to_string(), method.to_string()))
}

fn calls(authority: &Value, typed: &std::collections::BTreeMap<String, TypedRecord>) -> Result<Vec<Call>, String> {
    if authority.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.rust-authority.v1")
    {
        return Err("runtime consumers require acyclic.sdk.rust-authority.v1".into());
    }
    let families = authority
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| "authority is missing families".to_string())?;
    let mut output = Vec::new();
    for family in families {
        let source = required_string(family, "source")?.to_string();
        let methods = family
            .get("rpc_methods")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("authority family {source} is missing rpc_methods"))?;
        for method in methods {
            let rpc = required_string(method, "rpc")?.to_string();
            let (package, service, method_name) = method_parts(&rpc)?;
            let request = required_string(method, "request")?.to_string();
            let response = required_string(method, "response")?.to_string();
            let shape = match required_string(method, "shape")? {
                "server" => "server_stream".to_string(),
                "client" => "client_stream".to_string(),
                value => value.to_string(),
            };
            if !matches!(shape.as_str(), "unary" | "server_stream" | "client_stream" | "bidi") {
                return Err(format!("authority RPC {rpc} has unsupported shape {shape}"));
            }
            let request_base64 = typed
                .get(&rpc)
                .map(|record| record.request_base64.clone())
                .ok_or_else(|| format!("typed request manifest is missing {rpc}"))?;
            output.push(Call {
                package,
                service,
                method: method_name,
                request,
                response,
                shape,
                rpc,
                source: source.clone(),
                request_base64,
            });
        }
    }
    if output.is_empty() {
        return Err("authority contains no RPC methods".into());
    }
    Ok(output)
}

/// Write the ordered, Rust-owned streaming request projection consumed by
/// runtime language probes.  Every frame keeps its manifest sequence identity;
/// repeated messages are retained verbatim rather than deduplicated by RPC.
pub fn write_stream_scenarios(authority_path: &Path, typed_path: &Path, output: &Path) -> Result<(), String> {
    let authority: Value = serde_json::from_slice(
        &fs::read(authority_path)
            .map_err(|error| format!("read {}: {error}", authority_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", authority_path.display()))?;
    let typed = typed_records(Some(typed_path))?;
    let families = authority
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| "authority is missing families".to_string())?;
    let mut lines = Vec::new();
    for family in families {
        let methods = family
            .get("rpc_methods")
            .and_then(Value::as_array)
            .ok_or_else(|| "authority family is missing rpc_methods".to_string())?;
        for method in methods {
            let rpc = required_string(method, "rpc")?;
            let shape = required_string(method, "shape")?;
            if !matches!(shape, "server" | "server_stream" | "client" | "client_stream" | "bidi") {
                continue;
            }
            let record = typed
                .get(rpc)
                .ok_or_else(|| format!("typed request manifest is missing {rpc}"))?;
            let frames = if record.request_frames.is_empty() {
                vec![record.request_base64.clone()]
            } else {
                record.request_frames.clone()
            };
            for (sequence, frame) in frames.into_iter().enumerate() {
                // Empty Base64 is a valid canonical protobuf encoding for an
                // empty request message and must remain observable.
                lines.push(format!("{rpc}#{sequence}\t{frame}"));
            }
        }
    }
    if lines.is_empty() {
        return Err("authority contains no streaming request scenarios".into());
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(output, format!("{}\n", lines.join("\n")))
        .map_err(|error| format!("write {}: {error}", output.display()))
}

fn shape_atom(shape: &str) -> &str {
    match shape {
        "server_stream" => "server_stream",
        "client_stream" => "client_stream",
        other => other,
    }
}

fn elixir_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let service_module = format!("{}.{}.Stub", pascal(&call.package), call.service);
            format!(
                "  {{{}, :{}, {}, {}, :{}, \"{}\", \"{}\", [{}]}},",
                service_module,
                snake(&call.method),
                pascal(&call.request),
                pascal(&call.response),
                shape_atom(&call.shape),
                call.rpc,
                call.request_base64,
                ""
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn proto_module(source: &str) -> String {
    let stem = Path::new(source)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("contract");
    format!("{}_pb", snake(stem))
}

fn erlang_message_name(value: &str) -> String {
    let parts = value.trim_start_matches('.').split('.').collect::<Vec<_>>();
    let prefix = if parts.first() == Some(&"acyclic") {
        parts.get(1).copied().unwrap_or("message")
    } else {
        parts.first().copied().unwrap_or("message")
    };
    let message = parts.last().copied().unwrap_or("message");
    [erlang_name(prefix), erlang_name(message)]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}
fn erlang_name(value: &str) -> String {
    let normalized = snake(value);
    let mut output = String::with_capacity(normalized.len() + 2);
    let mut previous: Option<char> = None;
    for character in normalized.chars() {
        if character.is_ascii_digit() && previous.is_some_and(|value| value.is_ascii_alphabetic()) {
            output.push('_');
        }
        output.push(character);
        previous = Some(character);
    }
    output
}

fn erlang_segment(value: &str) -> String {
    erlang_name(value)
}

fn erlang_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let package_prefix = call
                .package
                .split('.')
                .map(erlang_segment)
                .collect::<Vec<_>>()
                .join("_");
            let module = [package_prefix, erlang_name(&call.service), "client".into()]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join("_");
            let request = erlang_message_name(&call.request);
            let response = erlang_message_name(&call.response);
            format!(
                "    {{{}, {}, {}, {}, \"{}\", \"{}\", \"{}\", \"{}\", [{}]}}{}",
                module,
                erlang_name(&call.method),
                shape_atom(&call.shape),
                proto_module(&call.source),
                request,
                response,
                call.rpc,
                call.request_base64,
                "",
                if std::ptr::eq(call, calls.last().unwrap()) { "" } else { "," }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn lisp_type(value: &str) -> (String, String) {
    let mut pieces = value.trim_start_matches('.').rsplitn(2, '.');
    let class = lisp_name(pieces.next().unwrap_or(value));
    let package = pieces.next().unwrap_or("").to_ascii_uppercase();
    (package, class)
}

fn lisp_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let request = lisp_type(&call.request);
            let response = lisp_type(&call.response);
            format!(
                "  (\"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" [{}])",
                call.package.to_ascii_uppercase(),
                lisp_name(&call.service),
                lisp_name(&call.method),
                if request.0.is_empty() { call.package.to_ascii_uppercase() } else { request.0 },
                request.1,
                if response.0.is_empty() { call.package.to_ascii_uppercase() } else { response.0 },
                response.1,
                call.shape.replace('_', "-"),
                call.rpc,
                call.request_base64,
                ""
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn render(language: Language, authority_path: &Path, typed_path: &Path, output: &Path) -> Result<(), String> {
    let bytes = fs::read(authority_path)
        .map_err(|error| format!("read {}: {error}", authority_path.display()))?;
    let authority: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", authority_path.display()))?;
    let typed = typed_records(Some(typed_path))?;
    let calls = calls(&authority, &typed)?;
    let (template, call_text) = match language {
        Language::Elixir => (include_str!("../templates/elixir.tmpl"), elixir_calls(&calls)),
        Language::Erlang => (include_str!("../templates/erlang.tmpl"), erlang_calls(&calls)),
        Language::CommonLisp => (
            include_str!("../templates/common-lisp.tmpl"),
            lisp_calls(&calls),
        ),
    };
    let rendered = template.replace("{{CALLS}}", &call_text);
    if rendered.contains("{{CALLS}}") {
        return Err("runtime-consumer template did not contain a single call slot".into());
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(output, rendered).map_err(|error| format!("write {}: {error}", output.display()))
}

pub fn parse_language(value: &str) -> Result<Language, String> {
    match value {
        "elixir" => Ok(Language::Elixir),
        "erlang" => Ok(Language::Erlang),
        "common-lisp" | "lisp" => Ok(Language::CommonLisp),
        _ => Err(format!("unsupported runtime consumer language {value}")),
    }
}

pub fn run_from_args(args: &[String]) -> Result<(), String> {
    let mut language = None;
    let mut authority = None;
    let mut output = None;
    let mut typed = None;
    let mut stream_scenarios = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--language" => {
                index += 1;
                language = args.get(index).map(|value| parse_language(value)).transpose()?;
            }
            "--authority" => {
                index += 1;
                authority = args.get(index).map(PathBuf::from);
            }
            "--output" => {
                index += 1;
                output = args.get(index).map(PathBuf::from);
            }
            "--typed-request-manifest" => {
                index += 1;
                typed = args.get(index).map(PathBuf::from);
            }
            "--stream-scenarios" => {
                index += 1;
                stream_scenarios = args.get(index).map(PathBuf::from);
            }
            value => return Err(format!("unknown runtime-consumer argument {value}")),
        }
        index += 1;
    }
    let authority = authority.ok_or_else(|| "--authority is required".to_string())?;
    let typed = typed.ok_or_else(|| "--typed-request-manifest is required".to_string())?;
    if let Some(output) = stream_scenarios {
        return write_stream_scenarios(&authority, &typed, &output);
    }
    render(
        language.ok_or_else(|| "--language is required".to_string())?,
        &authority,
        &typed,
        &output.ok_or_else(|| "--output is required".to_string())?,
    )
}
