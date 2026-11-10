//! Profile-declared argument templates. A native call's arguments become its
//! canonical capability's arguments by the family layer's data alone; no code
//! here knows a family.
//!
//! A template is JSON. `"$name"` is the native argument `name` and `"$@call_id"`
//! the model's call ID. `"$@operation_id"` is the runtime-owned operation UUID,
//! stable through execution and reconciliation. Four single-key operators compute values:
//! `{"$one_based": name}` (a 1-based line, made 0-based),
//! `{"$through": [first, last]}` (the count of an inclusive 1-based span; an
//! absent `first` is line 1), `{"$case": name, "$default": value, value: template, ...}`
//! and `{"$if": name, "then": template, "else": template}`. Every other value is
//! literal. An absent argument leaves its member out, and an object whose
//! references are all absent is itself absent.
use crate::{Error, Result, tool::ToolInvocation};
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value};

/// One declaration's checked template and refused arguments.
pub(crate) struct Arguments {
    template: Value,
    refuse: Vec<String>,
}

impl Arguments {
    /// Checks the template's operators once, when the profile binds.
    pub(crate) fn new(template: Value, refuse: Vec<String>) -> Result<Self> {
        check(&template)?;
        Ok(Self { template, refuse })
    }

    /// The canonical arguments, funded before any payload is copied.
    pub(crate) fn adapt(
        &self,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        examine(work)?;
        let fields = invocation.arguments.as_object().ok_or_else(|| {
            invalid(
                work,
                "native tool arguments are not an object",
                format_args!("native tool arguments are not an object"),
            )
        })?;
        for name in &self.refuse {
            examine(work)?;
            if fields.get(name.as_str()).is_some_and(set) {
                return Err(invalid(
                    work,
                    "native argument is not supported here",
                    format_args!("{name} is not supported here"),
                ));
            }
        }
        let empty = Value::Object(Map::new());
        let resolved = resolve(&self.template, &(fields, invocation, work))?
            .unwrap_or(Resolved::Value(&empty));
        crate::tool::value(&resolved, u64::MAX, work)
    }
}

type Call<'a> = (
    &'a Map<String, Value>,
    &'a ToolInvocation,
    crate::NativeWork<'a>,
);

fn examine(work: crate::NativeWork<'_>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
    }
    let _ = work;
    Ok(())
}

fn invalid(
    work: crate::NativeWork<'_>,
    reason: &'static str,
    message: std::fmt::Arguments<'_>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = (work, reason);
    Error::Invalid(message.to_string())
}

/// Present and meaningful: not null, false, zero or empty.
fn set(value: &Value) -> bool {
    !matches!(value, Value::Null | Value::Bool(false))
        && value.as_f64() != Some(0.0)
        && value.as_str() != Some("")
}

#[derive(Clone, Copy)]
enum Resolved<'a> {
    Value(&'a Value),
    Text(&'a str),
    Operation(&'a crate::OperationId),
    Number(u64),
    Object(&'a Map<String, Value>, Call<'a>),
}

impl<'a> Resolved<'a> {
    fn set(self) -> bool {
        match self {
            Self::Value(value) => set(value),
            Self::Text(value) => !value.is_empty(),
            _ => true,
        }
    }
}

impl Serialize for Resolved<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Value(value) => value.serialize(serializer),
            Self::Text(value) => value.serialize(serializer),
            Self::Operation(value) => value.serialize(serializer),
            Self::Number(value) => value.serialize(serializer),
            Self::Object(members, call) => {
                let mut map = serializer.serialize_map(None)?;
                // The original resolve already validated and charged this
                // template. Serialization charges the actual graph copy.
                let call = (call.0, call.1, None);
                for (key, template) in *members {
                    if let Some(value) =
                        resolve(template, &call).map_err(serde::ser::Error::custom)?
                    {
                        map.serialize_entry(key, &value)?;
                    }
                }
                map.end()
            }
        }
    }
}

fn field<'a>((fields, invocation, _): &Call<'a>, name: &str) -> Option<Resolved<'a>> {
    match name {
        "@call_id" => Some(Resolved::Text(&invocation.call_id)),
        "@operation_id" => Some(Resolved::Operation(&invocation.operation_id)),
        _ => fields
            .get(name)
            .filter(|value| !value.is_null())
            .map(Resolved::Value),
    }
}

fn line(call: &Call<'_>, name: &str) -> Result<Option<u64>> {
    field(call, name)
        .map(|value| {
            match value {
                Resolved::Value(value) => value.as_u64(),
                _ => None,
            }
            .ok_or_else(|| {
                invalid(
                    call.2,
                    "native argument is not a line number",
                    format_args!("{name} is not a line number"),
                )
            })
        })
        .transpose()
}

fn reference(template: &Value) -> Option<&str> {
    template.as_str().and_then(|text| text.strip_prefix('$'))
}

fn operator(members: &Map<String, Value>) -> Option<&str> {
    members.keys().find_map(|key| {
        key.strip_prefix('$')
            .filter(|key| ["one_based", "through", "case", "if"].contains(key))
    })
}

fn references(template: &Value) -> bool {
    match template {
        Value::Object(members) => operator(members).is_some() || members.values().any(references),
        Value::Array(items) => items.iter().any(references),
        other => reference(other).is_some(),
    }
}

fn resolve<'a>(template: &'a Value, call: &Call<'a>) -> Result<Option<Resolved<'a>>> {
    examine(call.2)?;
    if let Some(name) = reference(template) {
        return Ok(field(call, name));
    }
    let Value::Object(members) = template else {
        return Ok(Some(Resolved::Value(template)));
    };
    let name = |key: &str| members.get(key).and_then(Value::as_str).unwrap_or_default();
    match operator(members) {
        Some("one_based") => Ok(
            line(call, name("$one_based"))?.map(|line| Resolved::Number(line.saturating_sub(1)))
        ),
        Some("through") => {
            let span = &members["$through"];
            let bound = |index: usize| span[index].as_str().unwrap_or_default();
            let Some(last) = line(call, bound(1))? else {
                return Ok(None);
            };
            let first = line(call, bound(0))?.unwrap_or(1);
            last.checked_sub(first)
                .and_then(|span| span.checked_add(1))
                .map(|count| Some(Resolved::Number(count)))
                .ok_or_else(|| {
                    invalid(
                        call.2,
                        "native argument is not a representable inclusive line span",
                        format_args!(
                            "{}..{} is not a representable inclusive line span",
                            bound(0),
                            bound(1)
                        ),
                    )
                })
        }
        Some("case") => {
            let selector = name("$case");
            let value = field(call, selector)
                .or_else(|| members.get("$default").map(Resolved::Value))
                .ok_or_else(|| {
                    invalid(
                        call.2,
                        "native selector is required",
                        format_args!("{selector} is required"),
                    )
                })?;
            let mut operation = [0; 36];
            let text = match value {
                Resolved::Value(value) => value.as_str(),
                Resolved::Text(value) => Some(value),
                Resolved::Operation(value) => Some(
                    uuid::Uuid::from_bytes(value.into_bytes())
                        .hyphenated()
                        .encode_lower(&mut operation) as &str,
                ),
                _ => None,
            };
            let case = text
                .filter(|case| !case.starts_with('$'))
                .and_then(|case| members.get(case))
                .ok_or_else(|| {
                    invalid(
                        call.2,
                        "native selector is not supported",
                        format_args!("{selector} is not supported"),
                    )
                })?;
            resolve(case, call)
        }
        Some(_) => match members.get(if field(call, name("$if")).is_some_and(Resolved::set) {
            "then"
        } else {
            "else"
        }) {
            Some(branch) => resolve(branch, call),
            None => Ok(None),
        },
        None => {
            let (mut dynamic, mut found) = (false, false);
            for member in members.values() {
                let value = resolve(member, call)?;
                if references(member) {
                    dynamic = true;
                    found |= value.is_some();
                }
            }
            Ok((found || !dynamic).then_some(Resolved::Object(members, *call)))
        }
    }
}

/// Rejects an unknown or malformed operator, so a layer typo fails at bind.
fn check(template: &Value) -> Result<()> {
    let malformed = || Error::Invalid("native argument template is malformed".into());
    match template {
        Value::Object(members) => {
            let names = |key: &str| members.get(key).is_some_and(Value::is_string);
            let shape_ok = match operator(members) {
                Some("one_based") => members.len() == 1 && names("$one_based"),
                Some("through") => {
                    members.len() == 1
                        && members["$through"].as_array().is_some_and(|span| {
                            span.len() == 2 && span.iter().all(Value::is_string)
                        })
                }
                Some("case") => {
                    names("$case")
                        && members
                            .keys()
                            .all(|key| !key.starts_with('$') || key == "$case" || key == "$default")
                }
                Some(_) => {
                    names("$if")
                        && members.contains_key("then")
                        && members
                            .keys()
                            .all(|key| ["$if", "then", "else"].contains(&key.as_str()))
                }
                None => !members.keys().any(|key| key.starts_with('$')),
            };
            if !shape_ok {
                return Err(malformed());
            }
            members
                .iter()
                .filter(|(key, _)| !key.starts_with('$'))
                .try_for_each(|(_, member)| check(member))
        }
        Value::Array(items) => items.iter().try_for_each(check),
        _ => Ok(()),
    }
}
