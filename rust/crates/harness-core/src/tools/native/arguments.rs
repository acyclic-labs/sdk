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

    /// The canonical arguments for one native call.
    pub(crate) fn adapt(&self, invocation: &ToolInvocation) -> Result<Value> {
        let fields = invocation
            .arguments
            .as_object()
            .ok_or_else(|| Error::Invalid("native tool arguments are not an object".into()))?;
        if let Some(name) = self
            .refuse
            .iter()
            .find(|name| fields.get(name.as_str()).is_some_and(set))
        {
            return Err(Error::Invalid(format!("{name} is not supported here")));
        }
        Ok(resolve(&self.template, &(fields, invocation))?
            .unwrap_or_else(|| Value::Object(Map::new())))
    }
}

type Call<'a> = (&'a Map<String, Value>, &'a ToolInvocation);

/// Present and meaningful: not null, false, zero or empty.
fn set(value: &Value) -> bool {
    !matches!(value, Value::Null | Value::Bool(false))
        && value.as_f64() != Some(0.0)
        && value.as_str() != Some("")
}

fn field<'a>((fields, invocation): &Call<'a>, name: &str) -> Option<Value> {
    match name {
        "@call_id" => Some(Value::String(invocation.call_id.clone())),
        "@operation_id" => Some(Value::String(invocation.operation_id.to_string())),
        _ => fields.get(name).filter(|value| !value.is_null()).cloned(),
    }
}

fn line(call: &Call<'_>, name: &str) -> Result<Option<u64>> {
    field(call, name)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| Error::Invalid(format!("{name} is not a line number")))
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

fn resolve(template: &Value, call: &Call<'_>) -> Result<Option<Value>> {
    if let Some(name) = reference(template) {
        return Ok(field(call, name));
    }
    let Value::Object(members) = template else {
        return Ok(Some(template.clone()));
    };
    let name = |key: &str| members.get(key).and_then(Value::as_str).unwrap_or_default();
    match operator(members) {
        Some("one_based") => {
            Ok(line(call, name("$one_based"))?.map(|line| Value::from(line.saturating_sub(1))))
        }
        Some("through") => {
            let span = &members["$through"];
            let bound = |index: usize| span[index].as_str().unwrap_or_default();
            let Some(last) = line(call, bound(1))? else {
                return Ok(None);
            };
            let first = line(call, bound(0))?.unwrap_or(1);
            last.checked_sub(first)
                .map(|span| Some(Value::from(span + 1)))
                .ok_or_else(|| Error::Invalid(format!("{} precedes {}", bound(1), bound(0))))
        }
        Some("case") => {
            let selector = name("$case");
            let value = field(call, selector)
                .or_else(|| members.get("$default").cloned())
                .ok_or_else(|| Error::Invalid(format!("{selector} is required")))?;
            let case = value
                .as_str()
                .filter(|case| !case.starts_with('$'))
                .and_then(|case| members.get(case))
                .ok_or_else(|| Error::Invalid(format!("{selector} {value} is not supported")))?;
            resolve(case, call)
        }
        Some(_) => match members.get(if field(call, name("$if")).is_some_and(|v| set(&v)) {
            "then"
        } else {
            "else"
        }) {
            Some(branch) => resolve(branch, call),
            None => Ok(None),
        },
        None => {
            let (mut resolved, mut dynamic, mut found) = (Map::new(), false, false);
            for (key, member) in members {
                let value = resolve(member, call)?;
                if references(member) {
                    dynamic = true;
                    found |= value.is_some();
                }
                if let Some(value) = value {
                    resolved.insert(key.clone(), value);
                }
            }
            Ok((found || !dynamic).then_some(Value::Object(resolved)))
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
