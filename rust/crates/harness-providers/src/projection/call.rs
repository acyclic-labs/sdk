//! Calls borrow their canonical argument trees until final HTTP serialization.
use super::{arguments::Arguments, json_string::JsonString};
use crate::Adapter;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value;
use std::borrow::Cow;

pub(super) struct Call<'a> {
    pub adapter: Adapter,
    pub id: String,
    pub name: Cow<'a, str>,
    pub arguments: &'a Value,
    pub native: Option<&'a serde_json::value::RawValue>,
    pub freeform: bool,
    pub omit_gemini_id: bool,
}

impl Serialize for Call<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.adapter == Adapter::Gemini
            && let Some(native) = self.native
        {
            return native.serialize(serializer);
        }
        let mut map = serializer.serialize_map(None)?;
        match self.adapter {
            Adapter::Anthropic => {
                map.serialize_entry("type", "tool_use")?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("name", &self.name)?;
                map.serialize_entry("input", &Arguments(self.arguments))?;
            }
            Adapter::Responses => {
                map.serialize_entry(
                    "type",
                    if self.freeform {
                        "custom_tool_call"
                    } else {
                        "function_call"
                    },
                )?;
                map.serialize_entry("call_id", &self.id)?;
                map.serialize_entry("name", &self.name)?;
                if self.freeform {
                    map.serialize_entry(
                        "input",
                        self.arguments
                            .get("input")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                serde::ser::Error::custom("freeform input must be a string")
                            })?,
                    )?;
                } else {
                    map.serialize_entry("arguments", &JsonString(Arguments(self.arguments)))?;
                }
            }
            Adapter::Chat => {
                #[derive(Serialize)]
                struct Function<'a> {
                    name: &'a str,
                    arguments: JsonString<Arguments<'a>>,
                }
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("type", "function")?;
                map.serialize_entry(
                    "function",
                    &Function {
                        name: &self.name,
                        arguments: JsonString(Arguments(self.arguments)),
                    },
                )?;
            }
            Adapter::Gemini => {
                #[derive(Serialize)]
                struct Function<'a> {
                    #[serde(skip_serializing_if = "Option::is_none")]
                    id: Option<&'a str>,
                    name: &'a str,
                    args: Arguments<'a>,
                }
                map.serialize_entry(
                    "functionCall",
                    &Function {
                        id: (!self.omit_gemini_id).then_some(self.id.as_str()),
                        name: &self.name,
                        args: Arguments(self.arguments),
                    },
                )?;
            }
        }
        map.end()
    }
}
