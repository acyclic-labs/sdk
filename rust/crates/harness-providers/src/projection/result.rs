//! Results borrow canonical JSON until the final wire string is encoded.
use super::json_string::JsonString;
use crate::Adapter;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value;
use std::borrow::Cow;

pub(crate) enum Payload<'a> {
    Json(&'a Value),
    Text(Cow<'a, str>),
}

impl Serialize for Payload<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Json(value) => JsonString(value).serialize(serializer),
            Self::Text(text) => text.serialize(serializer),
        }
    }
}

pub(crate) struct ToolResult<'a> {
    pub adapter: Adapter,
    pub id: String,
    pub name: Cow<'a, str>,
    pub payload: Payload<'a>,
    pub error: bool,
}

impl Serialize for ToolResult<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self.adapter {
            Adapter::Anthropic => {
                map.serialize_entry("content", &self.payload)?;
                if self.error {
                    map.serialize_entry("is_error", &true)?;
                }
                map.serialize_entry("tool_use_id", &self.id)?;
                map.serialize_entry("type", "tool_result")?;
            }
            Adapter::Responses => {
                map.serialize_entry("call_id", &self.id)?;
                map.serialize_entry("output", &self.payload)?;
                map.serialize_entry("type", "function_call_output")?;
            }
            Adapter::Chat => {
                map.serialize_entry("content", &self.payload)?;
                map.serialize_entry("role", "tool")?;
                map.serialize_entry("tool_call_id", &self.id)?;
            }
            Adapter::Gemini => {
                #[derive(Serialize)]
                struct Response<'a> {
                    id: &'a str,
                    name: &'a str,
                    response: Output<'a>,
                }
                struct Output<'a>(&'a ToolResult<'a>);
                impl Serialize for Output<'_> {
                    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                        let mut map = serializer.serialize_map(Some(1))?;
                        map.serialize_entry(
                            if self.0.error { "error" } else { "output" },
                            &self.0.payload,
                        )?;
                        map.end()
                    }
                }
                map.serialize_entry(
                    "functionResponse",
                    &Response {
                        id: &self.id,
                        name: &self.name,
                        response: Output(self),
                    },
                )?;
            }
        }
        map.end()
    }
}
