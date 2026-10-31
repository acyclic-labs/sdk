//! Ordered declarations borrowed from an admitted profile. This is wire
//! projection only: executable capability and revision admission stay with
//! the profile owner, and server tools never become client function calls.
use crate::{Family, RenderError};
use serde::{Serialize, ser::SerializeSeq};
use std::borrow::Cow;

/// One model-visible declaration. The schema view is supplied by the shared
/// schema boundary, so rendering never clones or transforms a schema tree.
pub enum ToolDeclaration<'a, P> {
    /// A client function bound to its original executor and schema.
    Function {
        /// Provider-valid name already chosen by the canonical name binder.
        name: Cow<'a, str>,
        /// Model-facing description.
        description: &'a str,
        /// Borrowed model input schema view.
        parameters: P,
    },
    /// Provider-owned web search, without a client function or executor.
    WebSearch,
}

/// A declaration list admitted once and serialized in its original order.
/// Creating it transfers the vector; serialization allocates no staging tree.
pub struct ToolDeclarations<'a, P> {
    family: Family,
    declarations: Vec<ToolDeclaration<'a, P>>,
}

impl<'a, P> ToolDeclarations<'a, P> {
    /// Pins the protocol and checks that every server declaration is supported.
    /// This does not enable native response replay in a host: the ingress
    /// parser and durable conversation owner must retain server items too.
    ///
    /// # Errors
    /// Chat Completions cannot represent this native search declaration;
    /// Repeated search declarations and Anthropic name collisions are refused.
    pub fn new(
        family: Family,
        declarations: Vec<ToolDeclaration<'a, P>>,
    ) -> Result<Self, RenderError> {
        let mut search = false;
        let mut search_function = false;
        for declaration in &declarations {
            if let ToolDeclaration::Function { name, .. } = declaration {
                search_function |= name == "web_search";
            }
            if matches!(declaration, ToolDeclaration::WebSearch) {
                if family == Family::Chat || search {
                    return Err(RenderError::InvalidItem(
                        "unsupported or repeated native web search declaration",
                    ));
                }
                search = true;
            }
        }
        if family == Family::Anthropic && search && search_function {
            return Err(RenderError::InvalidItem(
                "native web search collides with a client function name",
            ));
        }
        Ok(Self {
            family,
            declarations,
        })
    }

    /// Whether an ordinary request can omit its tool field.
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty()
    }
}

impl<P: Serialize> Serialize for ToolDeclarations<'_, P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        let mut remaining = self.declarations.as_slice();
        while let Some((first, rest)) = remaining.split_first() {
            if self.family == Family::Gemini && matches!(first, ToolDeclaration::Function { .. }) {
                // Group only consecutive functions. Grouping across a server
                // declaration would change the frozen profile's tool order.
                let count = remaining
                    .iter()
                    .take_while(|tool| matches!(tool, ToolDeclaration::Function { .. }))
                    .count();
                #[derive(Serialize)]
                struct Functions<'a, 'b, P> {
                    #[serde(rename = "functionDeclarations")]
                    declarations: FunctionList<'a, 'b, P>,
                }
                sequence.serialize_element(&Functions {
                    declarations: FunctionList(&remaining[..count]),
                })?;
                remaining = &remaining[count..];
            } else {
                sequence.serialize_element(&Declaration {
                    family: self.family,
                    tool: first,
                })?;
                remaining = rest;
            }
        }
        sequence.end()
    }
}

struct FunctionList<'a, 'b, P>(&'a [ToolDeclaration<'b, P>]);
impl<P: Serialize> Serialize for FunctionList<'_, '_, P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for tool in self.0 {
            sequence.serialize_element(&Declaration {
                family: Family::Gemini,
                tool,
            })?;
        }
        sequence.end()
    }
}

struct Declaration<'a, 'b, P> {
    family: Family,
    tool: &'a ToolDeclaration<'b, P>,
}
impl<P: Serialize> Serialize for Declaration<'_, '_, P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        match self.tool {
            ToolDeclaration::WebSearch => match self.family {
                Family::Responses => map.serialize_entry("type", "web_search")?,
                Family::Anthropic => {
                    map.serialize_entry("type", "web_search_20250305")?;
                    map.serialize_entry("name", "web_search")?;
                }
                Family::Gemini => {
                    #[derive(Serialize)]
                    struct Empty {}
                    map.serialize_entry("googleSearch", &Empty {})?;
                }
                Family::Chat => {
                    return Err(serde::ser::Error::custom(
                        "Chat Completions has no native web search declaration",
                    ));
                }
            },
            ToolDeclaration::Function {
                name,
                description,
                parameters,
            } => {
                if self.family == Family::Chat {
                    #[derive(Serialize)]
                    struct Function<'a, P> {
                        name: &'a str,
                        description: &'a str,
                        parameters: &'a P,
                    }
                    map.serialize_entry("type", "function")?;
                    map.serialize_entry(
                        "function",
                        &Function {
                            name: name.as_ref(),
                            description,
                            parameters,
                        },
                    )?;
                } else {
                    if self.family == Family::Responses {
                        map.serialize_entry("type", "function")?;
                        map.serialize_entry("strict", &false)?;
                    }
                    map.serialize_entry("name", name)?;
                    map.serialize_entry("description", description)?;
                    let schema = match self.family {
                        Family::Anthropic => "input_schema",
                        Family::Gemini => "parametersJsonSchema",
                        Family::Responses | Family::Chat => "parameters",
                    };
                    map.serialize_entry(schema, parameters)?;
                }
            }
        }
        map.end()
    }
}
