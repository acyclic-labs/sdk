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
    /// A Responses custom tool over the original canonical JSON executor.
    Freeform {
        /// Provider-valid name already chosen by the canonical name binder.
        name: Cow<'a, str>,
        /// Model-facing description.
        description: &'a str,
        /// Borrowed native text or grammar format.
        format: P,
    },
    /// Provider-owned web search, without a client function or executor.
    WebSearch,
    /// Responses shell actions executed by the host's original local runtime.
    /// This declares neither an OpenAI-hosted container nor a client function.
    Shell,
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
    /// Native shell is supported only by Responses. Chat Completions cannot
    /// represent native search. Repeated native declarations and conflicting
    /// client function names are refused before visiting any input schema.
    pub fn new(
        family: Family,
        declarations: Vec<ToolDeclaration<'a, P>>,
    ) -> Result<Self, RenderError> {
        let mut search = false;
        let mut search_function = false;
        let mut shell = false;
        let mut shell_function = false;
        for declaration in &declarations {
            if matches!(declaration, ToolDeclaration::Freeform { .. })
                && family != Family::Responses
            {
                return Err(RenderError::InvalidItem("freeform tools require Responses"));
            }
            if let ToolDeclaration::Function { name, .. } | ToolDeclaration::Freeform { name, .. } =
                declaration
            {
                search_function |= name == "web_search";
                shell_function |= name == "shell";
            }
            if matches!(declaration, ToolDeclaration::WebSearch) {
                if family == Family::Chat || search {
                    return Err(RenderError::InvalidItem(
                        "unsupported or repeated native web search declaration",
                    ));
                }
                search = true;
            }
            if matches!(declaration, ToolDeclaration::Shell) {
                if family != Family::Responses || shell {
                    return Err(RenderError::InvalidItem(
                        "unsupported or repeated native shell declaration",
                    ));
                }
                shell = true;
            }
        }
        if family == Family::Anthropic && search && search_function {
            return Err(RenderError::InvalidItem(
                "native web search collides with a client function name",
            ));
        }
        if shell && shell_function {
            return Err(RenderError::InvalidItem(
                "native shell collides with a client function name",
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
            ToolDeclaration::Shell => {
                #[derive(Serialize)]
                struct Local {
                    #[serde(rename = "type")]
                    kind: &'static str,
                }
                map.serialize_entry("type", "shell")?;
                map.serialize_entry("environment", &Local { kind: "local" })?;
            }
            ToolDeclaration::Freeform {
                name,
                description,
                format,
            } => {
                map.serialize_entry("type", "custom")?;
                map.serialize_entry("name", name)?;
                map.serialize_entry("description", description)?;
                map.serialize_entry("format", format)?;
            }
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
