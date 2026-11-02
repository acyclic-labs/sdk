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
    /// Exact provider settings data, without schema rewriting or name gates.
    Provider {
        /// Canonical declaration object retained by settings admission.
        definition: &'a serde_json::Map<String, serde_json::Value>,
        /// Whether the provider can execute this declaration itself. The
        /// protocol owner classifies the admitted definition, independently of
        /// client tool names and executable capability grants.
        server_tool: bool,
    },
}

/// A declaration list admitted once and serialized in its original order.
/// Creating it transfers the vector; serialization allocates no staging tree.
pub struct ToolDeclarations<'a, P> {
    family: Family,
    declarations: Vec<ToolDeclaration<'a, P>>,
    server_tools: bool,
}

impl<'a, P> ToolDeclarations<'a, P> {
    /// Pins the protocol and retains declarations in their original order.
    /// Provider declarations are data; their validation belongs to the selected
    /// provider. Rendering grants no native response or execution authority.
    ///
    /// # Errors
    /// Freeform client tools require the Responses protocol.
    pub fn new(
        family: Family,
        declarations: Vec<ToolDeclaration<'a, P>>,
    ) -> Result<Self, RenderError> {
        let mut server_tools = false;
        for declaration in &declarations {
            if matches!(declaration, ToolDeclaration::Freeform { .. })
                && family != Family::Responses
            {
                return Err(RenderError::InvalidItem("freeform tools require Responses"));
            }
            server_tools |= matches!(
                declaration,
                ToolDeclaration::Provider {
                    server_tool: true,
                    ..
                }
            );
        }
        Ok(Self {
            family,
            declarations,
            server_tools,
        })
    }

    /// Whether an ordinary request can omit its tool field.
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty()
    }

    pub(crate) const fn family(&self) -> Family {
        self.family
    }

    /// GenerateContent's NONE controls function declarations only; it cannot
    /// establish that a built-in server tool will not execute.
    pub(crate) const fn has_server_tools(&self) -> bool {
        self.server_tools
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
        match self.tool {
            ToolDeclaration::Provider { definition, .. } => definition.serialize(serializer),
            ToolDeclaration::Freeform {
                name,
                description,
                format,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "custom")?;
                map.serialize_entry("name", name)?;
                map.serialize_entry("description", description)?;
                map.serialize_entry("format", format)?;
                map.end()
            }
            ToolDeclaration::Function {
                name,
                description,
                parameters,
            } => {
                let mut map = serializer.serialize_map(None)?;
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
                map.end()
            }
        }
    }
}
