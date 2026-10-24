//! Provider-configured host tool contracts, bound before issuing call grants.
use crate::{
    Result,
    tool::{ToolDefinition, schema},
};

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
enum WebOutput<T> {
    Succeeded(T),
    Rejected(WebRejection),
}

#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum WebRejection {
    Rejected {
        code: WebRejectionCode,
        #[schemars(length(min = 1))]
        reason: String,
    },
}

/// Only model-correctable failures belong in the published tool contract.
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WebRejectionCode {
    Invalid,
    Unavailable,
}

/// Describes exactly the capabilities available from this host configuration.
/// This does not construct or advertise an executor in a portable stock registry.
#[doc(hidden)]
pub fn definitions(search: bool) -> Result<Vec<ToolDefinition>> {
    let mut definitions = Vec::with_capacity(if search { 2 } else { 1 });
    if search {
        definitions.push(definition::<crate::SearchRequest, crate::SearchResponse>(
            "web_search",
            "Search the configured web provider for ranked sources and exact provider excerpts. Query text is sent to that provider; results retain source URLs for citations. An expected input/provider rejection returns status rejected with a typed code and safe reason so you can adapt",
        )?);
    }
    definitions.push(definition::<crate::url_fetch::FetchRequest, crate::url_fetch::FetchResponse>(
        "url_fetch",
        "Retrieve exact UTF-8 HTTP(S) response content from a URL, with original/final source URLs. This is direct HTTP retrieval, not a search or rendered browser page. An expected URL/provider rejection returns status rejected with a typed code and safe reason so you can adapt",
    )?);
    Ok(definitions)
}

fn definition<I: schemars::JsonSchema, O: schemars::JsonSchema>(
    name: &'static str,
    description: &'static str,
) -> Result<ToolDefinition> {
    Ok(ToolDefinition {
        name: name.into(),
        revision: "1".into(),
        description: description.into(),
        input_schema: schema::input::<I>()?,
        output_schema: schema::output::<WebOutput<O>>()?,
        projection_schema: schema::ProjectionMode::Full.schema::<WebOutput<O>>()?,
    })
}

mod executor;
pub use executor::WebExecutor;
