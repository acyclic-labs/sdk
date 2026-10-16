use acyclic_harness::mcp::McpCatalog;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpScript {
    pub catalog: McpCatalog,
    pub replies: Vec<Reply>,
    pub grants: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub name: String,
    pub arguments: Value,
    pub result: Value,
}
