use serde_json::Value;
use std::collections::BTreeMap;

/// File-owned model policies and the roles used when admitting agents.
#[derive(Debug, Default)]
pub struct ModelConfiguration {
    /// Normalized provider/model identity or an exact named alloy.
    pub selection: Option<String>,
    /// Provider-neutral policy documents, validated by the provider registry.
    pub alloys: BTreeMap<String, Value>,
    /// Root and child defaults; explicit spawn roles override the child default.
    pub roles: ModelRoles,
}

/// Role defaults supplied by the model settings file.
#[derive(Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelRoles {
    /// Role bound to root turns.
    pub root: String,
    /// Role bound to children without an explicit spawn role.
    pub child: String,
}

impl Default for ModelRoles {
    fn default() -> Self {
        Self {
            root: "root".into(),
            child: "child".into(),
        }
    }
}
