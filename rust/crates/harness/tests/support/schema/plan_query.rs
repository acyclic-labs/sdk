use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlanQuery {
    writes: Vec<Revision>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Revision {
    path: String,
    text: String,
    files: BTreeMap<String, String>,
    panel: Vec<String>,
}

impl PlanQuery {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.writes.is_empty()
            || case
                .writes
                .iter()
                .any(|revision| revision.files.is_empty() || revision.panel.is_empty())
        {
            return Err("every plan revision requires file and panel expectations".into());
        }
        Ok(case)
    }
}
