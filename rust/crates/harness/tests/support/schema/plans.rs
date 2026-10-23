use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plans {
    prompt: String,
    writes: Vec<Write>,
    expect: BTreeMap<String, Expected>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Write {
    path: String,
    text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Expected {
    markdown: String,
    tasks: Option<Vec<ExpectedTask>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ExpectedTask {
    state: String,
    text: String,
}

impl Plans {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        let written: BTreeSet<_> = case
            .writes
            .iter()
            .map(|write| write.path.as_str())
            .collect();
        let expected: BTreeSet<_> = case.expect.keys().map(String::as_str).collect();
        if written.is_empty()
            || written != expected
            || written.iter().any(|path| !path.starts_with("plans/"))
        {
            return Err("every written plans/ file requires an expectation".into());
        }
        Ok(case)
    }
}
