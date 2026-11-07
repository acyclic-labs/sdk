//! Read-only native marketplace admission, before filesystem source resolution.
use super::Result;
use serde_json::Value;

pub(super) const RECOVERY: &str = "recovery=codex plugin marketplace list --json";

pub(super) struct Marketplaces(Value);

impl Marketplaces {
    pub(super) fn parse(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map(Self)
            .map_err(|error| format!("step=decode Codex marketplaces: {error}; {RECOVERY}").into())
    }

    pub(super) fn local_source(&self) -> Result<Option<(&str, &str)>> {
        let entries = self
            .0
            .get("marketplaces")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                format!("step=admit Codex marketplaces: marketplace list absent; {RECOVERY}")
            })?;
        let mut selected = None;
        for entry in entries {
            let name = entry.get("name").and_then(Value::as_str).ok_or_else(|| {
                format!("step=admit Codex marketplace: native marketplace name absent; {RECOVERY}")
            })?;
            if name == "acyclic" && selected.replace(entry).is_some() {
                return Err(format!(
                    "step=admit Codex marketplace: NATIVE_MARKETPLACE_AMBIGUOUS; {RECOVERY}"
                )
                .into());
            }
        }
        let Some(entry) = selected else {
            return Ok(None);
        };
        let source = entry.get("marketplaceSource");
        let root = entry.get("root").and_then(Value::as_str);
        let original = source
            .and_then(|source| source.get("source"))
            .and_then(Value::as_str);
        let local = source
            .and_then(|source| source.get("sourceType"))
            .and_then(Value::as_str)
            == Some("local");
        let Some((root, original)) = root.zip(original).filter(|_| local) else {
            return Err(format!(
                "step=admit Codex marketplace: NATIVE_MARKETPLACE_SOURCE_UNAVAILABLE; {RECOVERY}"
            )
            .into());
        };
        Ok(Some((root, original)))
    }
}
