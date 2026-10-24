use super::super::{Error, Result};
use serde::{Deserialize, Serialize};

/// Canonical command, skill, config or workspace input source path.
#[derive(
    Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize, crate::HeapLayout,
)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct InputPath(pub(super) String);

impl InputPath {
    pub(super) fn is_source_root(root: &str) -> bool {
        matches!(
            root,
            "commands" | "workspace" | "config" | ".claude" | ".codex" | "skills" | "search"
        )
    }
    /// Canonical input source spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InputPath {
    type Error = Error;
    fn try_from(path: String) -> Result<Self> {
        let mut parts = path.split('/');
        if path.len() > 4096
            || !parts.next().is_some_and(Self::is_source_root)
            || !path.contains('/')
            || parts.any(|p| {
                p.is_empty()
                    || p == "."
                    || p == ".."
                    || p.chars().any(|c| c.is_control() || matches!(c, '\\' | ':'))
            })
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self(path))
    }
}
impl From<InputPath> for String {
    fn from(path: InputPath) -> Self {
        path.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_volume::{FileKey, Query};

    #[test]
    fn volume_search_publication_keys_round_trip_through_the_file_schema() {
        for path in [
            "search/query.json",
            "search/results.json",
            "search/progress.json",
        ] {
            let source = InputPath::try_from(path.to_owned()).unwrap();
            let key = FileKey::parse(path).unwrap();
            assert_eq!(key, FileKey::Input(source));
            assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
            assert_eq!(
                serde_json::from_str::<FileKey>(&serde_json::to_string(&key).unwrap()).unwrap(),
                key
            );
            assert_eq!(
                Query::Completions.matches(&key),
                path != "search/query.json"
            );
        }
        for path in [
            "search/../results.json",
            "search//query.json",
            "search\\results.json",
            "search/./query.json",
        ] {
            assert!(FileKey::parse(path).is_err());
        }
    }
}
