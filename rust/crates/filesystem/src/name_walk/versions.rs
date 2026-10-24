//! Shared directory metadata tokens survive process restart without listing.
use crate::demand::{SourceNode, SourceNodeKind, SourceVersion};
use imbl::OrdMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub(super) struct DirectoryVersions(
    OrdMap<String, Observation>,
    OrdMap<[u8; 32], imbl::OrdSet<String>>,
);

/// Identity is independent of the version, so a journal can locate a former
/// directory binding after that directory is renamed or removed offline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    pub(super) version: Option<SourceVersion>,
    pub(super) directory_identity: Option<[u8; 32]>,
}

impl DirectoryVersions {
    pub(super) fn insert(&mut self, path: String, version: Option<SourceVersion>) {
        self.restore(
            path,
            Observation {
                version,
                directory_identity: None,
            },
        );
    }

    pub(super) fn observe(&mut self, path: String, node: Option<SourceNode>) {
        self.restore(
            path,
            Observation {
                version: node.map(|node| node.version),
                directory_identity: node
                    .filter(|node| node.kind == SourceNodeKind::Directory)
                    .map(|node| node.file_identity),
            },
        );
    }

    pub(super) fn restore(&mut self, path: String, observation: Observation) {
        let before = self.0.get(&path).and_then(|entry| entry.directory_identity);
        if before == observation.directory_identity {
            self.0.insert(path, observation);
            return;
        }
        if let Some(identity) = before {
            self.remove_identity(&path, identity);
        }
        if let Some(identity) = observation.directory_identity {
            let mut paths = self.1.get(&identity).cloned().unwrap_or_default();
            paths.insert(path.clone());
            self.1.insert(identity, paths);
        }
        self.0.insert(path, observation);
    }

    fn remove_identity(&mut self, path: &str, identity: [u8; 32]) {
        let mut paths = self.1.get(&identity).cloned().unwrap_or_default();
        paths.remove(path);
        if paths.is_empty() {
            self.1.remove(&identity);
        } else {
            self.1.insert(identity, paths);
        }
    }

    fn remove(&mut self, path: &str) {
        if let Some(observation) = self.0.remove(path)
            && let Some(identity) = observation.directory_identity
        {
            self.remove_identity(path, identity);
        }
    }

    pub(super) fn paths(&self, identity: &[u8; 32]) -> impl Iterator<Item = &str> {
        self.1
            .get(identity)
            .into_iter()
            .flat_map(|paths| paths.iter())
            .map(String::as_str)
    }

    pub(super) fn identities(&self) -> impl Iterator<Item = (&str, [u8; 32])> {
        self.0.iter().filter_map(|(path, observation)| {
            observation.directory_identity.map(|id| (path.as_str(), id))
        })
    }

    pub(super) fn contains_key(&self, path: &str) -> bool {
        self.0.contains_key(path)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, Option<SourceVersion>)> {
        self.0
            .iter()
            .map(|(path, observation)| (path.as_str(), observation.version))
    }

    pub(super) fn after(
        &self,
        after: Option<&str>,
    ) -> impl Iterator<Item = (&str, Option<SourceVersion>)> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.0
            .range((
                after.map_or(Unbounded, |path| Excluded(path.to_owned())),
                Unbounded,
            ))
            .map(|(path, observation)| (path.as_str(), observation.version))
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&str, &Observation)> {
        self.0
            .iter()
            .map(|(path, version)| (path.as_str(), version))
    }

    pub(super) fn changes<'a>(
        &'a self,
        previous: &'a Self,
    ) -> impl Iterator<Item = (&'a str, Option<&'a Observation>)> {
        previous.0.diff(&self.0).map(|change| match change {
            imbl::ordmap::DiffItem::Add(path, version) => (path.as_str(), Some(version)),
            imbl::ordmap::DiffItem::Update {
                new: (path, version),
                ..
            } => (path.as_str(), Some(version)),
            imbl::ordmap::DiffItem::Remove(path, _) => (path.as_str(), None),
        })
    }

    pub(super) fn forget_subtree(&mut self, path: &str) {
        self.remove(path);
        let prefix = format!("{path}/");
        let removed: Vec<_> = self
            .0
            .range(prefix.clone()..)
            .take_while(|(directory, _)| directory.starts_with(&prefix))
            .map(|(directory, _)| directory.clone())
            .collect();
        for directory in removed {
            self.remove(&directory);
        }
    }
}

impl Serialize for DirectoryVersions {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.entries())
    }
}

impl<'de> Deserialize<'de> for DirectoryVersions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DirectoryVersions;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("directory version tokens")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut versions = DirectoryVersions::default();
                while let Some((path, version)) = map.next_entry::<String, Observation>()? {
                    if versions.contains_key(&path) {
                        return Err(serde::de::Error::custom(
                            "duplicate directory version token",
                        ));
                    }
                    versions.restore(path, version);
                }
                Ok(versions)
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_journal_parent_lookup_preserves_aliases_and_full_identities() {
        let mut original = DirectoryVersions::default();
        for ordinal in 0u64..10_000 {
            let mut id = [0; 32];
            id[..8].copy_from_slice(&ordinal.to_le_bytes());
            original.restore(
                format!("d{ordinal:05}"),
                Observation {
                    version: Some(SourceVersion([1; 32])),
                    directory_identity: Some(id),
                },
            );
        }
        let id = [0; 32];
        original.restore(
            "d00000/alias".into(),
            Observation {
                version: Some(SourceVersion([2; 32])),
                directory_identity: Some(id),
            },
        );
        let mut different = id;
        different[31] = 1;
        original.restore(
            "d00000-other".into(),
            Observation {
                version: Some(SourceVersion([3; 32])),
                directory_identity: Some(different),
            },
        );
        assert_eq!(
            original.paths(&id).collect::<Vec<_>>(),
            ["d00000", "d00000/alias"]
        );
        assert_eq!(
            original.paths(&different).collect::<Vec<_>>(),
            ["d00000-other"]
        );
        // Negative control: projecting these identities to their first 64 bits
        // aliases two distinct directories and loses the deletion target.
        assert_eq!(&id[..8], &different[..8]);
        let bytes = serde_json::to_vec(&original).unwrap();
        let mut restored: DirectoryVersions = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            restored.paths(&id).collect::<Vec<_>>(),
            original.paths(&id).collect::<Vec<_>>()
        );
        restored.forget_subtree("d00000");
        assert_eq!(restored.paths(&id).count(), 0);
        assert_eq!(
            restored.paths(&different).collect::<Vec<_>>(),
            ["d00000-other"]
        );
        assert_eq!(
            original.paths(&id).count(),
            2,
            "older checkpoint retains its aliases"
        );
        restored.insert("d00000-other".into(), None);
        assert_eq!(
            restored.paths(&different).count(),
            0,
            "an absent binding retires its reverse identity"
        );
    }

    #[test]
    fn name_search_directory_token_snapshots_persist_only_changed_keys() {
        let mut original = DirectoryVersions::default();
        for ordinal in 0..10_000 {
            original.insert(format!("d{ordinal:05}"), Some(SourceVersion([1; 32])));
        }
        let mut changed = original.clone();
        assert_eq!(changed.changes(&original).count(), 0);
        changed.insert("d00001".into(), Some(SourceVersion([2; 32])));
        changed.insert("d00001/child".into(), None);
        changed.insert("d00001-other".into(), None);
        changed.forget_subtree("d00001");
        assert!(changed.contains_key("d00001-other"));
        assert!(!changed.contains_key("d00001/child"));
        assert!(original.contains_key("d00001"));
        assert_eq!(changed.changes(&original).count(), 2);
        // Negative control: a full snapshot visits all ten thousand keys.
        assert_eq!(changed.iter().count(), 10_000);
        // Resume by a tree range, without replaying the preceding keys.
        assert_eq!(changed.after(Some("d09997")).count(), 2);
        assert_eq!(changed.after(None).take(128).count(), 128);
        let bytes = serde_json::to_vec(&changed).unwrap();
        let restored: DirectoryVersions = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            restored.iter().collect::<Vec<_>>(),
            changed.iter().collect::<Vec<_>>()
        );
        assert!(
            serde_json::from_str::<DirectoryVersions>(
                r#"{"a":{"version":null,"directory_identity":null}}"#
            )
            .is_ok()
        );
        assert!(serde_json::from_str::<DirectoryVersions>(r#"{"a":{"version":null,"directory_identity":null},"a":{"version":null,"directory_identity":null}}"#).is_err());
    }
}
