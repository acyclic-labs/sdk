//! Checkpoint snapshots share unchanged policy nodes and compiled rules. A
//! page publication must not clone every ignore file encountered by the walk.
use crate::GitIgnorePolicy;
use imbl::OrdMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct Policies(OrdMap<String, Arc<GitIgnorePolicy>>);

impl Policies {
    pub(super) fn contains_key(&self, path: &str) -> bool {
        self.0.contains_key(path)
    }

    pub(super) fn insert(&mut self, path: String, policy: GitIgnorePolicy) {
        self.0.insert(path, Arc::new(policy));
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, &GitIgnorePolicy)> {
        self.0
            .iter()
            .map(|(path, policy)| (path.as_str(), policy.as_ref()))
    }

    pub(super) fn changes<'a>(
        &'a self,
        previous: &'a Self,
    ) -> impl Iterator<Item = (&'a str, Option<&'a GitIgnorePolicy>)> {
        previous.0.diff(&self.0).map(|change| match change {
            imbl::ordmap::DiffItem::Add(path, policy) => (path.as_str(), Some(policy.as_ref())),
            imbl::ordmap::DiffItem::Update {
                new: (path, policy),
                ..
            } => (path.as_str(), Some(policy.as_ref())),
            imbl::ordmap::DiffItem::Remove(path, _) => (path.as_str(), None),
        })
    }

    pub(super) fn forget_subtree(&mut self, path: &str) {
        // Only visit the affected key range. Shared nodes outside this subtree
        // survive both in this snapshot and in older, generation-pinned ones.
        self.0.remove(path);
        let prefix = format!("{path}/");
        let removed: Vec<_> = self
            .0
            .range(prefix.clone()..)
            .take_while(|(directory, _)| directory.starts_with(&prefix))
            .map(|(directory, _)| directory.clone())
            .collect();
        for directory in removed {
            self.0.remove(&directory);
        }
    }

    pub(super) fn eligible(&self, root: &GitIgnorePolicy, path: &str, directory: bool) -> bool {
        root.eligible_with_lookup(|key| self.0.get(key).map(Arc::as_ref), path, directory)
    }
}

impl Serialize for Policies {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Policies {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Policies;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("directory ignore policies")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Policies, M::Error> {
                let mut policies = Policies::default();
                while let Some((path, policy)) = map.next_entry::<String, GitIgnorePolicy>()? {
                    if policies.contains_key(&path) {
                        return Err(serde::de::Error::custom(
                            "duplicate directory ignore policy",
                        ));
                    }
                    policies.insert(path, policy);
                }
                Ok(policies)
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_checkpoint_policy_snapshots_share_unchanged_rules() {
        let mut original = Policies::default();
        for ordinal in 0..10_000 {
            original.insert(
                format!("d{ordinal:05}"),
                GitIgnorePolicy::parse("*.ignored"),
            );
        }
        let mut snapshot = original.clone();
        // Negative control: cloning the former BTreeMap clones every policy
        // allocation. A checkpoint must retain these exact shared values.
        for (path, policy) in original.0.iter() {
            assert!(Arc::ptr_eq(policy, snapshot.0.get(path).unwrap()));
        }
        snapshot.insert(
            "d00001/child".into(),
            GitIgnorePolicy::parse("!keep.ignored"),
        );
        snapshot.insert("d00001-other".into(), GitIgnorePolicy::default());
        snapshot.forget_subtree("d00001");
        assert!(original.contains_key("d00001"));
        assert!(!snapshot.contains_key("d00001"));
        assert!(!snapshot.contains_key("d00001/child"));
        assert!(snapshot.contains_key("d00001-other"));
        assert!(Arc::ptr_eq(
            original.0.get("d00002").unwrap(),
            snapshot.0.get("d00002").unwrap()
        ));
        assert!(!snapshot.eligible(&GitIgnorePolicy::default(), "d00002/file.ignored", false));
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let restored: Policies = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&restored).unwrap());
        assert!(!restored.eligible(&GitIgnorePolicy::default(), "d00002/file.ignored", false));
        assert!(
            serde_json::from_str::<Policies>(r#"{"a":{"rules":[]},"a":{"rules":[]}}"#).is_err()
        );
    }
}
