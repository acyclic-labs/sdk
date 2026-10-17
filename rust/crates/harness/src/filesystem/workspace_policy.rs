use crate::{Error, Result};
use acyclic_fs::{
    DefaultTextMergeDriver, GitIgnorePolicy, LazyWorkspace, LocalAuthorityBackend,
    LocalCoreStateStore, LocalObjectBackend, MergeDriverRegistry,
    demand::native::NativeDemandSource,
};
use std::sync::Arc;
pub(super) type LocalLazyWorkspace = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
/// SDK-owned roots excluded from coding diffs, commits and native publication.
pub(crate) const RESERVED_DIRECTORIES: &[&str] = &[".git", ".acyclic-sdk", ".system", ".volumes"];

pub(crate) fn ignore_policy(contents: &str) -> GitIgnorePolicy {
    let mut rules = String::with_capacity(
        contents.len()
            + RESERVED_DIRECTORIES.iter().map(|name| name.len() + 2).sum::<usize>(),
    );
    rules.push_str(contents);
    for directory in RESERVED_DIRECTORIES {
        rules.push('\n');
        rules.push_str(directory);
        rules.push('/');
    }
    GitIgnorePolicy::parse(&rules)
}

/// Loads text merge drivers and workspace attributes.
pub(crate) async fn merge_drivers_for<'a>(
    workspace: impl Into<crate::tools::git::View<'a>>,
) -> Result<Arc<MergeDriverRegistry>> {
    let bytes = workspace
        .into()
        .optional("/.gitattributes", 1024 * 1024)
        .await?
        .unwrap_or_default();
    let attributes = std::str::from_utf8(&bytes)
        .map_err(|_| Error::Invalid(".gitattributes must be UTF-8".into()))?;
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .map_err(Error::from)?;
    registry.set_default("text").map_err(Error::from)?;
    if !attributes.trim().is_empty() {
        registry
            .set_git_attributes(attributes)
            .map_err(Error::from)?;
    }
    Ok(Arc::new(registry))
}

/// Loads ignores without reading or validating merge attributes.
pub(crate) async fn ignore_for<'a>(
    workspace: impl Into<crate::tools::git::View<'a>>,
) -> Result<GitIgnorePolicy> {
    let bytes = workspace
        .into()
        .optional("/.gitignore", 1024 * 1024)
        .await?
        .unwrap_or_default();
    let ignore_text = String::from_utf8_lossy(&bytes);
    Ok(ignore_policy(&ignore_text))
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Each reserved namespace stays excluded even when the project asks
        // to include it; a component sharing only its prefix stays ordinary.
        #[test]
        fn reserved_metadata_policy_is_shared_and_component_exact(name in "[a-z]{1,16}") {
            for root in RESERVED_DIRECTORIES {
                let policy = ignore_policy(&format!("!{root}/\n!{root}/**"));
                let path = format!("{root}/{name}");
                let ordinary = format!("{root}-project/{name}");
                prop_assert!(!policy.eligible(root, true, false));
                prop_assert!(!policy.eligible(&path, false, false));
                prop_assert!(policy.eligible(&ordinary, false, false));
                prop_assert!(policy.eligible(&path, false, true));
            }
        }
    }
}
