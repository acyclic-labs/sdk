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
/// Reserved SDK metadata excluded from compatibility commits.
const RESERVED_IGNORE_RULES: &str = ".acyclic-sdk/\n.git/\n";

/// Root namespaces the harness owns. They hold harness state such as
/// write-tool metadata and upload receipts, so they are outside every Git
/// view of the workspace rather than ignored content.
const HOST_NAMESPACES: [&str; 3] = [".acyclic-sdk", ".git", ".system"];

/// The Git view of a workspace whose root `.gitignore` holds `ignore_text`.
pub(crate) fn git_view_policy(ignore_text: &str) -> GitIgnorePolicy {
    GitIgnorePolicy::parse(&format!("{ignore_text}\n{RESERVED_IGNORE_RULES}"))
        .with_host_namespaces(HOST_NAMESPACES)
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
    Ok(git_view_policy(&String::from_utf8_lossy(&bytes)))
}
