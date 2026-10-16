use crate::{Error, Result};
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    CancellationToken, DefaultTextMergeDriver, GitIgnorePolicy, LazyWorkspace,
    LocalAuthorityBackend, LocalCoreStateStore, LocalObjectBackend, MergeDriverRegistry,
    WorkBudget, demand::native::NativeDemandSource,
};
use std::sync::Arc;
pub(super) type LocalLazyWorkspace = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
/// Reserved SDK metadata excluded from compatibility commits.
pub(crate) const RESERVED_IGNORE_RULES: &str = ".acyclic-sdk/\n.git/\n";

/// Loads text merge drivers and workspace attributes.
pub(crate) async fn merge_drivers_for(
    workspace: &LocalLazyWorkspace,
) -> Result<Arc<MergeDriverRegistry>> {
    let attributes = match workspace
        .read(
            "/.gitattributes",
            1024 * 1024,
            &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                allocations: Default::default(),
                work: acyclic_fs::WorkCounters::UNCHARGED,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &CancellationToken::new(),
            },
        )
        .await
    {
        Ok(chunk) => String::from_utf8(chunk.bytes.to_vec())
            .map_err(|_| Error::Invalid(".gitattributes must be UTF-8".to_owned()))?,
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(Error::from(error)),
    };
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .map_err(Error::from)?;
    registry.set_default("text").map_err(Error::from)?;
    if !attributes.trim().is_empty() {
        registry
            .set_git_attributes(&attributes)
            .map_err(Error::from)?;
    }
    Ok(Arc::new(registry))
}

/// Loads ignores without reading or validating merge attributes.
pub(crate) async fn ignore_for(workspace: &LocalLazyWorkspace) -> Result<GitIgnorePolicy> {
    let ignore_text = match workspace
        .read(
            "/.gitignore",
            1024 * 1024,
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
        )
        .await
    {
        Ok(chunk) => String::from_utf8_lossy(&chunk.bytes).into_owned(),
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(error.into()),
    };
    Ok(GitIgnorePolicy::parse(&format!(
        "{ignore_text}\n{RESERVED_IGNORE_RULES}"
    )))
}
