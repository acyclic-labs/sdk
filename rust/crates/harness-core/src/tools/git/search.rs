//! Paged grep of the selected logical view; no whole-tree exactification.
use super::*;
pub use acyclic_fs::SearchName;
use acyclic_fs::{GitGrepMatch, SearchError};

/// Bounded partial content-search results, also used for the final response.
#[derive(Clone, serde::Serialize)]
pub struct GrepProgress {
    /// Matching lines found so far, bounded by the search result budget.
    pub matches: Vec<GitGrepMatch>,
    /// A byte/result budget or a changed namespace prevented a complete answer.
    pub truncated: bool,
    /// Authorized regular candidates examined so far.
    pub files_examined: u64,
    /// Candidate content bytes read so far.
    pub bytes_read: u64,
    /// Metadata directories still awaiting enumeration.
    pub pending_directories: usize,
    /// True only when every eligible candidate was examined without truncation.
    pub complete: bool,
}

/// Publishes progress through the caller's existing followed tool-output files.
pub trait SearchProgress: Send + Sync {
    /// Commits a bounded partial result before search continues.
    fn publish<'a>(
        &'a self,
        progress: &'a GrepProgress,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<()>>;
}

impl GrepProgress {
    fn finish(mut self) -> Self {
        self.matches
            .sort_by(|left, right| (&left.path, left.line).cmp(&(&right.path, right.line)));
        self
    }
}
/// Metadata cursor over the local selected view.
pub type NameWalk<'a> = acyclic_fs::NameWalk<
    'a,
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
const FILE_BYTES: u64 = 1024 * 1024;
const TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MATCHES: usize = 1024;
/// Loads only the search ignore policy.
pub async fn search_ignore(view: View<'_>) -> crate::Result<GitIgnorePolicy> {
    // Policy files share the candidate reader's no-follow check. The generic
    // lazy reader also supports link payloads; a link target must never become
    // an ignore pattern or cause a policy file outside this volume to be read.
    let reader = walk_names(view, None, GitIgnorePolicy::default()).await?;
    let mut policy = GitIgnorePolicy::default();
    for path in GitIgnorePolicy::SEARCH_ROOT_FILES {
        match reader.read_policy(path, 1024 * 1024).await {
            Ok(bytes) => policy.extend(GitIgnorePolicy::parse(&String::from_utf8_lossy(&bytes))),
            Err(error) if error.is_missing_policy() => {}
            Err(error) => return Err(search_error(error)),
        }
    }
    policy.extend(crate::filesystem::workspace_policy::git_view_policy(""));
    Ok(policy)
}
/// Starts metadata-only enumeration of a selected view.
pub async fn walk_names<'a>(
    view: View<'a>,
    root: Option<&str>,
    ignore: GitIgnorePolicy,
) -> crate::Result<NameWalk<'a>> {
    match view {
        View::Checkout(lazy) => NameWalk::lazy(lazy, root, ignore).await,
        View::Volume(volume) => {
            NameWalk::exact(
                volume
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
                root,
                ignore,
            )
            .await
        }
    }
    .map_err(search_error)
}
pub(super) fn search_error(error: SearchError) -> Error {
    match error {
        SearchError::Json(error) => json_error(error),
        SearchError::Filesystem(error) => Error::from(WorkspaceError::from(error)),
        SearchError::Workspace(error) => error.into(),
        SearchError::Lazy(error) => error.into(),
        SearchError::Invalid(message) => Error::Invalid(message),
        SearchError::NotFound(path) => Error::NotFound(path),
        SearchError::PolicyUnavailable(path) => {
            Error::Storage(format!("search policy is not locally available: {path}"))
        }
    }
}
impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn grep(
        &self,
        tree: GitTreeRef,
        pattern: &str,
        path: Option<&str>,
    ) -> crate::Result<GrepProgress> {
        let reader = match tree {
            GitTreeRef::Lazy(snapshot) => {
                let View::Checkout(lazy) = self.lazy_current else {
                    return Err(Error::Unauthorized(
                        "lazy search requires the selected source".into(),
                    ));
                };
                NameWalk::at_snapshot(lazy, snapshot, path, self.ignore.clone()).await
            }
            GitTreeRef::Exact(reference) => {
                NameWalk::exact(
                    self.generation(GitTreeRef::Exact(reference)).await?,
                    path,
                    self.ignore.clone(),
                )
                .await
            }
        }
        .map_err(search_error)?;
        let mut names = IndexedNames::open(self.lazy_current).await?;
        let root = path.unwrap_or("").trim_matches('/');
        let mut result = GrepProgress {
            matches: Vec::new(),
            truncated: false,
            files_examined: 0,
            bytes_read: 0,
            pending_directories: names.pending_directories().await,
            complete: false,
        };
        self.report_search(&result).await?;
        let mut remaining = TOTAL_BYTES;
        while let Some(page) = names.next_page().await? {
            result.pending_directories = names.pending_directories().await;
            self.validate().await?;
            if !names.matches_tree(tree).await? {
                // A concurrent public namespace edit cannot be folded into
                // this pinned read. Preserve already found matches and report
                // a partial result instead of mixing generations.
                result.truncated = true;
                return self.finish_search(result).await;
            }
            for name in page.names.into_iter().filter(|name| name.regular) {
                if !root.is_empty()
                    && name.path != root
                    && !name
                        .path
                        .strip_prefix(root)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    continue;
                }
                if !self.ignore.eligible(&name.path, false, false)
                    || !self.repository.eligible(&name.path, false, false)
                    || !(self.read_allowed)(&name.path)
                {
                    continue;
                }
                if remaining == 0 {
                    result.truncated = true;
                    return self.finish_search(result).await;
                }
                let read = reader
                    .read(&name.path, FILE_BYTES.min(remaining))
                    .await
                    .map_err(search_error);
                result.files_examined += 1;
                let bytes = match read {
                    Ok(bytes) => bytes,
                    Err(Error::NativeSource { source, .. })
                        if matches!(
                            source.semantic_error(),
                            acyclic_fs::LazyWorkspaceError::StaleSource
                                | acyclic_fs::LazyWorkspaceError::StaleIdentity
                                | acyclic_fs::LazyWorkspaceError::Demand(
                                    acyclic_fs::demand::DemandError::StaleSource
                                        | acyclic_fs::demand::DemandError::StaleVersion
                                )
                        ) =>
                    {
                        result.truncated = true;
                        return self.finish_search(result).await;
                    }
                    Err(Error::NativeSource { source, .. })
                        if matches!(
                            source.semantic_error(),
                            acyclic_fs::LazyWorkspaceError::TooLarge
                                | acyclic_fs::LazyWorkspaceError::NotRegularFile
                                | acyclic_fs::LazyWorkspaceError::NotFound
                        ) =>
                    {
                        result.truncated = true;
                        continue;
                    }
                    Err(Error::NativeSource { source, .. })
                        if matches!(source.semantic_error(),
                            acyclic_fs::LazyWorkspaceError::Demand(acyclic_fs::demand::DemandError::Io(error))
                            if matches!(error.kind(), std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound)) =>
                    {
                        result.truncated = true;
                        continue;
                    }
                    Err(Error::NativeWorkspace { source, .. })
                        if matches!(
                            *source,
                            WorkspaceError::ReadLimitExceeded
                                | WorkspaceError::NotRegularFile
                                | WorkspaceError::NotFound
                        ) =>
                    {
                        result.truncated = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                remaining = remaining.saturating_sub(bytes.len() as u64);
                result.bytes_read += bytes.len() as u64;
                // Match the original bytes, including text files with invalid
                // UTF-8. Lossy decoding is only for the displayed matching line.
                for (line, raw) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
                    let text = raw
                        .strip_suffix(b"\n")
                        .map_or(raw, |line| line.strip_suffix(b"\r").unwrap_or(line));
                    if pattern.is_empty()
                        || text
                            .windows(pattern.len())
                            .any(|part| part == pattern.as_bytes())
                    {
                        if result.matches.len() == MATCHES {
                            result.truncated = true;
                            return self.finish_search(result).await;
                        }
                        result.matches.push(GitGrepMatch {
                            path: name.path.clone(),
                            line: line as u64 + 1,
                            text: String::from_utf8_lossy(text).into_owned(),
                        });
                    }
                }
            }
            self.report_search(&result).await?;
        }
        result.pending_directories = 0;
        result.truncated |= names.unavailable_entries().await != 0;
        result.complete = !result.truncated;
        self.finish_search(result).await
    }

    async fn report_search(&self, progress: &GrepProgress) -> crate::Result<()> {
        if let Some(output) = self.search_progress {
            self.validate().await?;
            output.publish(progress).await?;
        }
        Ok(())
    }

    async fn finish_search(&self, result: GrepProgress) -> crate::Result<GrepProgress> {
        self.report_search(&result).await?;
        Ok(result.finish())
    }
}
