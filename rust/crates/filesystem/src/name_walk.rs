//! Paged metadata-only traversal shared by pathname indexing and content grep.
use crate::demand::{DemandSource, SourceNodeKind};
use crate::facade::FsWork as _;
use crate::kernel::FileKind;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, GitIgnorePolicy,
    LazyDirectoryCursor, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore, WorkBudget,
};
mod policies;
use policies::Policies;
mod versions;
use versions::{DirectoryVersions, Observation};
#[cfg(not(target_arch = "wasm32"))]
mod journal;
#[cfg(not(target_arch = "wasm32"))]
pub use journal::NameJournalCheckpoint;
#[cfg(not(target_arch = "wasm32"))]
mod frontier;
#[cfg(not(target_arch = "wasm32"))]
use frontier::{Frontier, SavedFrontier};
#[cfg(target_arch = "wasm32")]
type Frontier = Vec<(String, Continuation)>;
const PAGE_ENTRIES: u32 = 128;
pub(crate) const NAME_PAGE_BYTES: u64 = 2 * 1024 * 1024;

fn page_entries(directory: &str, maximum_component_bytes: u32) -> u32 {
    // Windows UTF-16 components can expand in UTF-8; JSON can expand each
    // control byte to six bytes. Bound a page before asking the source for it.
    let entry = directory
        .len()
        .saturating_add((maximum_component_bytes as usize).saturating_mul(3))
        .saturating_add(1)
        .saturating_mul(6)
        .saturating_add(80);
    ((NAME_PAGE_BYTES as usize - 2) / entry).clamp(1, PAGE_ENTRIES as usize) as u32
}
/// Failure while traversing a pinned logical namespace.
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// Invalid durable index bytes.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Namespace encoding or bounded kernel operation failed.
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    /// Authored filesystem failure.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Demand-backed filesystem failure.
    #[error(transparent)]
    Lazy(#[from] crate::LazyWorkspaceError),
    /// Invalid search root or continuation.
    #[error("{0}")]
    Invalid(String),
    /// Missing explicit root.
    #[error("search root missing: {0}")]
    NotFound(String),
    /// An ignore policy would require fetching placeholder content.
    #[error("search policy is not locally available: {0}")]
    PolicyUnavailable(String),
}

impl SearchError {
    /// An absent policy includes a non-directory ancestor (for example a
    /// worktree's `.git` file). Access failures and unavailable content remain
    /// errors, so they cannot silently broaden the eligible namespace.
    pub fn is_missing_policy(&self) -> bool {
        match self {
            Self::Lazy(error) => missing_policy_parent(error),
            Self::Workspace(
                crate::WorkspaceError::NotFound
                | crate::WorkspaceError::NotDirectory
                | crate::WorkspaceError::NotRegularFile,
            ) => true,
            _ => false,
        }
    }
}

fn missing_policy_parent(error: &crate::LazyWorkspaceError) -> bool {
    use crate::{LazyWorkspaceError as Lazy, WorkspaceError as Workspace};
    match error.semantic_error() {
        Lazy::NotFound
        | Lazy::NotDirectory
        | Lazy::NotRegularFile
        | Lazy::WorkspaceCause(
            Workspace::NotFound | Workspace::NotDirectory | Workspace::NotRegularFile,
        ) => true,
        Lazy::Demand(crate::demand::DemandError::Io(error)) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
        ),
        _ => false,
    }
}
enum Cut<'a, A, O, D, S> {
    Lazy(&'a LazyWorkspace<A, O, D, S>, LazySnapshotRef),
    Exact(Generation<A, O>),
}

/// Immutable identity of the names represented by one index version.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NameCut {
    /// Authored generation plus pinned source, overlay and shadow bindings.
    Lazy(LazySnapshotRef),
    /// Exact SDK generation.
    Exact {
        /// Owning branch in the volume.
        workspace: crate::WorkspaceId,
        /// Immutable namespace generation.
        generation: crate::GenerationId,
    },
}

/// Bounded unfinished traversal, stored alongside index pages.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "StoredNameCheckpoint")]
pub struct NameCheckpoint {
    cut: NameCut,
    pending: SavedFrontier,
    single: Option<SearchName>,
    ignore_policies: Policies,
    directory_versions: DirectoryVersions,
    journal: Option<NameJournalCheckpoint>,
    unavailable_entries: usize,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredNameCheckpoint {
    cut: NameCut,
    pending: SavedFrontier,
    single: Option<SearchName>,
    ignore_policies: Policies,
    directory_versions: DirectoryVersions,
    journal: Option<NameJournalCheckpoint>,
    unavailable_entries: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<StoredNameCheckpoint> for NameCheckpoint {
    type Error = SearchError;
    fn try_from(value: StoredNameCheckpoint) -> Result<Self, Self::Error> {
        if let Some(journal) = value.journal {
            journal.validate_cut(&value.cut)?;
        }
        Ok(Self {
            cut: value.cut,
            pending: value.pending,
            single: value.single,
            ignore_policies: value.ignore_policies,
            directory_versions: value.directory_versions,
            journal: value.journal,
            unavailable_entries: value.unavailable_entries,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NameCheckpoint {
    /// The journal position committed with these names and continuations.
    pub fn journal_checkpoint(&self) -> Option<NameJournalCheckpoint> {
        self.journal
    }

    /// Attaches accepted journal progress to the next atomic publication.
    /// A new baseline must create a fresh checkpoint after root/journal changes.
    ///
    /// # Errors
    /// Rejects another source, a changed root/journal, or a regressing cursor.
    pub fn record_journal_checkpoint(
        &mut self,
        journal: NameJournalCheckpoint,
    ) -> Result<(), SearchError> {
        journal.validate_cut(&self.cut)?;
        if self.journal.is_some_and(|old| !journal.follows(old)) {
            return Err(SearchError::Invalid(
                "journal checkpoint requires a new baseline".into(),
            ));
        }
        self.journal = Some(journal);
        Ok(())
    }

    /// Native directories and ignore-policy metadata captured during discovery.
    /// Absence is also a token: later creation invalidates this cut.
    pub fn source_versions(
        &self,
    ) -> impl Iterator<Item = (&str, Option<crate::demand::SourceVersion>)> {
        self.directory_versions.iter()
    }

    /// Resumes metadata-token observation without revisiting preceding keys.
    /// Callers can bound each batch independently of the volume's directory count.
    pub fn source_versions_after(
        &self,
        after: Option<&str>,
    ) -> impl Iterator<Item = (&str, Option<crate::demand::SourceVersion>)> {
        self.directory_versions.after(after)
    }

    /// Stable native directory identities captured at this checkpoint. These
    /// survive renamed/deleted bindings and never include policy file IDs.
    pub fn source_directory_identities(&self) -> impl Iterator<Item = (&str, [u8; 32])> {
        self.directory_versions.identities()
    }

    /// Resolves a source directory identity through the shared checkpoint's
    /// reverse index, including aliases, without scanning volume paths.
    pub fn source_directory_paths(&self, identity: &[u8; 32]) -> impl Iterator<Item = &str> {
        self.directory_versions.paths(identity)
    }

    pub(crate) fn directory_records_since<'a>(
        &'a self,
        previous: Option<&'a Self>,
    ) -> impl Iterator<Item = Result<(&'a str, Option<Vec<u8>>), serde_json::Error>> + 'a {
        let changes: Box<dyn Iterator<Item = (&str, Option<&Observation>)> + Send + 'a> =
            match previous {
                Some(previous) => Box::new(
                    self.directory_versions
                        .changes(&previous.directory_versions),
                ),
                None => Box::new(
                    self.directory_versions
                        .entries()
                        .map(|(path, version)| (path, Some(version))),
                ),
            };
        changes.map(|(path, version)| {
            Ok((
                path,
                version
                    .map(|version| {
                        serde_json::to_vec(&CheckpointRecordRef::Directory(path, *version))
                    })
                    .transpose()?,
            ))
        })
    }

    pub(crate) fn restore_directory_record(
        &mut self,
        record: &[u8],
        hash: &str,
    ) -> Result<(), SearchError> {
        let CheckpointRecord::Directory(path, version) = serde_json::from_slice(record)? else {
            return Err(SearchError::Invalid(
                "expected directory version record".into(),
            ));
        };
        if blake3::hash(path.as_bytes()).to_hex().as_str() != hash
            || self.directory_versions.contains_key(&path)
        {
            return Err(SearchError::Invalid(
                "directory version identity mismatch".into(),
            ));
        }
        self.directory_versions.restore(path, version);
        Ok(())
    }
    pub(crate) fn records(&self) -> impl Iterator<Item = Result<Vec<u8>, serde_json::Error>> + '_ {
        std::iter::once(CheckpointRecordRef::Header {
            cut: &self.cut,
            single: &self.single,
            unavailable_entries: self.unavailable_entries,
            journal: self.journal,
        })
        .map(|record| serde_json::to_vec(&record))
    }

    pub(crate) fn frontier_blocks(&self) -> usize {
        self.pending.block_count()
    }

    pub(crate) fn frontier_records_since<'a>(
        &'a self,
        previous: Option<&'a Self>,
    ) -> impl Iterator<
        Item = (
            usize,
            Option<impl Iterator<Item = Result<Vec<u8>, serde_json::Error>> + 'a>,
        ),
    > + 'a {
        self.pending
            .changes(previous.map(|old| &old.pending))
            .map(|(ordinal, block)| {
                (
                    ordinal,
                    block.map(|block| {
                        block.iter().map(|(path, cursor)| {
                            serde_json::to_vec(&CheckpointRecordRef::Pending(path, cursor))
                        })
                    }),
                )
            })
    }

    pub(crate) fn policy_records_since<'a>(
        &'a self,
        previous: Option<&'a Self>,
    ) -> impl Iterator<Item = Result<(&'a str, Option<Vec<u8>>), serde_json::Error>> + 'a {
        let changes: Box<dyn Iterator<Item = (&str, Option<&GitIgnorePolicy>)> + Send + 'a> =
            match previous {
                Some(previous) => Box::new(self.ignore_policies.changes(&previous.ignore_policies)),
                None => Box::new(
                    self.ignore_policies
                        .iter()
                        .map(|(path, policy)| (path, Some(policy))),
                ),
            };
        changes.map(|(path, policy)| {
            Ok((
                path,
                policy
                    .map(|policy| serde_json::to_vec(&CheckpointRecordRef::Policy(path, policy)))
                    .transpose()?,
            ))
        })
    }

    pub(crate) fn restore_frontier_record(&mut self, record: &[u8]) -> Result<(), SearchError> {
        let CheckpointRecord::Pending(path, cursor) = serde_json::from_slice(record)? else {
            return Err(SearchError::Invalid(
                "expected directory continuation".into(),
            ));
        };
        self.pending.push((path, cursor));
        Ok(())
    }

    pub(crate) fn restore_record(&mut self, record: &[u8]) -> Result<(), SearchError> {
        match serde_json::from_slice::<CheckpointRecord>(record)? {
            CheckpointRecord::Header { .. } => {
                return Err(SearchError::Invalid("duplicate checkpoint header".into()));
            }
            CheckpointRecord::Pending(path, cursor) => self.pending.push((path, cursor)),
            CheckpointRecord::Policy(path, policy) => {
                self.ignore_policies.insert(path, policy);
            }
            CheckpointRecord::Directory(path, version) => {
                self.directory_versions.restore(path, version);
            }
        }
        Ok(())
    }

    pub(crate) fn restore_policy_record(
        &mut self,
        record: &[u8],
        hash: &str,
    ) -> Result<(), SearchError> {
        let CheckpointRecord::Policy(path, policy) = serde_json::from_slice(record)? else {
            return Err(SearchError::Invalid(
                "expected checkpoint policy record".into(),
            ));
        };
        if blake3::hash(path.as_bytes()).to_hex().as_str() != hash
            || self.ignore_policies.contains_key(&path)
        {
            return Err(SearchError::Invalid(
                "checkpoint policy identity mismatch".into(),
            ));
        }
        self.ignore_policies.insert(path, policy);
        Ok(())
    }

    pub(crate) fn from_header(record: &[u8]) -> Result<Self, SearchError> {
        match serde_json::from_slice::<CheckpointRecord>(record)? {
            CheckpointRecord::Header {
                cut,
                single,
                unavailable_entries,
                journal,
            } => {
                if let Some(journal) = journal {
                    journal.validate_cut(&cut)?;
                }
                Ok(Self {
                    cut,
                    single,
                    unavailable_entries,
                    pending: SavedFrontier::default(),
                    ignore_policies: Policies::default(),
                    directory_versions: DirectoryVersions::default(),
                    journal,
                })
            }
            _ => Err(SearchError::Invalid("missing checkpoint header".into())),
        }
    }

    /// Entries or subtrees unavailable to this traversal. A terminal walk
    /// with a nonzero count cannot certify the absence of further matches.
    pub fn unavailable_entries(&self) -> usize {
        self.unavailable_entries
    }

    pub(crate) fn completed_at(&self, cut: NameCut) -> Option<Self> {
        if !self.pending.is_empty() || self.single.is_some() {
            return None;
        }
        if self
            .journal
            .is_some_and(|journal| journal.validate_cut(&cut).is_err())
        {
            return None;
        }
        let mut checkpoint = self.clone();
        checkpoint.cut = cut;
        Some(checkpoint)
    }

    pub(crate) fn eligible(&self, root: &GitIgnorePolicy, path: &str) -> bool {
        self.ignore_policies.eligible(root, path, false)
    }

    pub(crate) fn eligible_directory(&self, root: &GitIgnorePolicy, path: &str) -> bool {
        self.ignore_policies.eligible(root, path, true)
    }

    pub(crate) fn forget_subtree(&mut self, path: &str) {
        self.ignore_policies.forget_subtree(path);
        self.directory_versions.forget_subtree(path);
    }

    pub(crate) fn discover_directory(&mut self, path: &str, authored: bool) {
        let absolute = format!("/{path}");
        if matches!(self.cut, NameCut::Lazy(_)) {
            self.pending
                .push((absolute.clone(), SavedCursor::Lazy(None)));
        }
        if authored || matches!(self.cut, NameCut::Exact { .. }) {
            self.pending.push((absolute, SavedCursor::Exact(None)));
        }
    }

    pub(crate) fn pending_directories(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn record_unavailable(&mut self) {
        self.unavailable_entries += 1;
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Serialize)]
enum CheckpointRecordRef<'a> {
    Header {
        cut: &'a NameCut,
        single: &'a Option<SearchName>,
        unavailable_entries: usize,
        journal: Option<NameJournalCheckpoint>,
    },
    Pending(&'a str, &'a SavedCursor),
    Policy(&'a str, &'a GitIgnorePolicy),
    Directory(&'a str, Observation),
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Deserialize)]
enum CheckpointRecord {
    Header {
        cut: NameCut,
        single: Option<SearchName>,
        unavailable_entries: usize,
        journal: Option<NameJournalCheckpoint>,
    },
    Pending(String, SavedCursor),
    Policy(String, GitIgnorePolicy),
    Directory(String, Observation),
}

pub(crate) fn within_subtree(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|tail| tail.starts_with('/'))
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum SavedCursor {
    Lazy(Option<Vec<u8>>),
    Exact(Option<(u8, Vec<u8>)>),
}

/// A name and its no-follow classification. File contents are never consulted.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SearchName {
    /// Root-relative slash-separated spelling.
    pub path: String,
    /// Only regular files can be read by content search.
    pub regular: bool,
    /// Only directories are traversed; links are leaves.
    pub directory: bool,
}

impl SearchName {
    pub(crate) fn maximum_json_bytes(&self) -> usize {
        self.path.bytes().fold(80usize, |size, byte| {
            size.saturating_add(match byte {
                0..=31 => 6,
                b'"' | b'\\' => 2,
                _ => 1,
            })
        })
    }
}

enum Continuation {
    Lazy(Option<LazyDirectoryCursor>),
    Exact(Option<crate::kernel::LogicalName>),
}

/// Bounded pages over one immutable authored/source cut. Retains directory
/// continuations, never file contents or the complete set of regular names.
pub struct NameWalk<'a, A, O, D, S> {
    cut: Cut<'a, A, O, D, S>,
    pending: Frontier,
    single: Option<SearchName>,
    ignore: GitIgnorePolicy,
    // Existing authored metadata is cheap and available before native listing.
    authored: Option<Generation<A, O>>,
    ignore_policies: Policies,
    directory_versions: DirectoryVersions,
    #[cfg(not(target_arch = "wasm32"))]
    journal: Option<NameJournalCheckpoint>,
    unavailable_entries: usize,
    directory_query: Option<String>,
}

impl<
    'a,
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> NameWalk<'a, A, O, D, S>
{
    /// Exact generation and source binding of this traversal.
    pub fn cut(&self) -> NameCut {
        match &self.cut {
            Cut::Lazy(_, cut) => NameCut::Lazy(*cut),
            Cut::Exact(generation) => NameCut::Exact {
                workspace: generation.workspace_id(),
                generation: generation.id(),
            },
        }
    }

    /// Captures the bounded frontier without retaining emitted candidates.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn checkpoint(&self) -> Result<NameCheckpoint, SearchError> {
        let pending = self.pending.checkpoint()?;
        Ok(NameCheckpoint {
            cut: self.cut(),
            pending,
            single: self.single.clone(),
            ignore_policies: self.ignore_policies.clone(),
            directory_versions: self.directory_versions.clone(),
            journal: self.journal,
            unavailable_entries: self.unavailable_entries,
        })
    }

    /// Resumes at the original cut; the supplied view retains its source capability.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume(
        view: &'a LazyWorkspace<A, O, D, S>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::resume_with(view.workspace(), Some(view), checkpoint, ignore).await
    }

    /// Restores an exact volume's unfinished metadata traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume_exact(
        workspace: &crate::Workspace<A, O>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::resume_with(workspace, None, checkpoint, ignore).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn resume_with(
        workspace: &crate::Workspace<A, O>,
        view: Option<&'a LazyWorkspace<A, O, D, S>>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let cut = match checkpoint.cut {
            NameCut::Lazy(cut) if cut.workspace_id == workspace.id() => Cut::Lazy(
                view.ok_or_else(|| {
                    SearchError::Invalid("lazy index requires its source capability".into())
                })?,
                cut,
            ),
            NameCut::Exact {
                workspace: id,
                generation,
            } if id == workspace.id() => Cut::Exact(
                workspace
                    .generation(generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            _ => {
                return Err(SearchError::Invalid(
                    "checkpoint belongs to another workspace".into(),
                ));
            }
        };
        let pending: Vec<(String, Continuation)> = checkpoint
            .pending
            .iter()
            .map(|(path, cursor)| {
                let cursor = match cursor {
                    SavedCursor::Lazy(cursor) => Continuation::Lazy(
                        cursor
                            .as_ref()
                            .map(|bytes| {
                                LazyDirectoryCursor::restore_checkpoint(
                                    &bytes,
                                    workspace.profile(),
                                    workspace.limits(),
                                )
                            })
                            .transpose()?,
                    ),
                    SavedCursor::Exact(name) => Continuation::Exact(
                        name.as_ref()
                            .map(|(encoding, bytes)| {
                                crate::kernel::LogicalName::new(
                                    crate::kernel::NameEncoding::from_tag(*encoding)?,
                                    bytes.clone(),
                                    workspace.limits().maximum_component_bytes,
                                )
                            })
                            .transpose()
                            .map_err(|error| SearchError::Invalid(error.to_string()))?,
                    ),
                };
                Ok((path.clone(), cursor))
            })
            .collect::<Result<_, SearchError>>()?;
        let authored = match &cut {
            Cut::Lazy(lazy, snapshot) => Some(
                lazy.workspace()
                    .generation(
                        snapshot.authored_generation,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            Cut::Exact(_) => None,
        };
        Ok(Self {
            cut,
            pending: Frontier::restored(pending, checkpoint.pending),
            single: checkpoint.single,
            ignore,
            authored,
            ignore_policies: checkpoint.ignore_policies,
            directory_versions: checkpoint.directory_versions,
            journal: checkpoint.journal,
            unavailable_entries: checkpoint.unavailable_entries,
            directory_query: None,
        })
    }

    /// Pins a lazy metadata cut without reading regular-file contents.
    pub async fn lazy(
        view: &'a LazyWorkspace<A, O, D, S>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let snapshot = view
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await?;
        Self::at(Cut::Lazy(view, snapshot), root, ignore).await
    }
    /// Lists one immutable exact generation.
    pub async fn exact(
        view: Generation<A, O>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::at(Cut::Exact(view), root, ignore).await
    }
    /// Lists a previously pinned lazy cut.
    pub async fn at_snapshot(
        view: &'a LazyWorkspace<A, O, D, S>,
        snapshot: LazySnapshotRef,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::at(Cut::Lazy(view, snapshot), root, ignore).await
    }
    async fn at(
        cut: Cut<'a, A, O, D, S>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let path = format!("/{}", root.unwrap_or("").trim_matches('/'));
        // Validate before any source call; parent traversal is not a search root.
        let workspace = match &cut {
            Cut::Lazy(lazy, _) => lazy.workspace().clone(),
            Cut::Exact(generation) => generation.workspace(),
        };
        crate::kernel::NamespacePath::parse_in_profile(
            &path,
            workspace.profile(),
            workspace.limits(),
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            &CancellationToken::new(),
        )?;
        let (directory, regular) = match &cut {
            Cut::Lazy(lazy, snapshot) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (entry, _, guard) = lazy.lookup_at_snapshot(*snapshot, &path, &mut ctx).await?;
                let entry = entry.ok_or_else(|| SearchError::NotFound(path.clone()))?;
                let kind = entry.stat().kind;
                lazy.retire_lookup((entry, guard), &mut ctx)?;
                (
                    kind == SourceNodeKind::Directory,
                    kind == SourceNodeKind::RegularFile,
                )
            }
            Cut::Exact(generation) => {
                let kind = generation
                    .stat(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                    .kind;
                (kind == FileKind::Directory, kind == FileKind::Regular)
            }
        };
        let authored = match &cut {
            Cut::Lazy(lazy, snapshot) => Some(
                lazy.workspace()
                    .generation(
                        snapshot.authored_generation,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            Cut::Exact(_) => None,
        };
        let mut walk = Self {
            cut,
            pending: Frontier::default(),
            single: None,
            ignore,
            authored,
            ignore_policies: Policies::default(),
            directory_versions: DirectoryVersions::default(),
            #[cfg(not(target_arch = "wasm32"))]
            journal: None,
            unavailable_entries: 0,
            directory_query: None,
        };
        if directory
            && let Cut::Lazy(lazy, snapshot) = &walk.cut
            && !lazy
                .same_volume_at_snapshot(
                    *snapshot,
                    &path,
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await?
        {
            return Ok(walk);
        }
        // An explicit subtree still inherits all ancestor ignore files.
        let relative = path.trim_matches('/');
        for (end, _) in relative.match_indices('/') {
            walk.load_directory_ignore(&relative[..end]).await?;
        }
        if directory && !relative.is_empty() {
            walk.load_directory_ignore(relative).await?;
        }
        if directory {
            let cursor = walk.start();
            walk.pending.push((path.clone(), cursor));
            if let Some(authored) = &walk.authored {
                match authored
                    .stat(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                {
                    Ok(stat) if stat.value.kind == FileKind::Directory => {
                        walk.pending.push((path, Continuation::Exact(None)));
                    }
                    Ok(_) => {}
                    Err(failure) if matches!(failure.error, crate::WorkspaceError::NotFound) => {}
                    Err(failure) => return Err(failure.error.into()),
                }
            }
        } else {
            walk.single = Some(SearchName {
                path: path.trim_start_matches('/').into(),
                regular,
                directory: false,
            });
        }
        Ok(walk)
    }

    /// Unfinished directory count; zero is meaningful only after a terminal page.
    pub fn pending_directories(&self) -> usize {
        self.pending.len()
    }

    /// Unreadable or unrepresentable entries encountered so far.
    pub fn unavailable_entries(&self) -> usize {
        self.unavailable_entries
    }

    /// Updates disposable scheduling from the caller's followed query file.
    /// Only pending sibling roots are reordered; a parent continuation never
    /// jumps ahead of its children and expands the frontier to tree breadth.
    pub fn prefer_directory_names(&mut self, query: Option<&str>) {
        let query = query.filter(|query| !query.is_empty());
        if self.directory_query.as_deref() == query {
            return;
        }
        self.directory_query = query.map(str::to_owned);
        let Some((top, cursor)) = self.pending.last() else {
            return;
        };
        if !matches!(cursor, Continuation::Lazy(None) | Continuation::Exact(None)) {
            return;
        }
        let depth = top.bytes().filter(|byte| *byte == b'/').count();
        let mut start = self.pending.len();
        while start > 0 {
            let (path, cursor) = &self.pending[start - 1];
            if path.bytes().filter(|byte| *byte == b'/').count() != depth
                || !matches!(cursor, Continuation::Lazy(None) | Continuation::Exact(None))
            {
                break;
            }
            start -= 1;
        }
        if let Some(query) = &self.directory_query {
            #[cfg(not(target_arch = "wasm32"))]
            self.pending.sort_by_query(start, query);
            #[cfg(target_arch = "wasm32")]
            self.pending[start..].sort_by_cached_key(|(path, _)| directory_matches(path, query));
        }
    }

    fn start(&self) -> Continuation {
        match self.cut {
            Cut::Lazy(..) => Continuation::Lazy(None),
            Cut::Exact(_) => Continuation::Exact(None),
        }
    }

    /// Lists at most one source page. Empty pages can make progress; `None`
    /// alone signals completion. No regular-file read occurs on this path.
    pub async fn next_page(&mut self) -> Result<Option<Vec<SearchName>>, SearchError> {
        match self.next_directory_page().await {
            Err(SearchError::PolicyUnavailable(_)) => {
                self.unavailable_entries += 1;
                Ok(Some(Vec::new()))
            }
            Err(SearchError::Lazy(crate::LazyWorkspaceError::Demand(
                crate::demand::DemandError::Io(error),
            ))) if matches!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
            ) =>
            {
                self.unavailable_entries += 1;
                Ok(Some(Vec::new()))
            }
            result => result,
        }
    }

    async fn next_directory_page(&mut self) -> Result<Option<Vec<SearchName>>, SearchError> {
        if let Some(single) = self.single.take() {
            return Ok(Some(
                if self
                    .ignore_policies
                    .eligible(&self.ignore, &single.path, false)
                {
                    vec![single]
                } else {
                    Vec::new()
                },
            ));
        }
        let Some((directory, cursor)) = self.pending.pop() else {
            return Ok(None);
        };
        let mut names = Vec::new();
        let limits = match &self.cut {
            Cut::Lazy(lazy, _) => lazy.workspace().limits(),
            Cut::Exact(generation) => generation.workspace().limits(),
        };
        let maximum = page_entries(&directory, limits.maximum_component_bytes);
        let exact = matches!(&cursor, Continuation::Exact(_));
        if matches!(
            &cursor,
            Continuation::Exact(None) | Continuation::Lazy(None)
        ) {
            // Check before reading this directory's ignore files, including
            // during the authored seed pass. Policy reads are content reads
            // too and cannot enter a foreign native mount.
            if let Cut::Lazy(lazy, snapshot) = &self.cut
                && !lazy
                    .same_volume_at_snapshot(
                        *snapshot,
                        &directory,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await?
            {
                return Ok(Some(Vec::new()));
            }
            self.load_directory_ignore(directory.trim_matches('/'))
                .await?;
        }
        let (rows, next) = match (&self.cut, cursor) {
            (Cut::Lazy(lazy, snapshot), Continuation::Lazy(cursor)) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                if cursor.is_none() {
                    let node = lazy
                        .source_metadata_at_snapshot(*snapshot, &directory, &mut ctx)
                        .await?;
                    self.directory_versions
                        .observe(directory.trim_matches('/').to_owned(), node);
                }
                if let Some(cursor) = &cursor {
                    // Pages (including restored continuations) have separate
                    // standalone ledgers. Fund the retained cursor in this
                    // ledger before the directory reader retires its backing.
                    let (_, admitted) = ctx.record_actual_owned(cursor.retained_bytes()?)?;
                    admitted?;
                }
                let (page, _, guard) = lazy
                    .list_directory_at_snapshot(*snapshot, &directory, cursor, maximum, &mut ctx)
                    .await?;
                let rows = page
                    .entries
                    .iter()
                    .filter(|entry| !entry.authored)
                    .filter(|entry| {
                        let unicode = entry.name.unicode_utf8_bytes().is_some();
                        self.unavailable_entries += usize::from(!unicode);
                        unicode
                    })
                    .map(|entry| {
                        Ok((
                            entry
                                .name
                                .unicode_text(WorkBudget::UNBOUNDED, &stop)
                                .map_err(|failure| failure.error)?
                                .value
                                .to_string(),
                            entry.kind == SourceNodeKind::RegularFile,
                            entry.kind == SourceNodeKind::Directory,
                        ))
                    })
                    .collect::<Result<Vec<_>, SearchError>>()?;
                let next = page.next.map(|cursor| Continuation::Lazy(Some(cursor)));
                drop(page.entries);
                drop(guard);
                (rows, next)
            }
            (cut, Continuation::Exact(after)) => {
                let generation = match cut {
                    Cut::Exact(generation) => generation,
                    Cut::Lazy(..) => self.authored.as_ref().ok_or_else(|| {
                        SearchError::Invalid("missing authored seed generation".into())
                    })?,
                };
                let page = generation
                    .list_directory(
                        &directory,
                        after.as_ref(),
                        maximum,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                    .0;
                let rows = page
                    .entries
                    .iter()
                    .filter(|entry| {
                        let unicode = entry.name.unicode_utf8_bytes().is_some();
                        self.unavailable_entries += usize::from(!unicode);
                        unicode
                    })
                    .map(|entry| {
                        Ok((
                            entry
                                .name
                                .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                                .map_err(|failure| failure.error)?
                                .value
                                .to_string(),
                            entry.kind == FileKind::Regular,
                            entry.kind == FileKind::Directory,
                        ))
                    })
                    .collect::<Result<Vec<_>, SearchError>>()?;
                let next = if page.has_more {
                    page.entries
                        .last()
                        .map(|entry| Continuation::Exact(Some(entry.name.clone())))
                } else {
                    None
                };
                (rows, next)
            }
            _ => {
                return Err(SearchError::Invalid(
                    "directory continuation changed kind".into(),
                ));
            }
        };
        if let Some(cursor) = next {
            self.pending.push((directory.clone(), cursor));
        }
        let mut children = Vec::new();
        for (name, regular, is_directory) in rows {
            let path = format!("{}/{}", directory.trim_matches('/'), name)
                .trim_start_matches('/')
                .to_owned();
            if path
                .split('/')
                .any(|part| matches!(part, ".git" | ".acyclic-sdk"))
                || !self
                    .ignore_policies
                    .eligible(&self.ignore, &path, is_directory)
            {
                continue;
            }
            let name = SearchName {
                path,
                regular,
                directory: is_directory,
            };
            if name.maximum_json_bytes() as u64 > NAME_PAGE_BYTES - 2 {
                self.unavailable_entries += 1;
                continue;
            }
            if is_directory {
                children.push((
                    format!("/{}", name.path),
                    if exact {
                        Continuation::Exact(None)
                    } else {
                        self.start()
                    },
                ));
            }
            names.push(name);
        }
        if let Some(query) = &self.directory_query {
            children.sort_by_cached_key(|(path, _)| !directory_matches(path, query));
        }
        // Descend before reading the parent's next page. The frontier is at
        // most PAGE_ENTRIES siblings per depth, independent of tree breadth.
        if exact && self.authored.is_some() {
            // Visit a child's cheap authored metadata first, then its native
            // remainder. Keeping both phases on one DFS stack avoids queuing
            // every authored directory before the first native directory is
            // consumed. The frontier depends on depth, not total tree size.
            for (path, cursor) in children.into_iter().rev() {
                self.pending.push((path.clone(), Continuation::Lazy(None)));
                self.pending.push((path, cursor));
            }
        } else {
            self.pending.extend(children.into_iter().rev());
        }
        Ok(Some(names))
    }

    async fn load_directory_ignore(&mut self, directory: &str) -> Result<(), SearchError> {
        if let Cut::Lazy(lazy, snapshot) = &self.cut {
            for name in [".gitignore", ".ignore"]
                .into_iter()
                .chain(directory.is_empty().then_some(".git/info/exclude"))
            {
                let path = format!("{directory}/{name}")
                    .trim_start_matches('/')
                    .to_owned();
                if self.directory_versions.contains_key(&path) {
                    continue;
                }
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (entry, _, guard) = match lazy
                    .lookup_at_snapshot(*snapshot, &format!("/{path}"), &mut ctx)
                    .await
                {
                    Ok(observed) => observed,
                    Err(error) if missing_policy_parent(&error) => {
                        self.directory_versions.insert(path, None);
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                // Use the pinned observation that supplies the policy bytes,
                // rather than recording a newer version after reading them.
                // Authored policy content is fenced by the SDK generation.
                match &entry {
                    Some(crate::LazyLookup::Source { node, .. }) => {
                        self.directory_versions.insert(path, Some(node.version));
                    }
                    None => self.directory_versions.insert(path, None),
                    Some(crate::LazyLookup::Authored { .. } | crate::LazyLookup::Shadow { .. }) => {
                    }
                }
                if let Some(entry) = entry {
                    lazy.retire_lookup((entry, guard), &mut ctx)?;
                } else {
                    crate::kernel::retire_allocation(guard, 0, &mut ctx)?;
                }
            }
        }
        // Root rules were supplied by the owner. Only policy files are read;
        // listing and classifying candidate files never opens their contents.
        if directory.is_empty() || self.ignore_policies.contains_key(directory) {
            return Ok(());
        }
        let mut policy = None;
        for name in [".gitignore", ".ignore"] {
            let path = format!("{directory}/{name}");
            let bytes = match self.read_policy(&path, 1024 * 1024).await {
                Ok(bytes) => bytes,
                Err(error) if error.is_missing_policy() => continue,
                Err(error) => return Err(error),
            };
            policy
                .get_or_insert_with(GitIgnorePolicy::default)
                .extend(GitIgnorePolicy::parse(&String::from_utf8_lossy(&bytes)));
        }
        if let Some(policy) = policy {
            self.ignore_policies.insert(directory.to_owned(), policy);
        }
        Ok(())
    }

    /// Reads only an explicitly selected candidate at the original pinned cut.
    pub async fn read(
        &self,
        path: &str,
        maximum: u64,
    ) -> Result<acyclic_stream::OwnedBytes, SearchError> {
        self.read_selected(path, maximum, false).await
    }

    /// Reads resident ignore-policy bytes without fetching a placeholder.
    /// A missing policy is distinct from one whose rules are unavailable.
    pub async fn read_policy(
        &self,
        path: &str,
        maximum: u64,
    ) -> Result<acyclic_stream::OwnedBytes, SearchError> {
        if let Cut::Lazy(lazy, snapshot) = &self.cut {
            let parent = path
                .trim_matches('/')
                .rsplit_once('/')
                .map_or("", |(parent, _)| parent);
            let stop = CancellationToken::new();
            if !lazy
                .same_volume_at_snapshot(
                    *snapshot,
                    &format!("/{parent}"),
                    &mut WorkBudget::UNBOUNDED.accounting(&stop),
                )
                .await?
            {
                return Err(SearchError::PolicyUnavailable(path.to_owned()));
            }
        }
        self.read_selected(path, maximum, true).await
    }

    async fn read_selected(
        &self,
        path: &str,
        maximum: u64,
        policy: bool,
    ) -> Result<acyclic_stream::OwnedBytes, SearchError> {
        let absolute = format!("/{path}");
        match &self.cut {
            Cut::Lazy(lazy, snapshot) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (entry, _, guard) = lazy
                    .lookup_at_snapshot(*snapshot, &absolute, &mut ctx)
                    .await?;
                let Some(entry) = entry else {
                    crate::kernel::retire_allocation(guard, 0, &mut ctx)?;
                    return Err(crate::LazyWorkspaceError::NotFound.into());
                };
                // An indexed regular name can be replaced by a link between
                // enumeration and reading. Search never reads link payloads,
                // including policy files, through the generic snapshot reader.
                let unavailable = policy
                    && matches!(&entry, crate::LazyLookup::Source { node, .. }
                        if policy_requires_hydration(&node.metadata));
                let result = if unavailable {
                    Err(SearchError::PolicyUnavailable(path.to_owned()))
                } else if entry.stat().kind == SourceNodeKind::RegularFile {
                    lazy.read_resolved(&entry, &absolute, None, maximum, &mut ctx)
                        .await
                        .map(|chunk| chunk.bytes)
                        .map_err(SearchError::from)
                } else {
                    Err(crate::LazyWorkspaceError::NotRegularFile.into())
                };
                lazy.retire_lookup((entry, guard), &mut ctx)?;
                result
            }
            Cut::Exact(generation) => generation
                .read(
                    &absolute,
                    maximum,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| SearchError::from(failure.error)),
        }
    }
}

fn policy_requires_hydration(metadata: &crate::demand::SourceMetadata) -> bool {
    // These are source metadata, so interpret them on every host (including
    // a Windows volume exposed by a remote provider to a Linux harness).
    // Windows: OFFLINE, RECALL_ON_OPEN, RECALL_ON_DATA_ACCESS.
    const WINDOWS_PLACEHOLDER: u32 = 0x0000_1000 | 0x0004_0000 | 0x0040_0000;
    // Darwin stat.h: SF_DATALESS.
    const DARWIN_DATALESS: u64 = 0x4000_0000;
    metadata
        .windows_attributes
        .is_some_and(|attributes| attributes & WINDOWS_PLACEHOLDER != 0)
        || metadata
            .posix_flags
            .is_some_and(|flags| flags & DARWIN_DATALESS != 0)
}

fn directory_matches(path: &str, query: &str) -> bool {
    let mut query = query.chars().flat_map(char::to_lowercase);
    let mut wanted = query.next();
    for letter in path.chars().flat_map(char::to_lowercase) {
        if wanted == Some(letter) {
            wanted = query.next();
        }
    }
    wanted.is_none()
}

#[cfg(test)]
mod byte_page_tests {
    use super::*;

    #[test]
    fn name_search_pages_bound_long_paths_and_json_expansion() {
        for component in ["a".repeat(250), "\u{001b}".repeat(250)] {
            let directory = std::iter::repeat_n(component.as_str(), 128)
                .collect::<Vec<_>>()
                .join("/");
            let names = |count| {
                (0..count)
                    .map(|i| SearchName {
                        path: format!("{directory}/file-{i}.txt"),
                        regular: true,
                        directory: false,
                    })
                    .collect::<Vec<_>>()
            };
            let count = page_entries(&directory, 510);
            assert!((1..PAGE_ENTRIES).contains(&count));
            let page = names(count);
            assert!(serde_json::to_vec(&page).unwrap().len() as u64 <= NAME_PAGE_BYTES);
            assert!(
                page.iter().all(
                    |name| serde_json::to_vec(name).unwrap().len() <= name.maximum_json_bytes()
                )
            );
            // Negative control: the old entry-only bound overflows the same
            // file budget with valid component lengths and traversal depth.
            assert!(
                serde_json::to_vec(&names(PAGE_ENTRIES)).unwrap().len() as u64 > NAME_PAGE_BYTES
            );
        }
        assert_eq!(page_entries("/", 510), PAGE_ENTRIES);
    }
}
