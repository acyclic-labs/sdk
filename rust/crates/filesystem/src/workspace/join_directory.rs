//! Incremental files for one retained merge, separate from publication authority.

use super::*;
use std::num::NonZeroU32;

/// The v1 `plan.json` file. Counts describe the exposed directory, not a census
/// performed ahead of its first page. `complete` makes the clean count final.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeDirectoryPlan {
    /// Persisted format version.
    pub version: u8,
    /// Original common ancestor, target and source; never reopened from HEAD.
    pub base: GenerationId,
    /// Pinned destination.
    pub ours: GenerationId,
    /// Pinned child.
    pub theirs: GenerationId,
    /// Clean paths exposed so far.
    pub clean: u64,
    /// Exact conflict total from the original merge producer.
    pub conflicts: u64,
    /// Conflict files exposed so far.
    pub exposed_conflicts: u64,
    /// Both retained cursors reached EOF.
    pub complete: bool,
}

/// A clean path's identities. Bodies stay in the retained preview; this file is
/// discovery metadata and cannot authorize reading or publishing a record.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCleanFile {
    /// Original portable path.
    pub path: String,
    /// Destination identity before the merge.
    pub before: Option<FileId>,
    /// Preview identity after the merge, or deletion.
    pub after: Option<FileId>,
}

/// One conflict file. The shared conflict codec preserves exact immutable
/// references for non-text values; binary bodies are never embedded in JSON.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeConflictFile {
    /// Persisted format version.
    pub version: u8,
    /// Non-text resolution category; the original key remains authoritative.
    pub kind: MergeConflictFileKind,
    /// The original domain conflict, including its authoritative key.
    pub conflict: crate::ConflictView,
}

/// Non-text categories in `<path>.conflict.json`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MergeConflictFileKind {
    /// Opaque regular bytes.
    Binary,
    /// One side removed the entry.
    Deleted,
    /// Kind or binding topology differs.
    Type,
    /// Metadata, including executable mode, differs.
    Mode,
}

/// A filesystem projection that owns the original preview and its cancellation,
/// closure hold and cumulative read credit. A page publishes only staging files,
/// never a source or destination work tree. Failure is terminal: replanning is
/// required rather than retrying an advanced cursor and silently losing rows.
pub struct MergeDirectory<A: 'static, O: 'static> {
    preview: JoinPreview<A, O>,
    staging: Workspace<A, O>,
    path: String,
    plan: MergeDirectoryPlan,
    plan_condition: FileCondition,
    clean_done: bool,
    failed: bool,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> JoinPreview<A, O> {
    /// Places this preview under `merge/<id>/` in a separate staging volume.
    /// Constructing the projection performs no directory or changed-path scan.
    pub fn directory(
        mut self,
        staging: Workspace<A, O>,
        id: OperationId,
    ) -> Result<MergeDirectory<A, O>, WorkspaceError> {
        let plan = self.plan();
        if staging.id() == plan.source().id() || staging.id() == plan.target().id() {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let (base, ours, theirs) = (
            plan.common_ancestor(),
            plan.target_head(),
            plan.source_head(),
        );
        let conflicts = self.conflict_count() as u64;
        Ok(MergeDirectory {
            preview: self,
            staging,
            path: format!("/merge/{}", hex::encode(id.into_bytes())),
            plan: MergeDirectoryPlan {
                version: 1,
                base,
                ours,
                theirs,
                clean: 0,
                conflicts,
                exposed_conflicts: 0,
                complete: false,
            },
            plan_condition: FileCondition::Absent,
            clean_done: false,
            failed: false,
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> MergeDirectory<A, O> {
    /// The original join, for the existing publication coordinator. Directory
    /// files are proposals; only the original publisher can admit their effects.
    pub fn join(&mut self) -> &JoinPlan<A, O> {
        self.preview.plan()
    }

    /// Root containing plan.json and the clean/conflicts directories.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Last successfully published page's counts and original generation pins.
    pub fn plan(&self) -> &MergeDirectoryPlan {
        &self.plan
    }

    /// Writes at most `steps` original rows as one staging transaction. Excluded
    /// clean rows consume steps too. Work is proportional to selected paths and
    /// selected text bytes, with one commit per page and no per-file barriers.
    /// A false `complete` result always permits another page, including an empty
    /// page whose entire allowance was consumed by excluded conflicts.
    pub async fn write_page(
        &mut self,
        steps: NonZeroU32,
        permit: crate::PublicationPermit,
        cancellation: &CancellationToken,
    ) -> Result<&MergeDirectoryPlan, WorkspaceError> {
        if self.failed {
            return Err(WorkspaceError::InvalidMergeResolution);
        }
        if self.plan.complete {
            return Ok(&self.plan);
        }
        self.failed = true;
        let result = self.write_page_inner(steps, permit, cancellation).await;
        if result.is_ok() {
            self.failed = false;
        }
        result?;
        Ok(&self.plan)
    }

    async fn write_page_inner(
        &mut self,
        steps: NonZeroU32,
        permit: crate::PublicationPermit,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut transaction = self
            .staging
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let mut next = self.plan.clone();
        for name in ["clean", "conflicts"] {
            transaction
                .create_dir_all(
                    &format!("{}/{name}", self.path),
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.error)?;
        }
        for _ in 0..steps.get() {
            if !self.clean_done {
                match self
                    .preview
                    .clean_step()
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                {
                    JoinCleanStep::Entry(entry) => {
                        let row = entry.value();
                        let file = MergeCleanFile {
                            path: namespace_path_text(&row.path)?,
                            before: row.before.map(|record| record.file_id),
                            after: row.after.map(|record| record.file_id),
                        };
                        let path = format!("{}/clean/{}.json", self.path, encode_path(&file.path));
                        write_file(&mut transaction, &path, json(&file)?, cancellation).await?;
                        next.clean += 1;
                    }
                    JoinCleanStep::Excluded => {}
                    JoinCleanStep::Done => {
                        self.clean_done = true;
                    }
                }
                continue;
            }
            let Some(row) = self
                .preview
                .next_conflict()
                .map_err(|failure| failure.error)?
                .value
            else {
                next.complete = true;
                break;
            };
            let view = self
                .preview
                .plan()
                .describe_conflict(row.value().clone())
                .await?;
            let path = view
                .path
                .as_deref()
                .ok_or_else(|| WorkspaceError::path("merge conflict has no portable path"))?;
            let path = format!("{}/conflicts/{}", self.path, encode_path(path));
            let (path, bytes) = if view.kind == ConflictKind::Text {
                use crate::MergeDriver;
                let resolution = crate::DefaultTextMergeDriver
                    .resolve(&view)
                    .map_err(|_| WorkspaceError::InvalidMergeResolution)?;
                let text = match resolution {
                    MergeResolution::Text(text) => text,
                    MergeResolution::Select(side) => {
                        let value = match side {
                            ConflictSide::Base => view.base,
                            ConflictSide::Ours => view.ours,
                            ConflictSide::Theirs => view.theirs,
                        };
                        let ConflictValue::Text(text) = value else {
                            return Err(WorkspaceError::InvalidMergeResolution);
                        };
                        text
                    }
                    _ => return Err(WorkspaceError::InvalidMergeResolution),
                };
                (path, text.into_bytes())
            } else {
                let absent = |value: &ConflictValue| {
                    matches!(value, ConflictValue::Absent | ConflictValue::Binding(None))
                };
                let kind = if absent(&view.ours) || absent(&view.theirs) {
                    MergeConflictFileKind::Deleted
                } else {
                    match view.kind {
                        ConflictKind::Binary => MergeConflictFileKind::Binary,
                        ConflictKind::Metadata => MergeConflictFileKind::Mode,
                        _ => MergeConflictFileKind::Type,
                    }
                };
                let mut reference = binding_conflict_view(self.preview.plan(), row.value().clone());
                reference.path = view.path;
                reference.kind = view.kind;
                if absent(&view.ours) {
                    reference.ours = ConflictValue::Absent;
                }
                if absent(&view.theirs) {
                    reference.theirs = ConflictValue::Absent;
                }
                (
                    format!("{path}.conflict.json"),
                    json(&MergeConflictFile {
                        version: 1,
                        kind,
                        conflict: reference,
                    })?,
                )
            };
            write_file(&mut transaction, &path, bytes, cancellation).await?;
            next.exposed_conflicts += 1;
        }
        let bytes = json(&next)?;
        let digest = <sha2::Sha256 as sha2::Digest>::digest(&bytes).into();
        transaction
            .write_if(
                &format!("{}/plan.json", self.path),
                bytes,
                self.plan_condition,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(|failure| failure.error)?;
        match transaction
            .commit::<WorkspaceError, _, _>(
                None,
                permit,
                WorkBudget::UNBOUNDED,
                cancellation,
                publication_ready,
            )
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
            TransactionCommit::Conflict { .. } => return Err(WorkspaceError::StaleGeneration),
            TransactionCommit::Fenced | TransactionCommit::IdempotencyConflict => {
                return Err(WorkspaceError::StaleIdentity);
            }
        }
        self.plan = next;
        self.plan_condition = FileCondition::Digest(digest);
        Ok(())
    }
}

fn json(value: &impl Serialize) -> Result<Vec<u8>, WorkspaceError> {
    serde_json::to_vec(value).map_err(WorkspaceError::engine)
}

// Reserve the suffix namespace without losing legitimate names such as a%2Eb,
// a.conflict.json, or a.resolve.json. Slash remains the directory separator.
fn encode_path(path: &str) -> String {
    path.trim_start_matches('/')
        .replace('%', "%25")
        .replace('.', "%2E")
}

async fn write_file<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &mut Transaction<A, O>,
    path: &str,
    bytes: Vec<u8>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    let parent = path
        .rsplit_once('/')
        .ok_or_else(|| WorkspaceError::path(path))?
        .0;
    transaction
        .create_dir_all(parent, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map_err(|failure| failure.error)?;
    transaction
        .write_if(
            path,
            bytes,
            FileCondition::Absent,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}
