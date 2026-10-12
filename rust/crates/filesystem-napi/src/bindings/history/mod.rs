use super::*;

pub(in crate::bindings) type NativeLocalChangeSet =
    ChangeSet<LocalAuthorityBackend, LocalObjectBackend>;

pub(in crate::bindings) type NativeLocalJoinPlan =
    JoinPlan<LocalAuthorityBackend, LocalObjectBackend>;

/// One exact immutable customer workspace generation.
#[napi]
pub struct NativeGeneration {
    pub(in crate::bindings) inner: NativeLocalGeneration,
}

/// One immutable semantic delta between exact generations.
#[napi]
pub struct NativeChangeSet {
    pub(in crate::bindings) inner: NativeLocalChangeSet,
}

/// One immutable, side-effect-free workspace join plan.
#[napi]
pub struct NativeJoinPlan {
    pub(in crate::bindings) inner: NativeLocalJoinPlan,
}

/// Exact bounded join planning options.
#[napi(object)]
pub struct NativeJoinOptions {
    /// `merge`, `rebase`, `squash`, or `cherry-pick`.
    pub history: String,
    /// Maximum lineage generations examined while finding an ancestor.
    pub maximum_generations: NapiU32,
    /// Maximum semantic changes admitted by the plan.
    pub maximum_changes: NapiU32,
    /// Maximum exact conflicts returned by application.
    pub maximum_conflicts: NapiU32,
    /// Optional caller-pinned source generation.
    pub source_generation: Option<Buffer>,
    /// Optional exact target CAS precondition.
    pub target_generation: Option<Buffer>,
}

/// Terminal result of applying one immutable join plan.
#[napi(object)]
pub struct NativeJoinResult {
    /// `applied`, `already-applied`, `no-changes`, `stale-target`, `conflicted`,
    pub status: String,
    /// Exact target generation for generation-bearing outcomes.
    pub generation_id: Option<Buffer>,
    /// Stable path-independent conflict regions.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether additional conflicts exceeded the retained result bound.
    pub truncated: bool,
}

#[napi]
impl NativeChangeSet {
    /// Exact immutable base endpoint.
    #[must_use]
    #[napi(getter)]
    pub fn from(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.from().clone(),
        }
    }

    /// Exact immutable resulting endpoint.
    #[must_use]
    #[napi(getter)]
    pub fn to(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.to().clone(),
        }
    }

    /// Stable path-independent records and namespace binding changes.
    #[napi]
    pub fn changes(&self) -> Result<NativeGenerationDiff> {
        encode_generation_diff(self.inner.changes().clone(), self.inner.work())
    }

    /// Composes contiguous immutable deltas by diffing their outer endpoints.
    #[napi]
    pub async fn compose(
        &self,
        next: &NativeChangeSet,
        maximum_changes: NapiU32,
    ) -> Result<NativeChangeSet> {
        let maximum_changes = maximum_changes.into_inner();

        self.inner
            .compose(&next.inner, maximum_changes)
            .await
            .map(|inner| NativeChangeSet { inner })
            .map_err(napi_error)
    }
}

#[napi]
impl NativeJoinPlan {
    /// Target generation observed while planning.
    #[must_use]
    #[napi(getter, js_name = targetHead)]
    pub fn target_head(&self) -> Buffer {
        Buffer::from(self.inner.target_head().digest().into_bytes().to_vec())
    }

    /// Exact discovered common ancestor.
    #[must_use]
    #[napi(getter, js_name = commonAncestor)]
    pub fn common_ancestor(&self) -> Buffer {
        Buffer::from(self.inner.common_ancestor().digest().into_bytes().to_vec())
    }

    /// Applies this immutable plan through one exact target-head CAS.
    #[napi]
    pub async fn apply(
        &self,
        if_target: Buffer,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeJoinResult> {
        let if_target = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &if_target,
            "target generation identity",
        )?));
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .apply(
                ApplyOptions {
                    if_target,
                    idempotency_key,
                },
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
            .map(native_join_result)
            .map_err(napi_error)
    }

    /// Applies an exact complete set of declarative conflict side selections.
    #[napi(js_name = applySides)]
    pub async fn apply_sides(
        &self,
        if_target: Buffer,
        idempotency_key: Option<Buffer>,
        selections: Vec<NativeMergeSelection>,
    ) -> Result<NativeJoinResult> {
        let if_target = generation_id(&if_target)?;
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        let mut resolved = BTreeMap::new();
        for selection in selections {
            let conflict = native_merge_selection_conflict(&selection)?;
            let side = match selection.side.as_str() {
                "base" => ConflictSide::Base,
                "ours" => ConflictSide::Ours,
                "theirs" => ConflictSide::Theirs,
                _ => {
                    return Err(Error::new(
                        Status::InvalidArg,
                        "merge selection side must be base, ours, or theirs",
                    ));
                }
            };
            if resolved.insert(conflict, side).is_some() {
                return Err(Error::new(
                    Status::InvalidArg,
                    "merge selections contain a duplicate conflict",
                ));
            }
        }
        self.inner
            .apply_sides(
                ApplyOptions {
                    if_target,
                    idempotency_key,
                },
                resolved,
            )
            .await
            .map(native_join_result)
            .map_err(napi_error)
    }
}

#[napi]
#[allow(
    missing_docs,
    reason = "the read_range, stat, list_directory, read_symbolic_link, and plan_extents bindings forward to the documented acyclic-fs methods of the same name"
)]
#[allow(
    clippy::missing_errors_doc,
    reason = "every method returns napi::Result, which reaches JavaScript as a thrown exception"
)]
impl NativeGeneration {
    /// Content-addressed generation identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().digest().into_bytes().to_vec())
    }

    /// Owning opaque workspace identity.
    #[must_use]
    #[napi(getter, js_name = workspaceId)]
    pub fn workspace_id(&self) -> Buffer {
        Buffer::from(self.inner.workspace_id().into_bytes().to_vec())
    }

    /// Reads one complete file from this exact immutable state.
    #[napi]
    pub async fn read(&self, path: String, maximum_bytes: BigInt) -> Result<Buffer> {
        Box::pin(self.inner.read(
            &path,
            bigint_u64(&maximum_bytes, "maximumBytes")?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| Buffer::from(receipt.value.to_vec()))
        .map_err(|failure| napi_error(failure.error))
    }

    #[napi(js_name = readRange)]
    pub async fn read_range(&self, path: String, offset: BigInt, length: BigInt) -> Result<Buffer> {
        Box::pin(self.inner.read_range(
            &path,
            bigint_u64(&offset, "offset")?,
            bigint_u64(&length, "length")?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| Buffer::from(receipt.value.bytes.to_vec()))
        .map_err(|failure| napi_error(failure.error))
    }

    #[napi]
    pub async fn stat(&self, path: String) -> Result<NativeWorkspaceStat> {
        Box::pin(async {
            self.inner
                .stat(
                    &path,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
        })
        .await
        .map(native_workspace_stat)
        .map_err(napi_error)
    }

    #[napi(js_name = listDirectory)]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<NativeWorkspaceName>,
        maximum_entries: NapiU32,
    ) -> Result<NativeWorkspaceDirectoryPage> {
        let maximum_entries = maximum_entries.into_inner();

        let after = after.map(native_workspace_name).transpose()?;
        Box::pin(async {
            self.inner
                .list_directory(
                    &path,
                    after.as_ref(),
                    maximum_entries,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| failure.error)
        })
        .await
        .map(native_workspace_directory_page)
        .map_err(napi_error)
    }

    #[napi(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Buffer> {
        Box::pin(async {
            self.inner
                .read_symbolic_link(
                    &path,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(|failure| failure.error)
        })
        .await
        .map(|bytes| Buffer::from(bytes.to_vec()))
        .map_err(napi_error)
    }

    #[napi(js_name = planExtents)]
    pub async fn plan_extents(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        maximum_spans: NapiU32,
    ) -> Result<NativeWorkspaceExtentPlan> {
        let maximum_spans = maximum_spans.into_inner();

        Box::pin(self.inner.plan_extents(
            &path,
            bigint_u64(&offset, "offset")?,
            bigint_u64(&length, "length")?,
            maximum_spans,
        ))
        .await
        .map(native_workspace_extent_plan)
        .map_err(napi_error)
    }

    /// Retains this exact generation under one opaque identity.
    #[napi]
    pub async fn pin(&self, identity: String) -> Result<NativeGeneration> {
        self.inner
            .pin(
                identity,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| NativeGeneration {
                inner: receipt.value.1,
            })
            .map_err(|failure| napi_error(failure.error))
    }
}

pub(in crate::bindings) fn native_join_history(value: &str) -> Result<JoinHistory> {
    match value {
        "merge" => Ok(JoinHistory::Merge),
        "rebase" => Ok(JoinHistory::Rebase),
        "squash" => Ok(JoinHistory::Squash),
        "cherry-pick" => Ok(JoinHistory::CherryPick),
        _ => Err(Error::new(
            Status::InvalidArg,
            "join history must be merge, rebase, squash, or cherry-pick",
        )),
    }
}

pub(in crate::bindings) fn native_join_result(
    outcome: JoinOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeJoinResult {
    let generation = |status: &str, generation: NativeLocalGeneration| NativeJoinResult {
        status: status.to_owned(),
        generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        conflicts: Vec::new(),
        truncated: false,
    };
    match outcome {
        JoinOutcome::Applied(value) => generation("applied", value.generation),
        JoinOutcome::AlreadyApplied(value) => generation("already-applied", value.generation),
        JoinOutcome::NoChanges(value) => generation("no-changes", value),
        JoinOutcome::StaleTarget(value) => generation("stale-target", value),
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => NativeJoinResult {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
        },
        JoinOutcome::Fenced => NativeJoinResult {
            status: "fenced".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
        JoinOutcome::IdempotencyConflict => NativeJoinResult {
            status: "idempotency-conflict".to_owned(),
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}
