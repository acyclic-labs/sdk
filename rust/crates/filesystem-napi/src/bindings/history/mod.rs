use super::*;

/// One exact immutable customer workspace generation.
#[napi]
pub struct NativeGeneration {
    pub(in crate::bindings) inner: Generation<LocalAuthorityBackend, LocalObjectBackend>,
}

/// One immutable semantic delta between exact generations.
#[napi]
pub struct NativeChangeSet {
    pub(in crate::bindings) inner: ChangeSet<LocalAuthorityBackend, LocalObjectBackend>,
}

/// One immutable, side-effect-free workspace join plan.
#[napi]
pub struct NativeJoinPlan {
    pub(in crate::bindings) inner: JoinPlan<LocalAuthorityBackend, LocalObjectBackend>,
}

/// Exact bounded join planning options.
#[napi(object)]
pub struct NativeJoinOptions {
    pub history: String,
    pub maximum_generations: NapiU32,
    pub maximum_changes: NapiU32,
    pub maximum_conflicts: NapiU32,
    pub source_generation: Option<Buffer>,
    pub target_generation: Option<Buffer>,
}

/// Terminal result of applying one immutable join plan.
#[napi(object)]
pub struct NativeJoinResult {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub conflicts: Vec<NativeMergeConflict>,
    pub truncated: bool,
}

#[napi]
impl NativeChangeSet {
    #[must_use]
    #[napi(getter)]
    pub fn from(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.from().clone(),
        }
    }

    #[must_use]
    #[napi(getter)]
    pub fn to(&self) -> NativeGeneration {
        NativeGeneration {
            inner: self.inner.to().clone(),
        }
    }

    #[napi]
    pub fn changes(&self) -> Result<NativeGenerationDiff> {
        encode_generation_diff(self.inner.changes().clone(), self.inner.work())
    }

    #[napi]
    pub async fn compose(
        &self,
        next: &NativeChangeSet,
        maximum_changes: NapiU32,
    ) -> Result<NativeChangeSet> {
        let maximum_changes = maximum_changes.into_inner();
        sdk_value!(unbounded_future!(
            self.inner,
            compose(&next.inner, maximum_changes)
        ))
        .map(|inner| NativeChangeSet { inner })
    }
}

#[napi]
impl NativeJoinPlan {
    #[must_use]
    #[napi(getter, js_name = targetHead)]
    pub fn target_head(&self) -> Buffer {
        Buffer::from(self.inner.target_head().digest().into_bytes().to_vec())
    }

    #[must_use]
    #[napi(getter, js_name = commonAncestor)]
    pub fn common_ancestor(&self) -> Buffer {
        Buffer::from(self.inner.common_ancestor().digest().into_bytes().to_vec())
    }

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
                }, std::collections::BTreeMap::new(),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
            .map(native_join_result)
            .map_err(napi_error)
    }

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
    clippy::missing_errors_doc,
    reason = "every method returns napi::Result, which reaches JavaScript as a thrown exception"
)]
impl NativeGeneration {
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().digest().into_bytes().to_vec())
    }

    #[must_use]
    #[napi(getter, js_name = workspaceId)]
    pub fn workspace_id(&self) -> Buffer {
        Buffer::from(self.inner.workspace_id().into_bytes().to_vec())
    }

    #[napi]
    pub async fn read(&self, path: String, maximum_bytes: BigInt) -> Result<Buffer> {
        sdk_value!(Box::pin(unbounded_future!(
            self.inner,
            read(&path, bigint_u64(&maximum_bytes, "maximumBytes")?)
        )))
        .map(|value| Buffer::from(value.to_vec()))
    }

    #[napi(js_name = readRange)]
    pub async fn read_range(&self, path: String, offset: BigInt, length: BigInt) -> Result<Buffer> {
        sdk_value!(Box::pin(unbounded_future!(
            self.inner,
            read_range(
                &path,
                bigint_u64(&offset, "offset")?,
                bigint_u64(&length, "length")?
            )
        )))
        .map(|value| Buffer::from(value.bytes.to_vec()))
    }

    #[napi]
    pub async fn stat(&self, path: String) -> Result<NativeWorkspaceStat> {
        sdk_value!(Box::pin(unbounded_future!(self.inner, stat(&path))))
            .map(|value| native_workspace_stat(value))
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
        sdk_value!(Box::pin(unbounded_future!(
            self.inner,
            list_directory(&path, after.as_ref(), maximum_entries)
        )))
        .map(|value| native_workspace_directory_page(value.0))
    }

    #[napi(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Buffer> {
        sdk_value!(Box::pin(unbounded_future!(
            self.inner,
            read_symbolic_link(&path)
        )))
        .map(|value| Buffer::from(value.bytes.to_vec()))
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

    #[napi]
    pub async fn pin(&self, identity: String) -> Result<NativeGeneration> {
        sdk_value!(unbounded_future!(self.inner, pin(identity)))
            .map(|value| NativeGeneration { inner: value.1 })
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
    let (status, generation) = match outcome {
        JoinOutcome::Applied(value) => ("applied", Some(value.generation)),
        JoinOutcome::AlreadyApplied(value) => ("already-applied", Some(value.generation)),
        JoinOutcome::NoChanges(value) => ("no-changes", Some(value)),
        JoinOutcome::StaleTarget(value) => ("stale-target", Some(value)),
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => {
            return NativeJoinResult {
                status: "conflicted".to_owned(),
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
            };
        }
        JoinOutcome::Fenced => ("fenced", None),
        JoinOutcome::IdempotencyConflict => ("idempotency-conflict", None),
    };
    NativeJoinResult {
        status: status.to_owned(),
        generation_id: generation.map(|value| native_generation_buffer(value.id())),
        conflicts: Vec::new(),
        truncated: false,
    }
}
