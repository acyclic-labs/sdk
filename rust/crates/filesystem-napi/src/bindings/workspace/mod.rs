use super::*;

/// Workspace-aware final-close outcome.
#[napi(object)]
pub struct NativeWorkspaceOperationFinish {
    /// `still-active`, `already-closed`, or `reconciled`.
    pub kind: String,
    /// Live lease count for `still-active`.
    pub remaining: Option<u32>,
    /// Core rebase result for `reconciled`.
    pub rebase: Option<NativeWorkspaceRebaseResult>,
}

/// One named customer workspace backed by the embedded local engine.
#[napi]
pub struct NativeWorkspace {
    pub(in crate::bindings) inner: Workspace<LocalAuthorityBackend, LocalObjectBackend>,
}

/// Terminal result of advancing a fork onto its source workspace.
#[napi(object)]
pub struct NativeWorkspaceRebaseResult {
    /// `rebased`, `already-rebased`, `current`, `stale`, `conflicted`, `fenced`,
    pub status: String,
    /// Exact fork generation for generation-bearing outcomes.
    pub generation_id: Option<Buffer>,
    /// Stable path-independent conflict regions.
    pub conflicts: Vec<NativeMergeConflict>,
    /// Whether additional conflicts exceeded the retained result bound.
    pub truncated: bool,
}

/// Terminal workspace publication outcome.
#[napi(object)]
pub struct NativeWorkspaceCommit {
    /// Stable terminal status.
    pub status: String,
    /// Published or actual generation identity when applicable.
    pub generation_id: Option<Buffer>,
}

/// One exact customer-visible path stat without storage topology.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceStat {
    pub file_id: Buffer,
    pub kind: FileKind,
    pub link_count: BigInt,
    pub logical_bytes: Option<BigInt>,
    pub metadata: NativeWorkspaceMetadata,
}

/// Scalar cross-profile metadata and opaque-payload presence.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceMetadata {
    pub posix_mode: Option<u32>,
    pub posix_uid: Option<u32>,
    pub posix_gid: Option<u32>,
    pub posix_flags: Option<BigInt>,
    pub windows_attributes: Option<u32>,
    pub created_ns: Option<BigInt>,
    pub modified_ns: Option<BigInt>,
    pub accessed_ns: Option<BigInt>,
    pub changed_ns: Option<BigInt>,
    pub has_named_attributes: bool,
    pub has_acl: bool,
    pub has_security_descriptor: bool,
}

/// One exact encoded directory name.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceName {
    pub encoding: String,
    pub bytes: Buffer,
}

/// One child in a bounded directory page.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceDirectoryEntry {
    pub name: NativeWorkspaceName,
    pub file_id: Buffer,
    pub kind: FileKind,
}

/// One bounded authenticated directory page.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceDirectoryPage {
    pub entries: Vec<NativeWorkspaceDirectoryEntry>,
    pub has_more: bool,
}

/// One topology-free sparse extent span.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceExtentSpan {
    pub offset: BigInt,
    pub length: BigInt,
    pub source_end: BigInt,
    pub kind: String,
}

/// One bounded topology-free sparse extent plan.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeWorkspaceExtentPlan {
    pub spans: Vec<NativeWorkspaceExtentSpan>,
}

#[napi]
#[allow(
    missing_docs,
    reason = "the read_range, stat, read_symbolic_link, and plan_extents bindings forward to the documented acyclic-fs methods of the same name"
)]
#[allow(
    clippy::missing_errors_doc,
    reason = "every method returns napi::Result, which reaches JavaScript as a thrown exception"
)]
impl NativeWorkspace {
    /// Canonical customer workspace name.
    #[must_use]
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().as_str().to_owned()
    }

    /// Stable opaque workspace identity.
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().into_bytes().to_vec())
    }

    /// Current immutable generation identity.
    #[napi]
    pub async fn head(&self) -> Result<Buffer> {
        sdk_value!(unbounded_future!(self.inner, head()))
            .map(|value| Buffer::from(value.id().digest().into_bytes().to_vec()))
    }

    /// Reopens an authenticated exact retained generation without moving the head.
    #[napi]
    pub async fn generation(&self, id: Buffer) -> Result<NativeGeneration> {
        let id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            id.as_ref(),
            "generation identity",
        )?));
        sdk_value!(unbounded_future!(self.inner, generation(id)))
            .map(|inner| NativeGeneration { inner })
    }

    /// Synchronizes prior workspace operations and returns the exact immutable head.
    #[napi]
    pub async fn sync(&self) -> Result<NativeGeneration> {
        sdk_value!(unbounded_future!(self.inner, head()))
            .map(|value| NativeGeneration { inner: value })
    }

    /// Retains the current generation under one human-readable label.
    #[napi]
    pub async fn checkpoint(&self, label: String) -> Result<NativeGeneration> {
        self.inner
            .checkpoint(label)
            .await
            .map(|checkpoint| NativeGeneration {
                inner: checkpoint.1,
            })
            .map_err(napi_error)
    }

    /// Retains the current generation under one opaque stable identity.
    #[napi]
    pub async fn pin(&self, identity: String) -> Result<NativeGeneration> {
        self.inner
            .pin(identity)
            .await
            .map(|pin| NativeGeneration { inner: pin.1 })
            .map_err(napi_error)
    }

    /// Terminally removes this mutable workspace head without invalidating retained state.
    #[napi]
    pub async fn delete(&self, idempotency_key: Option<Buffer>) -> Result<String> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .delete(idempotency_key)
            .await
            .map(|outcome| {
                match outcome {
                    WorkspaceDelete::Deleted => "deleted",
                    WorkspaceDelete::AlreadyDeleted => "already-deleted",
                    WorkspaceDelete::Conflict => "conflict",
                    WorkspaceDelete::IdempotencyConflict => "idempotency-conflict",
                }
                .to_owned()
            })
            .map_err(napi_error)
    }

    /// Returns the durable semantic state of this workspace's attached source.
    #[napi(js_name = sourceState)]
    pub async fn source_state(&self) -> NativeSourceResult {
        match self.inner.source().cloned() {
            Some(source) => native_source_state(source.state().await),
            None => NativeSourceResult {
                status: "none".to_owned(),
                reason: None,
                generation_id: None,
            },
        }
    }

    /// Captures one contiguous native change interval and conditionally publishes it.
    #[napi(js_name = reconcileSource)]
    pub async fn reconcile_source(&self) -> Result<NativeSourceResult> {
        let source =
            self.inner.source().cloned().ok_or_else(|| {
                Error::new(Status::InvalidArg, "workspace has no attached source")
            })?;
        Box::pin(source.reconcile())
            .await
            .map(native_reconcile_outcome)
            .map_err(napi_error)
    }

    /// Rebuilds a complete authenticated source baseline.
    #[napi(js_name = rescanSource)]
    pub async fn rescan_source(&self) -> Result<NativeSourceResult> {
        let source =
            self.inner.source().cloned().ok_or_else(|| {
                Error::new(Status::InvalidArg, "workspace has no attached source")
            })?;
        Box::pin(source.rescan())
            .await
            .map(native_reconcile_outcome)
            .map_err(napi_error)
    }

    /// Captures all remaining source state into an independent immutable generation.
    #[napi]
    pub async fn seal(&self) -> Result<NativeGeneration> {
        Box::pin(self.inner.seal())
            .await
            .map(|inner| NativeGeneration { inner })
            .map_err(napi_error)
    }

    /// Reads one complete regular file under a caller bound.
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

    /// Atomically creates or replaces one complete file.
    #[napi]
    pub async fn write(&self, path: String, bytes: Buffer) -> Result<NativeWorkspaceCommit> {
        self.inner
            .write(&path, bytes::Bytes::from(bytes.to_vec()))
            .await
            .map(workspace_commit)
            .map_err(napi_error)
    }

    /// Removes one existing path atomically.
    #[napi]
    pub async fn remove(&self, path: String) -> Result<NativeWorkspaceCommit> {
        self.inner
            .remove(&path)
            .await
            .map(workspace_commit)
            .map_err(napi_error)
    }

    /// Creates an independent named workspace at the current generation.
    #[napi]
    pub async fn fork(
        &self,
        destination: String,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspace> {
        let generation = sdk_value!(unbounded_future!(self.inner, head()))?;
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        self.inner
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                destination,
                ForkOptions::from_generation(generation, idempotency_key),
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                &mut publication_ready,
            )
            .await
            .map(|receipt| receipt.value)
            .map(|inner| NativeWorkspace { inner })
            .map_err(|failure| napi_error(failure.error))
    }

    /// Creates an independent workspace at one caller-selected exact generation.
    #[napi(js_name = forkAt)]
    pub async fn fork_at(
        &self,
        destination: String,
        generation: &NativeGeneration,
        options: NativeForkOptions,
    ) -> Result<NativeWorkspace> {
        let key = native_idempotency_key(options.idempotency_key)?;
        let fork = ForkOptions::from_generation(generation.inner.clone(), key);
        let fork = match options.paths {
            Some(paths) => fork.inherit_paths(paths),
            None => fork,
        };
        self.inner
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                destination,
                fork,
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                &mut publication_ready,
            )
            .await
            .map(|receipt| receipt.value)
            .map(|inner| NativeWorkspace { inner })
            .map_err(|failure| napi_error(failure.error))
    }

    /// Begins one sparse atomic transaction at the current workspace head.
    #[napi(js_name = beginTransaction)]
    pub async fn begin_transaction(
        &self,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspaceTransaction> {
        let idempotency_key = native_idempotency_key(idempotency_key)?;
        unbounded_future!(self.inner, begin_transaction(idempotency_key))
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map(|inner| NativeWorkspaceTransaction {
                inner: tokio::sync::Mutex::new(inner),
            })
            .map_err(napi_error)
    }

    /// Advances this fork onto its source workspace's current generation.
    #[napi(js_name = liveRebase)]
    pub async fn live_rebase(
        &self,
        idempotency_key: Option<Buffer>,
        maximum_generations: NapiU32,
        maximum_changes: NapiU32,
        maximum_conflicts: NapiU32,
    ) -> Result<NativeWorkspaceRebaseResult> {
        let maximum_generations = maximum_generations.into_inner();
        let maximum_changes = maximum_changes.into_inner();
        let maximum_conflicts = maximum_conflicts.into_inner();

        self.inner
            .live_rebase(
                native_idempotency_key(idempotency_key)?,
                maximum_generations,
                maximum_changes,
                maximum_conflicts,
            )
            .await
            .map(native_workspace_rebase_result)
            .map_err(napi_error)
    }

    /// Computes one immutable bounded semantic delta between exact generations.
    #[napi]
    pub async fn diff(
        &self,
        from: &NativeGeneration,
        to: &NativeGeneration,
        maximum_changes: NapiU32,
    ) -> Result<NativeChangeSet> {
        let maximum_changes = maximum_changes.into_inner();
        sdk_value!(unbounded_future!(
            self.inner,
            diff(&from.inner, &to.inner, maximum_changes)
        ))
        .map(|inner| NativeChangeSet { inner })
    }

    /// Builds one immutable side-effect-free plan for joining this workspace into a target.
    #[napi(js_name = joinInto)]
    pub async fn join_into(
        &self,
        target: &NativeWorkspace,
        options: NativeJoinOptions,
    ) -> Result<NativeJoinPlan> {
        let history = native_join_history(&options.history)?;
        self.inner
            .join_into(&target.inner)
            .history(history)
            .bounds(
                options.maximum_generations.into_inner(),
                options.maximum_changes.into_inner(),
                options.maximum_conflicts.into_inner(),
            )
            .plan_at(
                &match options.source_generation {
                    Some(id) => self
                        .inner
                        .generation(
                            acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                                &id,
                                "source generation",
                            )?)),
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| napi_error(failure.error))?,
                    None => sdk_value!(unbounded_future!(self.inner, head()))?,
                },
                &match options.target_generation {
                    Some(id) => sdk_value!(unbounded_future!(
                        target.inner,
                        generation(acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                            &id,
                            "target generation",
                        )?)))
                    ))?,
                    None => sdk_value!(unbounded_future!(target.inner, head()))?,
                },
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map(|inner| NativeJoinPlan { inner })
            .map_err(napi_error)
    }

    /// Mounts this workspace or one exact subtree through the host-native driver.
    #[napi]
    pub async fn mount(
        &self,
        destination: String,
        options: NativeWorkspaceMountOptions,
    ) -> Result<NativeWorkspaceMount> {
        let publication = match options.publication.as_str() {
            "close-and-sync" => MountPublication::CloseAndSync,
            "per-mutation" => MountPublication::PerMutation,
            "manual" => MountPublication::Manual,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "mount publication must be close-and-sync, per-mutation, or manual",
                ));
            }
        };
        let mount_options = if options.writable {
            MountOptions::read_write()
        } else {
            MountOptions::read_only()
        }
        .subdirectory(options.subdirectory)
        .publication(publication);
        let mount = self
            .inner
            .mount(PathBuf::from(&destination), mount_options)
            .await
            .map_err(napi_error)?;
        Ok(NativeWorkspaceMount {
            inner: Arc::new(std::sync::Mutex::new(Some(mount))),
            path: destination,
        })
    }
}

pub(in crate::bindings) fn native_workspace_rebase_result(
    outcome: WorkspaceRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeWorkspaceRebaseResult {
    let (status, generation) = match outcome {
        WorkspaceRebase::Rebased(value) => ("rebased", Some(value)),
        WorkspaceRebase::AlreadyRebased(value) => ("already-rebased", Some(value)),
        WorkspaceRebase::Current(value) => ("current", Some(value)),
        WorkspaceRebase::Stale(value) => ("stale", Some(value)),
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => {
            return NativeWorkspaceRebaseResult {
                status: "conflicted".to_owned(),
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
            };
        }
        WorkspaceRebase::Fenced => ("fenced", None),
        WorkspaceRebase::IdempotencyConflict => ("idempotency-conflict", None),
    };
    NativeWorkspaceRebaseResult {
        status: status.to_owned(),
        generation_id: generation.map(|value| native_generation_buffer(value.id())),
        conflicts: Vec::new(),
        truncated: false,
    }
}

pub(in crate::bindings) fn native_workspace_operation_finish(
    outcome: WorkspaceOperationFinish<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeWorkspaceOperationFinish {
    let (kind, remaining, rebase) = match outcome {
        WorkspaceOperationFinish::StillActive { remaining } => {
            ("still-active", Some(remaining), None)
        }
        WorkspaceOperationFinish::AlreadyClosed => ("already-closed", None, None),
        WorkspaceOperationFinish::Reconciled(outcome) => (
            "reconciled",
            None,
            Some(native_workspace_rebase_result(outcome)),
        ),
    };
    NativeWorkspaceOperationFinish {
        kind: kind.to_owned(),
        remaining,
        rebase,
    }
}

pub(in crate::bindings) fn native_workspace_metadata(
    value: WorkspaceMetadata,
) -> NativeWorkspaceMetadata {
    NativeWorkspaceMetadata {
        posix_mode: value.posix_mode,
        posix_uid: value.posix_uid,
        posix_gid: value.posix_gid,
        posix_flags: value.posix_flags.map(bigint),
        windows_attributes: value.windows_attributes,
        created_ns: value.created_ns.map(signed_bigint),
        modified_ns: value.modified_ns.map(signed_bigint),
        accessed_ns: value.accessed_ns.map(signed_bigint),
        changed_ns: value.changed_ns.map(signed_bigint),
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes the stat it projects into the N-API shape"
)]
pub(in crate::bindings) fn native_workspace_stat(value: WorkspaceStat) -> NativeWorkspaceStat {
    NativeWorkspaceStat {
        file_id: Buffer::from(value.file_id.into_bytes().to_vec()),
        kind: value.kind,
        link_count: bigint(value.link_count),
        logical_bytes: value.logical_bytes.map(bigint),
        metadata: native_workspace_metadata(value.metadata),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes the JavaScript name it validates"
)]
pub(in crate::bindings) fn native_workspace_name(
    value: NativeWorkspaceName,
) -> Result<LogicalName> {
    let encoding = NameEncoding::from_public_str(&value.encoding)
        .ok_or_else(|| Error::new(Status::InvalidArg, "unknown name encoding"))?;
    LogicalName::new(encoding, value.bytes.to_vec(), u32::MAX).map_err(napi_error)
}

pub(in crate::bindings) fn native_workspace_name_value(name: LogicalName) -> NativeWorkspaceName {
    NativeWorkspaceName {
        encoding: name.encoding().as_str().to_owned(),
        bytes: Buffer::from(name.as_bytes().to_vec()),
    }
}

pub(in crate::bindings) fn native_workspace_directory_page(
    value: DirectoryPage,
) -> NativeWorkspaceDirectoryPage {
    NativeWorkspaceDirectoryPage {
        entries: value
            .entries
            .into_iter()
            .map(|entry| NativeWorkspaceDirectoryEntry {
                name: native_workspace_name_value(entry.name),
                file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
                kind: entry.kind,
            })
            .collect(),
        has_more: value.has_more,
    }
}

pub(in crate::bindings) fn native_workspace_extent_plan(
    value: WorkspaceExtentPlan,
) -> NativeWorkspaceExtentPlan {
    NativeWorkspaceExtentPlan {
        spans: value
            .spans
            .into_iter()
            .map(|span| NativeWorkspaceExtentSpan {
                offset: bigint(span.offset),
                length: bigint(span.length),
                source_end: bigint(span.source_end),
                kind: match span.kind {
                    WorkspaceExtentKind::Hole => "hole",
                    WorkspaceExtentKind::AllocatedZero => "allocated-zero",
                    WorkspaceExtentKind::Content => "content",
                }
                .to_owned(),
            })
            .collect(),
    }
}
