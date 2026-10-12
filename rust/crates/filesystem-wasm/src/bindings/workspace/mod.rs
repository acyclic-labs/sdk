use super::*;

/// One named customer workspace.
#[wasm_bindgen]
pub struct BrowserWorkspace {
    pub(in crate::bindings) engine: Workspace<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum BrowserWorkspaceCommitStatus {
    Committed,
    AlreadyCommitted,
    Conflict,
    Fenced,
    IdempotencyConflict,
}

#[derive(Deserialize, Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserWorkspaceName {
    #[tsify(type = "\"utf8\" | \"posix-bytes\" | \"windows-utf16le\"")]
    pub(in crate::bindings) encoding: String,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) bytes: Vec<u8>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserWorkspaceMetadata {
    pub(in crate::bindings) posix_mode: Option<u32>,
    pub(in crate::bindings) posix_uid: Option<u32>,
    pub(in crate::bindings) posix_gid: Option<u32>,
    pub(in crate::bindings) posix_flags: Option<u64>,
    pub(in crate::bindings) windows_attributes: Option<u32>,
    pub(in crate::bindings) created_ns: Option<i64>,
    pub(in crate::bindings) modified_ns: Option<i64>,
    pub(in crate::bindings) accessed_ns: Option<i64>,
    pub(in crate::bindings) changed_ns: Option<i64>,
    pub(in crate::bindings) has_named_attributes: bool,
    pub(in crate::bindings) has_acl: bool,
    pub(in crate::bindings) has_security_descriptor: bool,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserWorkspaceStat {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) kind: FileKind,
    pub(in crate::bindings) link_count: u64,
    pub(in crate::bindings) logical_bytes: Option<u64>,
    pub(in crate::bindings) metadata: BrowserWorkspaceMetadata,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserWorkspaceDirectoryEntry {
    pub(in crate::bindings) name: BrowserWorkspaceName,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) kind: FileKind,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserWorkspaceDirectoryPage {
    pub(in crate::bindings) entries: Vec<BrowserWorkspaceDirectoryEntry>,
    pub(in crate::bindings) has_more: bool,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserWorkspaceExtentSpan {
    pub(in crate::bindings) offset: u64,
    pub(in crate::bindings) length: u64,
    pub(in crate::bindings) source_end: u64,
    #[tsify(type = "\"hole\" | \"allocated-zero\" | \"content\"")]
    pub(in crate::bindings) kind: &'static str,
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserWorkspaceExtentPlan {
    pub(in crate::bindings) spans: Vec<BrowserWorkspaceExtentSpan>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserWorkspaceRebaseResult {
    #[tsify(
        type = "\"rebased\" | \"already-rebased\" | \"current\" | \"stale\" | \"conflicted\" | \"fenced\" | \"idempotency-conflict\""
    )]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) conflicts: Vec<MergeConflictResult>,
    pub(in crate::bindings) truncated: bool,
}

#[wasm_bindgen]
impl BrowserWorkspace {
    /// Canonical workspace name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.engine.name().as_str().to_owned()
    }

    /// Stable opaque workspace identity.
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> Vec<u8> {
        self.engine.id().into_bytes().to_vec()
    }

    /// Current immutable generation identity.
    #[wasm_bindgen]
    pub async fn head(&self) -> Result<Vec<u8>, JsValue> {
        let id = self
            .engine
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?
            .id();
        Ok(id.digest().into_bytes().to_vec())
    }

    /// Reopens an authenticated exact retained generation without moving the head.
    #[wasm_bindgen]
    pub async fn generation(&self, id: Vec<u8>) -> Result<BrowserGeneration, JsValue> {
        let id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &id,
            "generation identity",
        )?));
        let engine = Box::pin(self.engine.generation(
            id,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| js_error(failure.error))?;
        Ok(BrowserGeneration { engine })
    }

    /// Synchronizes prior operations and returns the exact immutable head.
    #[wasm_bindgen]
    pub async fn sync(&self) -> Result<BrowserGeneration, JsValue> {
        let engine = Box::pin(self.engine.head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map_err(|failure| js_error(failure.error))?
        .value;
        Ok(BrowserGeneration { engine })
    }

    /// Retains the current generation under one human-readable label.
    #[wasm_bindgen]
    pub async fn checkpoint(&self, label: String) -> Result<BrowserGeneration, JsValue> {
        let engine = Box::pin(self.engine.checkpoint(label))
            .await
            .map_err(js_error)?
            .1;
        Ok(BrowserGeneration { engine })
    }

    /// Retains the current generation under one opaque stable identity.
    #[wasm_bindgen]
    pub async fn pin(&self, identity: String) -> Result<BrowserGeneration, JsValue> {
        let engine = Box::pin(self.engine.pin(identity))
            .await
            .map_err(js_error)?
            .1;
        Ok(BrowserGeneration { engine })
    }

    /// Terminally removes this mutable workspace head.
    #[wasm_bindgen]
    pub async fn delete(&self, idempotency_key: Option<Vec<u8>>) -> Result<String, JsValue> {
        let key = idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let outcome = self.engine.delete(key).await.map_err(js_error)?;
        Ok(match outcome {
            WorkspaceDelete::Deleted => "deleted",
            WorkspaceDelete::AlreadyDeleted => "already-deleted",
            WorkspaceDelete::Conflict => "conflict",
            WorkspaceDelete::IdempotencyConflict => "idempotency-conflict",
        }
        .to_owned())
    }

    /// Reads one complete regular file under a byte bound.
    #[wasm_bindgen]
    pub async fn read(&self, path: String, maximum_bytes: u64) -> Result<Vec<u8>, JsValue> {
        let bytes = Box::pin(self.engine.read(
            &path,
            maximum_bytes,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| js_error(failure.error))?;
        Ok(bytes.to_vec())
    }

    #[wasm_bindgen(js_name = readRange)]
    pub async fn read_range(
        &self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, JsValue> {
        let bytes = Box::pin(self.engine.read_range(
            &path,
            offset,
            length,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value.bytes)
        .map_err(|failure| js_error(failure.error))?;
        Ok(bytes.to_vec())
    }

    #[wasm_bindgen]
    pub async fn stat(&self, path: String) -> Result<Ts<BrowserWorkspaceStat>, JsValue> {
        let value = self
            .engine
            .stat(
                &path,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?;
        ts(browser_workspace_stat(value))
    }

    #[wasm_bindgen(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Vec<u8>, JsValue> {
        let value = self
            .engine
            .read_symbolic_link(
                &path,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value.bytes)
            .map_err(|failure| js_error(failure.error))?;
        Ok(value.to_vec())
    }

    #[wasm_bindgen(js_name = planExtents)]
    pub async fn plan_extents(
        &self,
        path: String,
        offset: u64,
        length: u64,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_spans: JsValue,
    ) -> Result<Ts<BrowserWorkspaceExtentPlan>, JsValue> {
        let maximum_spans = wasm_u32(&maximum_spans)?;

        let value = self
            .engine
            .plan_extents(&path, offset, length, maximum_spans)
            .await
            .map_err(js_error)?;
        ts(browser_workspace_extent_plan(value))
    }

    /// Atomically creates or replaces one complete file.
    #[wasm_bindgen]
    pub async fn write(
        &self,
        path: String,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserWorkspaceCommit>, JsValue> {
        let outcome = Box::pin(self.engine.write(&path, bytes::Bytes::from(bytes)))
            .await
            .map(browser_workspace_commit)
            .map_err(js_error)?;
        ts(outcome)
    }

    /// Removes one existing path atomically.
    #[wasm_bindgen]
    pub async fn remove(&self, path: String) -> Result<Ts<BrowserWorkspaceCommit>, JsValue> {
        let outcome = Box::pin(self.engine.remove(&path))
            .await
            .map(browser_workspace_commit)
            .map_err(js_error)?;
        ts(outcome)
    }

    /// Forks the current generation into an independent named workspace.
    #[wasm_bindgen]
    pub async fn fork(
        &self,
        destination: String,
        idempotency_key: Option<Vec<u8>>,
    ) -> Result<BrowserWorkspace, JsValue> {
        let idempotency_key = idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let engine = {
            let value = &self.engine;
            let generation = Box::pin(value.head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            ))
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?;
            Box::pin(value.fork(
                destination,
                ForkOptions::from_generation(generation, idempotency_key),
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::default(),
                    }))
                },
            ))
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?
        };
        Ok(BrowserWorkspace { engine })
    }

    /// Creates an independent workspace at one caller-selected exact generation.
    #[wasm_bindgen(js_name = forkAt)]
    pub async fn fork_at(
        &self,
        destination: String,
        generation: &BrowserGeneration,
        #[wasm_bindgen(unchecked_param_type = "BrowserForkOptions")] options: JsValue,
    ) -> Result<BrowserWorkspace, JsValue> {
        let options: BrowserForkOptions =
            serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let idempotency_key = options.idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let engine = {
            let (value, generation) = (&self.engine, &generation.engine);
            Box::pin(value.fork(
                destination,
                browser_fork_options(generation.clone(), idempotency_key, options.paths.clone()),
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::default(),
                    }))
                },
            ))
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| js_error(failure.error))?
        };
        Ok(BrowserWorkspace { engine })
    }

    /// Begins one sparse atomic transaction at the current workspace head.
    #[wasm_bindgen(js_name = beginTransaction)]
    pub async fn begin_transaction(
        &self,
        idempotency_key: Option<Vec<u8>>,
    ) -> Result<BrowserTransaction, JsValue> {
        let idempotency_key = idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let engine = Box::pin(self.engine.begin_transaction(
            idempotency_key,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| js_error(failure.error))?;
        Ok(BrowserTransaction { engine })
    }

    /// Advances this fork onto its source workspace's current generation.
    #[wasm_bindgen(js_name = liveRebase)]
    pub async fn live_rebase(
        &self,
        idempotency_key: Option<Vec<u8>>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_generations: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_changes: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserWorkspaceRebaseResult>, JsValue> {
        let maximum_generations = wasm_u32(&maximum_generations)?;
        let maximum_changes = wasm_u32(&maximum_changes)?;
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;

        let idempotency_key = idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let outcome = self
            .engine
            .live_rebase(
                idempotency_key,
                maximum_generations,
                maximum_changes,
                maximum_conflicts,
            )
            .await
            .map(browser_workspace_rebase_result)
            .map_err(js_error)?;
        ts(outcome)
    }

    /// Computes one immutable bounded semantic delta between exact generations.
    #[wasm_bindgen]
    pub async fn diff(
        &self,
        from: &BrowserGeneration,
        to: &BrowserGeneration,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_changes: JsValue,
    ) -> Result<BrowserChangeSet, JsValue> {
        let maximum_changes = wasm_u32(&maximum_changes)?;

        let engine = {
            let (workspace, from, to) = (&self.engine, &from.engine, &to.engine);
            workspace
                .diff(from, to, maximum_changes)
                .await
                .map_err(js_error)?
        };
        Ok(BrowserChangeSet { engine })
    }

    /// Builds one immutable side-effect-free plan for joining this workspace into a target.
    #[wasm_bindgen(js_name = joinInto)]
    pub async fn join_into(
        &self,
        target: &BrowserWorkspace,
        options: JsValue,
    ) -> Result<BrowserJoinPlan, JsValue> {
        let options: BrowserJoinOptions =
            serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let history = browser_join_history(&options.history)?;
        let engine = {
            let (source, target) = (&self.engine, &target.engine);
            source
                .join_into(target)
                .history(history)
                .bounds(
                    options.maximum_generations,
                    options.maximum_changes,
                    options.maximum_conflicts,
                )
                .plan_pinned(
                    browser_join_generation(
                        source,
                        options.source_generation.as_deref(),
                        "source generation",
                    )
                    .await?,
                    browser_join_generation(
                        target,
                        options.target_generation.as_deref(),
                        "target generation",
                    )
                    .await?,
                )
                .await
                .map_err(js_error)?
        };
        Ok(BrowserJoinPlan { engine })
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used as an Option::map adapter, which hands over the name by value"
)]
pub(in crate::bindings) fn browser_workspace_name_value(name: LogicalName) -> BrowserWorkspaceName {
    BrowserWorkspaceName {
        encoding: name.encoding().as_str().to_owned(),
        bytes: name.as_bytes().to_vec(),
    }
}

pub(in crate::bindings) fn browser_workspace_name(value: JsValue) -> Result<LogicalName, JsValue> {
    let value: BrowserWorkspaceName = serde_wasm_bindgen::from_value(value).map_err(js_error)?;
    let encoding = NameEncoding::from_public_str(&value.encoding)
        .ok_or_else(|| js_error("unknown name encoding"))?;
    LogicalName::new(encoding, value.bytes, u32::MAX).map_err(js_error)
}

pub(in crate::bindings) fn browser_workspace_metadata(
    value: WorkspaceMetadata,
) -> BrowserWorkspaceMetadata {
    BrowserWorkspaceMetadata {
        posix_mode: value.posix_mode,
        posix_uid: value.posix_uid,
        posix_gid: value.posix_gid,
        posix_flags: value.posix_flags,
        windows_attributes: value.windows_attributes,
        created_ns: value.created_ns,
        modified_ns: value.modified_ns,
        accessed_ns: value.accessed_ns,
        changed_ns: value.changed_ns,
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes the stat it projects into the browser shape"
)]
pub(in crate::bindings) fn browser_workspace_stat(value: WorkspaceStat) -> BrowserWorkspaceStat {
    BrowserWorkspaceStat {
        file_id: value.file_id.into_bytes().to_vec(),
        kind: value.kind,
        link_count: value.link_count,
        logical_bytes: value.logical_bytes,
        metadata: browser_workspace_metadata(value.metadata),
    }
}

pub(in crate::bindings) fn browser_workspace_directory_page(
    value: DirectoryPage,
) -> BrowserWorkspaceDirectoryPage {
    BrowserWorkspaceDirectoryPage {
        entries: value
            .entries
            .into_iter()
            .map(|entry| BrowserWorkspaceDirectoryEntry {
                name: BrowserWorkspaceName {
                    encoding: entry.name.encoding().as_str().to_owned(),
                    bytes: entry.name.as_bytes().to_vec(),
                },
                file_id: entry.file_id.into_bytes().to_vec(),
                kind: entry.kind,
            })
            .collect(),
        has_more: value.has_more,
    }
}

pub(in crate::bindings) fn browser_workspace_extent_plan(
    value: WorkspaceExtentPlan,
) -> BrowserWorkspaceExtentPlan {
    BrowserWorkspaceExtentPlan {
        spans: value
            .spans
            .into_iter()
            .map(|span| BrowserWorkspaceExtentSpan {
                offset: span.offset,
                length: span.length,
                source_end: span.source_end,
                kind: match span.kind {
                    WorkspaceExtentKind::Hole => "hole",
                    WorkspaceExtentKind::AllocatedZero => "allocated-zero",
                    WorkspaceExtentKind::Content => "content",
                },
            })
            .collect(),
    }
}

pub(in crate::bindings) fn browser_workspace_rebase_result<A, O>(
    outcome: WorkspaceRebase<A, O>,
) -> BrowserWorkspaceRebaseResult {
    let generation = |status, generation: Generation<A, O>| BrowserWorkspaceRebaseResult {
        status,
        generation_id: Some(serde_bytes::ByteBuf::from(
            generation.id().digest().into_bytes().to_vec(),
        )),
        conflicts: Vec::new(),
        truncated: false,
    };
    match outcome {
        WorkspaceRebase::Rebased(value) => generation("rebased", value),
        WorkspaceRebase::AlreadyRebased(value) => generation("already-rebased", value),
        WorkspaceRebase::Current(value) => generation("current", value),
        WorkspaceRebase::Stale(value) => generation("stale", value),
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => BrowserWorkspaceRebaseResult {
            status: "conflicted",
            generation_id: None,
            conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
            truncated,
        },
        WorkspaceRebase::Fenced => BrowserWorkspaceRebaseResult {
            status: "fenced",
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::IdempotencyConflict => BrowserWorkspaceRebaseResult {
            status: "idempotency-conflict",
            generation_id: None,
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}
