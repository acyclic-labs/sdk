use super::*;

/// One exact immutable browser workspace generation.
#[wasm_bindgen]
pub struct BrowserGeneration {
    pub(in crate::bindings) engine: Generation<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

/// One immutable semantic delta between exact generations.
#[wasm_bindgen]
pub struct BrowserChangeSet {
    pub(in crate::bindings) engine: ChangeSet<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

/// One immutable, side-effect-free workspace join plan.
#[wasm_bindgen]
pub struct BrowserJoinPlan {
    pub(in crate::bindings) engine: JoinPlan<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

#[derive(Deserialize, Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserJoinOptions {
    pub(in crate::bindings) history: String,
    pub(in crate::bindings) maximum_generations: u32,
    pub(in crate::bindings) maximum_changes: u32,
    pub(in crate::bindings) maximum_conflicts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) source_generation: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) target_generation: Option<Vec<u8>>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserJoinResult {
    #[tsify(
        type = "\"applied\" | \"already-applied\" | \"no-changes\" | \"stale-target\" | \"conflicted\" | \"fenced\" | \"idempotency-conflict\""
    )]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) conflicts: Vec<MergeConflictResult>,
    pub(in crate::bindings) truncated: bool,
}

#[wasm_bindgen]
impl BrowserChangeSet {
    /// Exact immutable base endpoint.
    #[wasm_bindgen(getter)]
    pub fn from(&self) -> BrowserGeneration {
        let engine = self.engine.from().clone();
        BrowserGeneration { engine }
    }

    /// Exact immutable resulting endpoint.
    #[wasm_bindgen(getter)]
    pub fn to(&self) -> BrowserGeneration {
        let engine = self.engine.to().clone();
        BrowserGeneration { engine }
    }

    /// Stable path-independent records and namespace binding changes.
    pub fn changes(&self) -> Result<Ts<BrowserGenerationDiffResult>, JsValue> {
        let (changes, work) = (self.engine.changes().clone(), self.engine.work());
        encode_generation_diff(changes, work)
    }

    /// Composes contiguous immutable deltas by diffing their outer endpoints.
    #[wasm_bindgen]
    pub async fn compose(
        &self,
        next: &BrowserChangeSet,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_changes: JsValue,
    ) -> Result<BrowserChangeSet, JsValue> {
        let maximum_changes = wasm_u32(&maximum_changes)?;

        let engine = {
            let (value, next) = (&self.engine, &next.engine);
            value
                .compose(next, maximum_changes)
                .await
                .map_err(js_error)?
        };
        Ok(BrowserChangeSet { engine })
    }
}

#[wasm_bindgen]
impl BrowserJoinPlan {
    /// Target generation observed while planning.
    #[wasm_bindgen(getter, js_name = targetHead)]
    pub fn target_head(&self) -> Vec<u8> {
        self.engine.target_head().digest().into_bytes().to_vec()
    }

    /// Exact discovered common ancestor.
    #[wasm_bindgen(getter, js_name = commonAncestor)]
    pub fn common_ancestor(&self) -> Vec<u8> {
        self.engine.common_ancestor().digest().into_bytes().to_vec()
    }

    /// Applies this immutable plan through one exact target-head CAS.
    #[wasm_bindgen]
    pub async fn apply(
        &self,
        if_target: Vec<u8>,
        idempotency_key: Option<Vec<u8>>,
    ) -> Result<Ts<BrowserJoinResult>, JsValue> {
        let if_target = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &if_target,
            "target generation identity",
        )?));
        let idempotency_key = idempotency_key.map_or_else(
            || Ok(IdempotencyKey::new()),
            |value| fixed_16(&value).map(IdempotencyKey::from_bytes),
        )?;
        let options = ApplyOptions {
            if_target,
            idempotency_key,
        };
        let result = browser_join_result(
            self.engine
                .apply(options, acyclic_fs::PublicationPermit::Unrestricted)
                .await
                .map_err(js_error)?,
        );
        ts(result)
    }
}

#[wasm_bindgen]
impl BrowserGeneration {
    /// Content-addressed generation identity.
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> Vec<u8> {
        self.engine.id().digest().into_bytes().to_vec()
    }

    /// Owning opaque workspace identity.
    #[wasm_bindgen(getter, js_name = workspaceId)]
    pub fn workspace_id(&self) -> Vec<u8> {
        self.engine.workspace_id().into_bytes().to_vec()
    }

    /// Reads one complete file from this exact immutable state.
    #[wasm_bindgen]
    pub async fn read(&self, path: String, maximum_bytes: u64) -> Result<Vec<u8>, JsValue> {
        let bytes = sdk_value!(Box::pin(unbounded_future!(
            self.engine,
            read(&path, maximum_bytes)
        )))?;
        Ok(bytes.to_vec())
    }

    #[wasm_bindgen(js_name = readRange)]
    pub async fn read_range(
        &self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, JsValue> {
        let bytes = sdk_value!(unbounded_future!(
            self.engine,
            read_range(&path, offset, length)
        ))
        .map(|value| value.bytes)?;
        Ok(bytes.to_vec())
    }

    #[wasm_bindgen]
    pub async fn stat(&self, path: String) -> Result<Ts<BrowserWorkspaceStat>, JsValue> {
        let value = sdk_value!(unbounded_future!(self.engine, stat(&path)))?;
        ts(browser_workspace_stat(value))
    }

    #[wasm_bindgen(js_name = listDirectory)]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<JsValue>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_entries: JsValue,
    ) -> Result<Ts<BrowserWorkspaceDirectoryPage>, JsValue> {
        let maximum_entries = wasm_u32(&maximum_entries)?;

        let after = after.map(browser_workspace_name).transpose()?;
        let value = sdk_value!(unbounded_future!(
            self.engine,
            list_directory(&path, after.as_ref(), maximum_entries)
        ))
        .map(|value| value.0)?;
        ts(browser_workspace_directory_page(value))
    }

    #[wasm_bindgen(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self, path: String) -> Result<Vec<u8>, JsValue> {
        let value = sdk_value!(unbounded_future!(self.engine, read_symbolic_link(&path)))
            .map(|value| value.bytes)?;
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

    /// Retains this exact generation under one opaque identity.
    #[wasm_bindgen]
    pub async fn pin(&self, identity: String) -> Result<BrowserGeneration, JsValue> {
        let engine = sdk_value!(Box::pin(unbounded_future!(self.engine, pin(identity))))
            .map(|value| value.1)?;
        Ok(BrowserGeneration { engine })
    }
}

pub(in crate::bindings) async fn browser_join_generation<
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
>(
    workspace: &Workspace<A, O>,
    id: Option<&[u8]>,
    label: &str,
) -> Result<Generation<A, O>, JsValue> {
    match id {
        Some(id) => {
            workspace
                .generation(
                    acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(id, label)?)),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
        }
        None => {
            workspace
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
        }
    }
    .map(|receipt| receipt.value)
    .map_err(|failure| js_error(failure.error))
}

pub(in crate::bindings) fn browser_join_history(value: &str) -> Result<JoinHistory, JsValue> {
    match value {
        "merge" => Ok(JoinHistory::Merge),
        "rebase" => Ok(JoinHistory::Rebase),
        "squash" => Ok(JoinHistory::Squash),
        "cherry-pick" => Ok(JoinHistory::CherryPick),
        _ => Err(js_error(
            "join history must be merge, rebase, squash, or cherry-pick",
        )),
    }
}

pub(in crate::bindings) fn browser_join_result<A, O>(
    outcome: JoinOutcome<A, O>,
) -> BrowserJoinResult {
    let (status, generation) = match outcome {
        JoinOutcome::Applied(value) => ("applied", Some(value.generation)),
        JoinOutcome::AlreadyApplied(value) => ("already-applied", Some(value.generation)),
        JoinOutcome::NoChanges(value) => ("no-changes", Some(value)),
        JoinOutcome::StaleTarget(value) => ("stale-target", Some(value)),
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => {
            return BrowserJoinResult {
                status: "conflicted",
                generation_id: None,
                conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
                truncated,
            };
        }
        JoinOutcome::Fenced => ("fenced", None),
        JoinOutcome::IdempotencyConflict => ("idempotency-conflict", None),
    };
    BrowserJoinResult {
        status,
        generation_id: generation.map(|value| value.id().digest().into_bytes().to_vec().into()),
        conflicts: Vec::new(),
        truncated: false,
    }
}
