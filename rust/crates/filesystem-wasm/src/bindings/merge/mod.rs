use super::*;

compat_json!(
    encode_merge_plan_json,
    encode_merge_plan_payload,
    encodeMergePlanJson,
    "Encodes a merge-plan payload in the versioned compatibility envelope."
);

compat_json!(
    decode_merge_plan_json,
    decode_merge_plan_payload,
    decodeMergePlanJson,
    "Decodes a versioned merge-plan envelope to its canonical payload."
);

compat_json!(
    encode_merge_candidate_json,
    encode_merge_candidate_payload,
    encodeMergeCandidateJson,
    "Encodes a merge-candidate payload in the versioned compatibility envelope."
);

compat_json!(
    decode_merge_candidate_json,
    decode_merge_candidate_payload,
    decodeMergeCandidateJson,
    "Decodes a versioned merge-candidate envelope to its canonical payload."
);

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserTreeEntryResult {
    pub(in crate::bindings) name: NameComponentResult,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) file_kind: FileKind,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserBindingChangeResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) directory_id: Vec<u8>,
    pub(in crate::bindings) name: NameComponentResult,
    pub(in crate::bindings) before: Option<BrowserTreeEntryResult>,
    pub(in crate::bindings) after: Option<BrowserTreeEntryResult>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserGenerationDiffResult {
    pub(in crate::bindings) files: Vec<BrowserFileRecordChangeResult>,
    pub(in crate::bindings) bindings: Vec<BrowserBindingChangeResult>,
    pub(in crate::bindings) truncated: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct MergeConflictResult {
    #[tsify(type = "\"file\" | \"binding\"")]
    pub(in crate::bindings) kind: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) file_id: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) directory_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) name: Option<NameComponentResult>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserMergePreparationResult {
    #[tsify(type = "\"prepared\" | \"conflicted\"")]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) conflicts: Vec<MergeConflictResult>,
    pub(in crate::bindings) truncated: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = prepareMerge)]
    pub async fn prepare_merge(
        &mut self,
        theirs: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_changes: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserMergePreparationResult>, JsValue> {
        let maximum_changes = wasm_u32(&maximum_changes)?;
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;

        let theirs = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &theirs,
            "merge generation identity",
        )?));
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .prepare_merge(
                theirs,
                maximum_changes,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        encode_merge_preparation(receipt.value, receipt.work)
    }
}

pub(in crate::bindings) fn encode_generation_diff(
    diff: acyclic_fs::GenerationDiff,
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserGenerationDiffResult>, JsValue> {
    ts(BrowserGenerationDiffResult {
        files: diff
            .files
            .into_iter()
            .map(|change| BrowserFileRecordChangeResult {
                file_id: change.file_id.into_bytes().to_vec(),
                before: change.before.map(encode_file_record),
                after: change.after.map(encode_file_record),
            })
            .collect(),
        bindings: diff
            .bindings
            .into_iter()
            .map(|change| BrowserBindingChangeResult {
                directory_id: change.directory_id.into_bytes().to_vec(),
                name: encode_name_component(&change.name),
                before: change.before.as_ref().map(encode_tree_entry),
                after: change.after.as_ref().map(encode_tree_entry),
            })
            .collect(),
        truncated: diff.truncated,
        work: browser_work(work),
    })
}

pub(in crate::bindings) fn encode_merge_preparation(
    preparation: MergePreparation,
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserMergePreparationResult>, JsValue> {
    let (status, generation, conflicts, truncated) = match preparation {
        MergePreparation::Prepared { generation_id } => {
            ("prepared", Some(generation_id), Vec::new(), false)
        }
        MergePreparation::Conflicted {
            conflicts,
            truncated,
        } => ("conflicted", None, conflicts, truncated),
    };
    ts(BrowserMergePreparationResult {
        status: status,
        generation_id: generation.map(|value| value.digest().into_bytes().to_vec().into()),
        conflicts: conflicts.into_iter().map(encode_merge_conflict).collect(),
        truncated,
        work: browser_work(work),
    })
}

pub(in crate::bindings) fn encode_merge_conflict(conflict: MergeConflict) -> MergeConflictResult {
    match conflict {
        MergeConflict::File(file_id) => MergeConflictResult {
            kind: "file",
            file_id: Some(serde_bytes::ByteBuf::from(file_id.into_bytes().to_vec())),
            directory_id: None,
            name: None,
        },
        MergeConflict::Binding { directory_id, name } => MergeConflictResult {
            kind: "binding",
            file_id: None,
            directory_id: Some(serde_bytes::ByteBuf::from(
                directory_id.into_bytes().to_vec(),
            )),
            name: Some(encode_name_component(&name)),
        },
    }
}
