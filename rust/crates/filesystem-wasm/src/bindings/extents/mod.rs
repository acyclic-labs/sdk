use super::*;

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserExtentSeekResult {
    pub(in crate::bindings) offset: Option<u64>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserExtentSpanResult {
    #[tsify(type = "\"hole\" | \"allocated-zero\" | \"content\"")]
    pub(in crate::bindings) kind: &'static str,
    pub(in crate::bindings) offset: u64,
    pub(in crate::bindings) length: u64,
    pub(in crate::bindings) source_end: u64,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) object_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) object_offset: Option<u64>,
}

#[derive(Serialize, Tsify)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
#[tsify(large_number_types_as_bigints)]
pub enum BrowserExtentPlanResult {
    Inline {
        work: BrowserWorkCounters,
    },
    Sparse {
        spans: Vec<BrowserExtentSpanResult>,
        retained_allocation_bytes: u64,
        work: BrowserWorkCounters,
    },
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = planFileExtents)]
    pub async fn plan_file_extents(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_spans: JsValue,
    ) -> Result<Ts<BrowserExtentPlanResult>, JsValue> {
        let maximum_spans = wasm_u32(&maximum_spans)?;

        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            plan_file_extents(&path, ByteRange { offset, length }, maximum_spans,)
        )?;
        let result = match receipt.value {
            None => BrowserExtentPlanResult::Inline {
                work: browser_work(receipt.work),
            },
            Some(plan) => BrowserExtentPlanResult::Sparse {
                spans: plan
                    .spans
                    .iter()
                    .map(encode_extent_span)
                    .collect::<Result<_, _>>()?,
                retained_allocation_bytes: plan.retained_allocation_bytes,
                work: browser_work(receipt.work),
            },
        };
        ts(result)
    }

    #[wasm_bindgen(js_name = planFileExtentsById)]
    pub async fn plan_file_extents_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        length: u64,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_spans: JsValue,
    ) -> Result<Ts<BrowserExtentPlanResult>, JsValue> {
        let maximum_spans = wasm_u32(&maximum_spans)?;

        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(
            self,
            plan_file_extents_by_id(file_id, ByteRange { offset, length }, maximum_spans,)
        )?;
        let result = match receipt.value {
            None => BrowserExtentPlanResult::Inline {
                work: browser_work(receipt.work),
            },
            Some(plan) => BrowserExtentPlanResult::Sparse {
                spans: plan
                    .spans
                    .iter()
                    .map(encode_extent_span)
                    .collect::<Result<_, _>>()?,
                retained_allocation_bytes: plan.retained_allocation_bytes,
                work: browser_work(receipt.work),
            },
        };
        ts(result)
    }

    #[wasm_bindgen(js_name = seekFileExtent)]
    pub async fn seek_file_extent(
        &mut self,
        path: String,
        offset: u64,
        target: String,
    ) -> Result<Ts<BrowserExtentSeekResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let target = extent_seek_target(&target)?;
        let receipt = browser_call!(self, seek_file_extent(&path, offset, target,))?;
        ts(BrowserExtentSeekResult {
            offset: receipt.value,
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = seekFileExtentById)]
    pub async fn seek_file_extent_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        target: String,
    ) -> Result<Ts<BrowserExtentSeekResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let target = extent_seek_target(&target)?;
        let receipt = browser_call!(self, seek_file_extent_by_id(file_id, offset, target,))?;
        ts(BrowserExtentSeekResult {
            offset: receipt.value,
            work: browser_work(receipt.work),
        })
    }
}

pub(in crate::bindings) fn encode_extent_span(
    span: &acyclic_fs::kernel::ExtentSlice,
) -> Result<BrowserExtentSpanResult, JsValue> {
    let (kind, object_id, object_offset) = match &span.kind {
        ExtentKind::Hole => ("hole", None, None),
        ExtentKind::AllocatedZero => ("allocated-zero", None, None),
        ExtentKind::Content {
            object,
            object_offset,
        } => (
            "content",
            Some(encode_object_id(*object)),
            Some(*object_offset),
        ),
    };
    Ok(BrowserExtentSpanResult {
        kind,
        offset: span.offset,
        length: span.length,
        source_end: span.source_end,
        object_id: object_id.map(Into::into),
        object_offset,
    })
}

pub(in crate::bindings) fn extent_seek_target(target: &str) -> Result<ExtentSeekTarget, JsValue> {
    match target {
        "data" => Ok(ExtentSeekTarget::Data),
        "hole" => Ok(ExtentSeekTarget::Hole),
        _ => Err(js_error("unknown sparse seek target")),
    }
}
