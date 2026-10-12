use super::*;

/// One sparse data/hole boundary lookup.
#[napi(object)]
pub struct NativeExtentSeek {
    /// Exact logical offset, or absence when no matching extent remains.
    pub offset: Option<BigInt>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One exact logical sparse span.
#[napi(object)]
pub struct NativeExtentSpan {
    /// Physical span class.
    pub kind: String,
    /// Inclusive logical file offset.
    pub offset: BigInt,
    /// Positive logical span length.
    pub length: BigInt,
    /// Exclusive end of the complete canonical source extent.
    pub source_end: BigInt,
    /// Typed immutable object identity for content spans.
    pub object_id: Option<Buffer>,
    /// Byte offset within the immutable object for content spans.
    pub object_offset: Option<BigInt>,
}

/// Bounded sparse range plan without file-body reads.
#[napi(object)]
pub struct NativeExtentPlan {
    /// `inline` for embedded tiny files or `sparse` for an extent plan.
    pub kind: String,
    /// Exact ordered spans for a sparse file.
    pub spans: Vec<NativeExtentSpan>,
    /// Logical allocation retained by the span vector.
    pub retained_allocation_bytes: Option<BigInt>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    /// Plans one bounded sparse range without reading file content blobs.
    #[napi]
    pub async fn plan_file_extents(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        maximum_spans: NapiU32,
    ) -> Result<NativeExtentPlan> {
        let maximum_spans = maximum_spans.into_inner();

        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            plan_file_extents(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
                maximum_spans,
            )
        )?;
        native_extent_plan(receipt)
    }

    /// Plans one bounded sparse range by stable file identity.
    #[napi]
    pub async fn plan_file_extents_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        maximum_spans: NapiU32,
    ) -> Result<NativeExtentPlan> {
        let maximum_spans = maximum_spans.into_inner();

        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            plan_file_extents_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
                maximum_spans,
            )
        )?;
        native_extent_plan(receipt)
    }

    /// Finds the next sparse data or hole boundary without reading file bodies.
    #[napi]
    pub async fn seek_file_extent(
        &self,
        path: String,
        offset: BigInt,
        target: String,
    ) -> Result<NativeExtentSeek> {
        let path = native_path(&path, self.config)?;
        let target = extent_seek_target(&target)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            seek_file_extent(&path, bigint_u64(&offset, "offset")?, target,)
        )?;
        Ok(NativeExtentSeek {
            offset: receipt.value.map(bigint),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Finds the next sparse boundary by stable file identity.
    #[napi]
    pub async fn seek_file_extent_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        target: String,
    ) -> Result<NativeExtentSeek> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let target = extent_seek_target(&target)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            seek_file_extent_by_id(file_id, bigint_u64(&offset, "offset")?, target,)
        )?;
        Ok(NativeExtentSeek {
            offset: receipt.value.map(bigint),
            work_json: work_json(&receipt.work)?,
        })
    }
}

pub(in crate::bindings) fn native_extent_span(
    span: &acyclic_fs::kernel::ExtentSlice,
) -> NativeExtentSpan {
    let (kind, object_id, object_offset) = match &span.kind {
        ExtentKind::Hole => ("hole", None, None),
        ExtentKind::AllocatedZero => ("allocated-zero", None, None),
        ExtentKind::Content {
            object,
            object_offset,
        } => (
            "content",
            Some(encode_object_id(*object)),
            Some(bigint(*object_offset)),
        ),
    };
    NativeExtentSpan {
        kind: kind.to_owned(),
        offset: bigint(span.offset),
        length: bigint(span.length),
        source_end: bigint(span.source_end),
        object_id,
        object_offset,
    }
}

pub(in crate::bindings) fn native_extent_plan(
    receipt: acyclic_fs::FsReceipt<Option<acyclic_fs::kernel::ExtentPlan>>,
) -> Result<NativeExtentPlan> {
    let work_json = work_json(&receipt.work)?;
    let (kind, spans, retained_allocation_bytes) = match receipt.value {
        None => ("inline", Vec::new(), None),
        Some(plan) => (
            "sparse",
            plan.spans.iter().map(native_extent_span).collect(),
            Some(bigint(plan.retained_allocation_bytes)),
        ),
    };
    Ok(NativeExtentPlan {
        kind: kind.to_owned(),
        spans,
        retained_allocation_bytes,
        work_json,
    })
}

pub(in crate::bindings) fn extent_seek_target(target: &str) -> Result<ExtentSeekTarget> {
    match target {
        "data" => Ok(ExtentSeekTarget::Data),
        "hole" => Ok(ExtentSeekTarget::Hole),
        _ => Err(Error::new(Status::InvalidArg, "unknown sparse seek target")),
    }
}
