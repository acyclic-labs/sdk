use super::*;

/// Customer-visible sparse representation class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceExtentKind {
    /// Unallocated logical bytes.
    Hole,
    /// Physically represented zeros.
    AllocatedZero,
    /// Immutable content bytes.
    Content,
}

/// One clipped sparse logical span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceExtentSpan {
    /// Inclusive logical offset.
    pub offset: u64,
    /// Positive span length.
    pub length: u64,
    /// Exclusive end of the complete source extent.
    pub source_end: u64,
    /// Physical representation class without backend identity.
    pub kind: WorkspaceExtentKind,
}

/// Bounded sparse plan without storage topology or object identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceExtentPlan {
    /// Ordered contiguous spans covering the request.
    pub spans: Vec<WorkspaceExtentSpan>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Plans clipped sparse spans without reading file bodies.
    pub async fn plan_extents(&self, path: &str, offset: u64, length: u64, maximum_spans: u32) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(self, GenerationSelector::Head, path, offset, length, maximum_spans).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Plans clipped sparse spans from this immutable generation.
    pub async fn plan_extents(&self, path: &str, offset: u64, length: u64, maximum_spans: u32) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(&self.workspace, GenerationSelector::Exact(self.id), path, offset, length, maximum_spans).await
    }
}


async fn plan<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    maximum_spans: u32,
) -> Result<WorkspaceExtentPlan, WorkspaceError> {
    let mut checkout = workspace
        .checkout(selector, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await.map_err(|failure| failure.error)?.value;
    let path = customer_path(path, checkout.volume_config())?;
    let plan = checkout
        .plan_file_extents(
            &path,
            crate::ByteRange { offset, length },
            maximum_spans,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?
        .value;
    let spans = match plan {
        Some(plan) => plan
            .spans
            .into_iter()
            .map(|span| WorkspaceExtentSpan {
                offset: span.offset,
                length: span.length,
                source_end: span.source_end,
                kind: match span.kind {
                    ExtentKind::Hole => WorkspaceExtentKind::Hole,
                    ExtentKind::AllocatedZero => WorkspaceExtentKind::AllocatedZero,
                    ExtentKind::Content { .. } => WorkspaceExtentKind::Content,
                },
            })
            .collect(),
        None if length == 0 => Vec::new(),
        None => vec![WorkspaceExtentSpan {
            offset,
            length,
            source_end: offset
                .checked_add(length)
                .ok_or_else(|| WorkspaceError::Engine("file range overflows".to_owned()))?,
            kind: WorkspaceExtentKind::Content,
        }],
    };
    Ok(WorkspaceExtentPlan { spans })
}
