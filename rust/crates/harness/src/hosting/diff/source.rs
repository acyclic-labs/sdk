//! One query owns one native changed-path frontier and one file comparison.
use super::*;
use acyclic_harness_client::{DiffProjection, DiffSnapshot, DiffSummary};
use acyclic_stream::BoxProviderStream;
use futures::TryStreamExt as _;

/// Pinned workspace selection and its original authorized reader. Platforms
/// select endpoints; this shared owner performs every traversal and comparison.
pub struct DiffSource(Box<dyn Source>);

trait Source: acyclic_stream::ProviderPlatform {
    fn frames(
        self: Box<Self>,
        page: AgentPageRequest,
        projection: DiffProjection,
    ) -> BoxProviderStream<'static, crate::Result<bytes::Bytes>>;
}

struct FilesystemSource<A: 'static, O: 'static> {
    reader: Arc<FilesystemContentVerifier<A, O>>,
    snapshot: DiffSnapshot,
}

impl DiffSource {
    /// Retain an immutable selection without reading its changed-path suffix.
    pub fn new<A, O>(reader: Arc<FilesystemContentVerifier<A, O>>, snapshot: DiffSnapshot) -> Self
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        Self(Box::new(FilesystemSource { reader, snapshot }))
    }

    pub(in crate::hosting) fn frames(
        self,
        page: AgentPageRequest,
        projection: DiffProjection,
    ) -> BoxProviderStream<'static, crate::Result<bytes::Bytes>> {
        self.0.frames(page, projection)
    }
}

impl<A, O> Source for FilesystemSource<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Credited frames from this one source. A file projection is discarded
    /// after its last fragment; no complete path set or patch set is retained.
    fn frames(
        self: Box<Self>,
        page: AgentPageRequest,
        projection: DiffProjection,
    ) -> BoxProviderStream<'static, crate::Result<bytes::Bytes>> {
        Box::pin(async_stream::try_stream! {
            self.reader.diff_disclosure(&self.snapshot.volume, (&self.snapshot.base, &self.snapshot.current), std::iter::empty()).await?;
            yield json_frame(1, &self.snapshot, &page)?;
            let paths = self.reader.diff_steps(&self.snapshot.volume, &self.snapshot.base, &self.snapshot.current);
            futures::pin_mut!(paths);
            let mut summary = DiffSummary::default();
            while let Some(path) = paths.try_next().await? {
                let Some(path) = path else {
                    yield bytes::Bytes::from_static(&[4]);
                    continue;
                };
                let path_name = path.path.clone();
                let mut view = DiffView::new(self.snapshot.volume.clone(), self.snapshot.base.clone())?;
                view.apply(&crate::context::DiffPublication {
                    previous: self.snapshot.base.clone(),
                    generation: self.snapshot.current.clone(),
                    paths: vec![path],
                }, self.reader.as_ref(), DiffLimits {
                    files: 1, changes: 1, file_bytes: DIFF_FILE_BYTES,
                    render_bytes: usize::MAX, work: page.bytes() as usize,
                }).await?;
                let Some(file) = view.files().values().next() else {
                    yield bytes::Bytes::from_static(&[4]);
                    continue;
                };
                let add = |left: u64, right: u64| left.checked_add(right)
                    .ok_or_else(|| crate::Error::Invalid("diff totals overflow".into()));
                summary.files = add(summary.files, 1)?;
                summary.added = add(summary.added, file.added)?;
                summary.removed = add(summary.removed, file.removed)?;
                summary.omitted = add(summary.omitted, u64::from(file.omitted_bytes.is_some()))?;
                let kind = file.kind;
                let disclose = async || -> crate::Result<()> {
                    if !self.reader.diff_disclosure(&self.snapshot.volume, (&self.snapshot.base, &self.snapshot.current), [(path_name.as_str(), kind)]).await? {
                        return Err(crate::Error::Unauthorized("diff disclosure is no longer authorized".into()));
                    }
                    Ok(())
                };
                if projection == DiffProjection::Summary {
                    disclose().await?;
                    yield json_frame(3, &summary, &page)?;
                    continue;
                }
                let mut cursor = None;
                loop {
                    let request = AgentPageRequest::new(page.items(), page.bytes(),
                        cursor.as_ref().map(serde_json::to_string).transpose()
                            .map_err(|error| crate::Error::Invalid(error.to_string()))?)
                        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
                    let items = view.agent_page(cursor.as_ref(), &request)?;
                    cursor = items.last().and_then(|item| item.next.clone());
                    let command = acyclic_harness_client::AgentCommand::Diff {
                        agent: self.snapshot.agent.clone(), page: request,
                    };
                    let body = acyclic_harness_client::encode_agent_reply(&Ok(AgentReply::Diff { items }), &command)
                        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
                    disclose().await?;
                    yield frame(2, body, &page)?;
                    if cursor.is_none() { break; }
                }
            }
        })
    }
}

fn json_frame(
    kind: u8,
    value: &impl serde::Serialize,
    page: &AgentPageRequest,
) -> crate::Result<bytes::Bytes> {
    let size = super::super::page_json_bytes(value).map_err(credit_error)?;
    if size > u64::from(page.bytes()) {
        return Err(acyclic_stream::StreamError::Capacity.into());
    }
    let mut body = Vec::with_capacity(size as usize + 1);
    body.push(kind);
    serde_json::to_writer(&mut body, value)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    Ok(body.into())
}

pub(in crate::hosting) fn frame(
    kind: u8,
    mut body: Vec<u8>,
    page: &AgentPageRequest,
) -> crate::Result<bytes::Bytes> {
    if body.len() > page.bytes() as usize {
        return Err(acyclic_stream::StreamError::Capacity.into());
    }
    body.insert(0, kind);
    Ok(body.into())
}
