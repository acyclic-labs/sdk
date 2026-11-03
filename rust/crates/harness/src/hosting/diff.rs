//! The one producer of workspace diff pages. A platform's
//! [`super::Platform::diff`] only selects the agent's volume, its fork base and
//! its current generation, and passes its authorized reader; projection,
//! authority over disclosed paths and exact paging are shared.

use super::{AgentPageRequest, AgentReply, Error, PageCredit};
use crate::{
    client::{DiffCursor, DiffFilePage, DiffLimits, DiffView},
    conversation::VolumeRef,
    filesystem::FilesystemContentVerifier,
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamError;

/// Bytes read from either endpoint of one changed file. A longer file is
/// listed with its length rather than read or rendered.
pub const DIFF_FILE_BYTES: u64 = 64 * 1024;

/// Only the per-file read is bounded; the view retains every changed file.
const LIMITS: DiffLimits = DiffLimits {
    files: usize::MAX,
    changes: usize::MAX,
    file_bytes: DIFF_FILE_BYTES,
    render_bytes: usize::MAX,
    work: usize::MAX,
};

/// One page of `volume`'s changes from the fork `base` to `current`, as far
/// as `reader`'s authority may list and read them, after `page`'s cursor.
///
/// Rows are the page's items: a file header and each patch line spend one.
/// Fragments fill the page's byte credit, a long patch splitting at a UTF-8
/// boundary. The returned cursor is exact: it binds the volume, base and
/// projection of `current`, so a later generation refuses it rather than
/// mixing positions of two projections.
///
/// # Errors
/// Missing authority, an invalid or stale cursor, or a byte credit too small
/// for one fragment.
pub async fn diff_page<A, O>(
    reader: &FilesystemContentVerifier<A, O>,
    volume: &VolumeRef,
    (base, current): (&GenerationRef, &GenerationRef),
    page: &AgentPageRequest,
) -> crate::Result<Vec<DiffFilePage>>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let mut cursor = page
        .cursor()
        .map(serde_json::from_str::<DiffCursor>)
        .transpose()
        .map_err(|_| crate::Error::Invalid("invalid diff page position".into()))?;
    let publication = reader.diff_publication(volume, base, current).await?;
    let mut view = DiffView::new(volume.clone(), base.clone())?;
    view.apply(&publication, reader, LIMITS).await?;
    let mut credit =
        PageCredit::new(page, |_| AgentReply::Diff { items: Vec::new() }).map_err(credit_error)?;
    let mut rows = page.items() as usize;
    let mut patch_bytes = page.bytes() as usize;
    let mut items = Vec::new();
    while rows != 0 {
        let Some(fragment) = view.page(cursor.as_ref(), rows, patch_bytes)? else {
            break;
        };
        if credit
            .admits(&fragment, None)
            .map_err(credit_error)?
            .is_none()
        {
            // Retry the same position with a shorter patch fragment.
            patch_bytes = fragment.patch.len() / 2;
            if patch_bytes != 0 {
                continue;
            }
            if items.is_empty() {
                return Err(StreamError::Capacity.into());
            }
            break;
        }
        credit.accept(&fragment, None).map_err(credit_error)?;
        let spent = usize::from(!fragment.continued) + fragment.patch.split_inclusive('\n').count();
        rows = rows.saturating_sub(spent);
        cursor = fragment.next.clone();
        items.push(fragment);
        if cursor.is_none() {
            break;
        }
    }
    Ok(items)
}

fn credit_error(error: Error) -> crate::Error {
    match error {
        Error::Stream(error) => error.into(),
        error => crate::Error::Invalid(error.to_string()),
    }
}
