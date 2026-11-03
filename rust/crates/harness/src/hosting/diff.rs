//! The one producer of workspace diff pages. A platform's
//! [`super::Platform::diff`] only selects the agent's volume, its fork base and
//! its current generation, and passes its authorized reader and its retained
//! [`DiffPages`]; projection, authority over disclosed paths and exact paging
//! are shared.

use super::{AgentPageRequest, AgentReply, Error, PageCredit};
use crate::{
    client::{DiffCursor, DiffFilePage, DiffLimits, DiffView},
    conversation::VolumeRef,
    filesystem::FilesystemContentVerifier,
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamError;
use std::sync::Arc;

/// Bytes read from either endpoint of one changed file. A longer file is
/// listed with its length rather than read or rendered.
pub const DIFF_FILE_BYTES: u64 = 64 * 1024;

/// The one retained projection a platform pages an agent's diff from: the
/// last requested volume, fork base, current generation and work credit.
/// Replacing it on any change bounds what is retained to one projection,
/// itself bounded by its credit. A later generation is a new projection whose
/// digest refuses the older cursors with [`crate::Error::Conflict`].
/// A platform keeps one per reader authority it pages with, so a projection
/// is only ever paged under the authority whose listing it holds.
#[derive(Default)]
pub struct DiffPages(std::sync::Mutex<Option<Arc<Projection>>>);

struct Projection {
    volume: VolumeRef,
    base: GenerationRef,
    current: GenerationRef,
    work: u32,
    view: DiffView,
}

impl DiffPages {
    /// One page of `volume`'s changes from the fork `base` to `current`, as
    /// far as `reader`'s authority may list and read them, after `page`'s
    /// cursor.
    ///
    /// Rows are the page's items: a file header and each patch line spend
    /// one. Fragments fill the page's byte credit, a long patch splitting at
    /// a UTF-8 boundary. The returned cursor is exact: it binds the volume,
    /// base and projection of `current`, so a later generation refuses it
    /// rather than mixing positions of two projections.
    /// The byte credit also funds input and comparison steps. Expensive
    /// comparisons retain the file's authenticated length as an explicit
    /// content omission. Changing that credit changes the projection, so its
    /// old cursor refuses.
    ///
    /// Only the first page of a projection enumerates and compares changes.
    /// A continuation pages the retained projection, and discloses its paths
    /// only after `reader` may still list each of them at the endpoints that
    /// hold it; otherwise the projection is rebuilt under current authority.
    ///
    /// # Errors
    /// Missing authority, an invalid or stale cursor, or a byte credit too
    /// small for one fragment.
    pub async fn page<A, O>(
        &self,
        reader: &FilesystemContentVerifier<A, O>,
        volume: &VolumeRef,
        (base, current): (&GenerationRef, &GenerationRef),
        page: &AgentPageRequest,
    ) -> crate::Result<Vec<DiffFilePage>>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let cursor = page
            .cursor()
            .map(serde_json::from_str::<DiffCursor>)
            .transpose()
            .map_err(|_| crate::Error::Invalid("invalid diff page position".into()))?;
        let retained = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(|projection| {
                (
                    &projection.volume,
                    &projection.base,
                    &projection.current,
                    projection.work,
                ) == (volume, base, current, page.bytes())
            });
        if let Some(projection) = retained {
            let items = fill(&projection.view, cursor.as_ref(), page)?;
            let paths = items.iter().map(|item| (item.path.as_str(), item.kind));
            if reader
                .diff_disclosure(volume, (base, current), paths)
                .await?
            {
                return Ok(items);
            }
        }
        let publication = reader.diff_publication(volume, base, current).await?;
        let mut view = DiffView::new(volume.clone(), base.clone())?;
        view.apply(
            &publication,
            reader,
            DiffLimits {
                files: usize::MAX,
                changes: usize::MAX,
                file_bytes: DIFF_FILE_BYTES,
                render_bytes: usize::MAX,
                work: page.bytes() as usize,
            },
        )
        .await?;
        let items = fill(&view, cursor.as_ref(), page);
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(Projection {
            volume: volume.clone(),
            base: base.clone(),
            current: current.clone(),
            work: page.bytes(),
            view,
        }));
        items
    }
}

/// The fragments of `view` after `cursor` that fill `page`'s rows and byte
/// credit. Work is bounded by the returned fragments.
fn fill(
    view: &DiffView,
    cursor: Option<&DiffCursor>,
    page: &AgentPageRequest,
) -> crate::Result<Vec<DiffFilePage>> {
    let mut credit =
        PageCredit::new(page, |_| AgentReply::Diff { items: Vec::new() }).map_err(credit_error)?;
    let mut rows = page.items() as usize;
    let mut patch_bytes = page.bytes() as usize;
    let mut cursor = cursor.cloned();
    let mut items = Vec::new();
    while rows != 0 {
        let Some(fragment) = view.page(cursor.as_ref(), rows, patch_bytes)? else {
            break;
        };
        match credit.accept(&fragment, None) {
            Ok(true) => {}
            // Keep the accepted prefix and its continuation. Shrinking another
            // fragment must never discard rows already funded by this page.
            Ok(false) => break,
            Err(Error::Stream(StreamError::Capacity)) if fragment.patch.len() > 1 => {
                patch_bytes = fragment.patch.len() / 2;
                continue;
            }
            Err(error) => return Err(credit_error(error)),
        }
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
