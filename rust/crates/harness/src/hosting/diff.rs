//! Shared bounded paging of one authorized file projection.
use super::{AgentPageRequest, AgentReply, Error, PageCredit};
use crate::client::{DiffCursor, DiffFilePage, DiffView};
use acyclic_stream::StreamError;

/// Endpoint bytes admitted for one file comparison; larger files are explicit omissions.
pub const DIFF_FILE_BYTES: u64 = 64 * 1024;

impl DiffView {
    /// The retained projection's fragments after `cursor`, within `page`'s rows
    /// and canonical wire credit. This performs no filesystem reads. The caller
    /// must authorize disclosure of the returned paths before returning them.
    ///
    /// # Errors
    /// An invalid cursor or insufficient credit for the next UTF-8 scalar and
    /// its metadata envelope.
    pub fn agent_page(
        &self,
        cursor: Option<&DiffCursor>,
        page: &AgentPageRequest,
    ) -> crate::Result<Vec<DiffFilePage>> {
        let mut credit = PageCredit::new(page, |_| AgentReply::Diff { items: Vec::new() })
            .map_err(credit_error)?;
        let mut rows = page.items() as usize;
        let mut patch_bytes = page.bytes() as usize;
        let mut cursor = cursor.cloned();
        let mut items = Vec::new();
        while rows != 0 {
            let Some(fragment) = self.page(cursor.as_ref(), rows, patch_bytes)? else {
                break;
            };
            let scalar_bytes = fragment.patch.chars().next().map_or(0, char::len_utf8);
            match credit.accept(&fragment, None) {
                Ok(true) => {}
                // Keep the accepted prefix and its continuation. Shrinking another
                // fragment must never discard rows already funded by this page.
                Ok(false) => break,
                Err(Error::Stream(StreamError::Capacity))
                    if scalar_bytes != 0 && scalar_bytes < fragment.patch.len() =>
                {
                    // The smallest retry must still contain the next scalar. A
                    // byte midpoint can fall below a four-byte scalar even when
                    // that scalar and its actual wire envelope fit the credit.
                    patch_bytes = (fragment.patch.len() / 2).max(scalar_bytes);
                    continue;
                }
                Err(error) => return Err(credit_error(error)),
            }
            let spent =
                usize::from(!fragment.continued) + fragment.patch.split_inclusive('\n').count();
            rows = rows.saturating_sub(spent);
            cursor = fragment.next.clone();
            items.push(fragment);
            patch_bytes = page.bytes() as usize;
            if cursor.is_none() {
                break;
            }
        }
        Ok(items)
    }
}

fn credit_error(error: Error) -> crate::Error {
    match error {
        Error::Stream(error) => error.into(),
        error => crate::Error::Invalid(error.to_string()),
    }
}

mod source;
pub use source::DiffSource;
pub(in crate::hosting) use source::frame;
