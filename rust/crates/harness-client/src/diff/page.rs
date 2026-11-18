//! Visible output from an already authenticated diff projection.

use super::DiffView;
use crate::{Error, Result};
use std::ops::Bound::{Excluded, Unbounded};

pub use crate::context::{DiffCursor, DiffFilePage};

impl DiffView {
    /// Returns one visible file fragment. The first fragment spends one row on
    /// its header; subsequent fragments spend all rows on patch text. A long
    /// line splits at a UTF-8 boundary instead of exceeding `patch_bytes`.
    ///
    /// Both allowances are caller-selected page sizes, not total query limits.
    /// Every successful nonterminal page advances its position, including a
    /// header-only page. Insufficient space for one UTF-8 scalar refuses.
    /// Work is at most two ordered file lookups plus returned text; there is no IO,
    /// hashing, full-view clone, or traversal of earlier pages. The cursor binds
    /// the retained publication history and its content omission decisions.
    pub fn page(
        &self,
        cursor: Option<&DiffCursor>,
        rows: usize,
        patch_bytes: usize,
    ) -> Result<Option<DiffFilePage>> {
        if rows == 0 || patch_bytes == 0 {
            return Err(Error::Invalid("diff page requires output credit".into()));
        }
        if let Some(cursor) = cursor
            && Some(cursor.publication) != self.projection_digest
        {
            return Err(Error::Conflict("diff page projection changed".into()));
        }
        let entry = match cursor {
            Some(cursor) => Some((
                &cursor.path,
                self.files.get(&cursor.path).ok_or_else(invalid_cursor)?,
            )),
            None => self.files.first_key_value(),
        };
        let Some((path, file)) = entry else {
            return Ok(None);
        };
        let publication = self.projection_digest.ok_or_else(invalid_cursor)?;
        let offset = cursor.and_then(|cursor| cursor.offset);
        let continued = offset.is_some();
        let start = offset.unwrap_or(0);
        if start > file.patch.len()
            || !file.patch.is_char_boundary(start)
            || (continued && start == file.patch.len())
        {
            return Err(invalid_cursor());
        }
        let mut end = start;
        let available_rows = rows - usize::from(!continued);
        if available_rows != 0 && start < file.patch.len() {
            let tail = &file.patch[start..];
            let mut maximum = patch_bytes.min(tail.len());
            while !tail.is_char_boundary(maximum) {
                maximum -= 1;
            }
            // Bound the searched slice before looking for a newline: a single
            // enormous line must not make a small visible page scan its tail.
            for line in tail[..maximum].split_inclusive('\n').take(available_rows) {
                end += line.len();
            }
            if end == start {
                return Err(Error::Invalid(
                    "diff page cannot fit the next UTF-8 character".into(),
                ));
            }
        }
        let position = |path: String, offset| DiffCursor {
            publication,
            path,
            offset,
        };
        let next = if end < file.patch.len() {
            Some(position(path.clone(), Some(end)))
        } else {
            self.files
                .range::<str, _>((Excluded(path.as_str()), Unbounded))
                .next()
                .map(|(path, _)| position(path.clone(), None))
        };
        Ok(Some(DiffFilePage {
            publication,
            path: path.clone(),
            kind: file.kind,
            added: file.added,
            removed: file.removed,
            binary: file.binary,
            omitted_bytes: file.omitted_bytes,
            continued,
            patch: file.patch[start..end].to_owned(),
            next,
        }))
    }
}

fn invalid_cursor() -> Error {
    Error::Invalid("invalid diff page position".into())
}
