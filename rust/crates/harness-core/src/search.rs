//! Shared search adapter for original versioned content referenced by records.
//! It reuses the stream search frontier and the existing authorized content
//! reader. Neither constructing it nor an idle cursor reads or indexes data.
use crate::{
    Capabilities, Error, Result,
    conversation::{ContentResidencyVerifier, FileRef},
};
use acyclic_stream::{
    Record,
    search::{Matcher, Step, contains},
};
use serde::{Deserialize, Serialize};
mod files;
pub use files::{FileCursor, FileMatch, FilePage, FileRootKind};

fn source_window(offset: u64, pattern: &[u8]) -> acyclic_fs::ByteRange {
    let start = offset.saturating_sub(pattern.len().saturating_sub(1) as u64);
    acyclic_fs::ByteRange {
        offset: start,
        length: if pattern.is_empty() {
            0
        } else {
            offset - start + 64 * 1024
        },
    }
}

enum Window {
    Match(u64),
    More(u64),
    Done,
}

fn inspect_window(
    length: u64,
    offset: u64,
    pattern: &[u8],
    bytes: &[u8],
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Window> {
    if offset > length {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "search cursor exceeds its source",
        ));
    }
    if pattern.is_empty() {
        return Ok(Window::Match(0));
    }
    let range = source_window(offset, pattern);
    let end = offset.saturating_add(64 * 1024).min(length);
    if bytes.len() as u64 != end - range.offset {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "search range ended before its source cut",
        ));
    }
    if let Some(found) = acyclic_stream::search::find(bytes, pattern) {
        return Ok(Window::Match(range.offset + found as u64));
    }
    Ok(if end == length {
        Window::Done
    } else {
        Window::More(end)
    })
}

mod linked;
pub use linked::ContentMatcher;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
