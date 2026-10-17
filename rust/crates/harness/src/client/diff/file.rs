use super::compute;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Captured file status, independent of any terminal renderer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    /// Path did not exist at the fork base.
    Added,
    /// Path no longer exists.
    Removed,
    /// Both endpoints exist and differ.
    Modified,
}

/// Exact line ranges for one unified hunk. Lines remain in the unified patch,
/// once, rather than being duplicated into another attachment or text store.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DiffHunk {
    /// One-based baseline start (zero for an insertion into an empty file).
    pub old_start: usize,
    /// Number of baseline lines covered.
    pub old_lines: usize,
    /// One-based resulting start.
    pub new_start: usize,
    /// Number of resulting lines covered.
    pub new_lines: usize,
}

/// Renderer-neutral file diff; binary changes are explicit and have no line counts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileDiff {
    /// Added, removed or modified.
    pub kind: DiffKind,
    /// At least one side is non-UTF-8 or contains NUL.
    pub binary: bool,
    /// Inserted lines across all hunks, excluding context lines.
    pub added: u64,
    /// Removed lines across all hunks, excluding context lines.
    pub removed: u64,
    /// Exact hunk ranges.
    pub hunks: Vec<DiffHunk>,
    /// Standard unified patch, including no-newline markers.
    pub patch: String,
}

impl FileDiff {
    /// Computes a file against its immutable base. `None` means the path returned
    /// to its baseline; adding/removing an empty file remains a visible change.
    /// Formatting stops before the patch exceeds `maximum_render_bytes`.
    /// `maximum_work` bounds input scanning and computation, including traceback.
    pub fn between(
        base: Option<&[u8]>,
        current: Option<&[u8]>,
        maximum_render_bytes: usize,
        mut maximum_work: usize,
    ) -> Result<Option<Self>> {
        compute::charge(
            &mut maximum_work,
            base.unwrap_or_default()
                .len()
                .checked_add(current.unwrap_or_default().len())
                .ok_or_else(compute::work_bound)?,
        )?;
        Self::between_work(base, current, maximum_render_bytes, &mut maximum_work)
    }

    pub(super) fn between_work(
        base: Option<&[u8]>,
        current: Option<&[u8]>,
        maximum_render_bytes: usize,
        work: &mut usize,
    ) -> Result<Option<Self>> {
        let before = base.unwrap_or_default();
        let after = current.unwrap_or_default();
        if base == current {
            return Ok(None);
        }
        let kind = match (base, current) {
            (None, _) => DiffKind::Added,
            (_, None) => DiffKind::Removed,
            _ => DiffKind::Modified,
        };
        let mut result = Self {
            kind,
            binary: true,
            added: 0,
            removed: 0,
            hunks: Vec::new(),
            patch: String::new(),
        };
        if before.contains(&0) || after.contains(&0) {
            return Ok(Some(result));
        }
        let (Ok(before), Ok(after)) = (std::str::from_utf8(before), std::str::from_utf8(after))
        else {
            return Ok(Some(result));
        };
        if before.len().abs_diff(after.len()) > maximum_render_bytes {
            return Err(render_bound());
        }
        result.binary = false;
        compute::text_diff(before, after, maximum_render_bytes, work, &mut result)?;
        Ok(Some(result))
    }
}

pub(super) struct PatchText {
    pub(super) text: String,
    pub(super) maximum: usize,
}

impl std::fmt::Write for PatchText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if text.len() > self.maximum - self.text.len() {
            return Err(std::fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

pub(super) fn render_bound() -> Error {
    Error::Invalid("diff view exceeds retained render bound".into())
}
