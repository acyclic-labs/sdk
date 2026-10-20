//! Pure bounded three-way text merge shared by every filesystem consumer.

use std::borrow::Cow;

/// Result of one three-way UTF-8 merge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextMerge {
    /// Whether the result contains no conflict blocks.
    pub clean: bool,
    /// Complete merged content, including diff3 markers when conflicted.
    pub content: String,
}

/// Maximum input size accepted by [`merge_bytes`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteMergeLimits {
    /// Maximum bytes accepted for any one side.
    pub max_bytes: u64,
}

/// Why byte content cannot enter the text merge engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ByteMergeError {
    /// Content is not UTF-8 text or contains a NUL in its probe window.
    Binary,
    /// At least one side exceeds the configured byte limit.
    TooLarge,
}

/// Conflict produced by a bounded byte merge.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ByteConflictKind {
    /// Both sides changed overlapping text hunks.
    Hunks,
    /// Theirs deleted content that ours modified.
    TheirsDeleted,
    /// Ours deleted content that theirs modified.
    OursDeleted,
}

/// Result of merging optional bounded byte values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ByteMerge {
    /// Clean merged content.
    Merged(Vec<u8>),
    /// Marker-bearing conflict content.
    Conflicted {
        /// Complete content including conflict markers.
        bytes: Vec<u8>,
        /// Number of conflict blocks.
        hunks: u32,
        /// Shape of the conflict.
        kind: ByteConflictKind,
    },
}

const BINARY_PROBE_BYTES: usize = 8 * 1024;

fn bounded_text(bytes: &[u8], limits: ByteMergeLimits) -> Result<&str, ByteMergeError> {
    if bytes.len() as u64 > limits.max_bytes {
        return Err(ByteMergeError::TooLarge);
    }
    if bytes.iter().take(BINARY_PROBE_BYTES).any(|byte| *byte == 0) {
        return Err(ByteMergeError::Binary);
    }
    std::str::from_utf8(bytes).map_err(|_| ByteMergeError::Binary)
}

fn optional_text(
    bytes: Option<&[u8]>,
    limits: ByteMergeLimits,
) -> Result<Option<&str>, ByteMergeError> {
    bytes.map(|bytes| bounded_text(bytes, limits)).transpose()
}

fn modify_delete(
    base: &str,
    kept: &str,
    ours_label: &str,
    theirs_label: &str,
    kind: ByteConflictKind,
) -> ByteMerge {
    let base = with_trailing_newline(base);
    let kept = with_trailing_newline(kept);
    let (ours, theirs) = match kind {
        ByteConflictKind::TheirsDeleted => (kept.as_ref(), ""),
        ByteConflictKind::OursDeleted => ("", kept.as_ref()),
        ByteConflictKind::Hunks => unreachable!("modify/delete conflict cannot be hunk-only"),
    };
    ByteMerge::Conflicted {
        bytes: format!(
            "<<<<<<< {ours_label}\n{ours}||||||| original\n{base}=======\n{theirs}>>>>>>> {theirs_label}\n"
        )
        .into_bytes(),
        hunks: 1,
        kind,
    }
}

/// Merges optional bounded byte values after proving they are text.
///
/// `None` represents deletion. Both absent values produce empty clean output.
pub fn merge_bytes(
    base: Option<&[u8]>,
    ours: Option<&[u8]>,
    theirs: Option<&[u8]>,
    ours_label: &str,
    theirs_label: &str,
    limits: ByteMergeLimits,
) -> Result<ByteMerge, ByteMergeError> {
    let base = optional_text(base, limits)?;
    let ours = optional_text(ours, limits)?;
    let theirs = optional_text(theirs, limits)?;
    match (ours, theirs) {
        (Some(ours), Some(theirs)) if ours == theirs => {
            Ok(ByteMerge::Merged(ours.as_bytes().to_vec()))
        }
        (Some(ours), Some(theirs)) => {
            let result = merge_text(base.unwrap_or(""), ours, theirs, ours_label, theirs_label);
            if result.clean {
                Ok(ByteMerge::Merged(result.content.into_bytes()))
            } else {
                let hunks = conflict_hunks(&result.content);
                Ok(ByteMerge::Conflicted {
                    bytes: result.content.into_bytes(),
                    hunks,
                    kind: ByteConflictKind::Hunks,
                })
            }
        }
        (Some(ours), None) => Ok(modify_delete(
            base.unwrap_or(""),
            ours,
            &format!("{ours_label} (modified)"),
            &format!("{theirs_label} (deleted)"),
            ByteConflictKind::TheirsDeleted,
        )),
        (None, Some(theirs)) => Ok(modify_delete(
            base.unwrap_or(""),
            theirs,
            &format!("{ours_label} (deleted)"),
            &format!("{theirs_label} (modified)"),
            ByteConflictKind::OursDeleted,
        )),
        (None, None) => Ok(ByteMerge::Merged(Vec::new())),
    }
}

fn with_trailing_newline(value: &str) -> Cow<'_, str> {
    if value.is_empty() || value.ends_with('\n') {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(format!("{value}\n"))
    }
}

fn result_has_trailing_newline(base: &str, ours: &str, theirs: &str) -> bool {
    let base = base.ends_with('\n');
    let ours = ours.ends_with('\n');
    let theirs = theirs.ends_with('\n');
    if ours == theirs {
        ours
    } else if ours == base {
        theirs
    } else {
        ours
    }
}

/// Merges complete UTF-8 values with diff3 conflict markers.
///
/// Clean output preserves the newline decision made by the changed side.
/// Conflict markers always occupy complete lines and use the supplied labels.
#[must_use]
pub fn merge_text(
    base: &str,
    ours: &str,
    theirs: &str,
    ours_label: &str,
    theirs_label: &str,
) -> TextMerge {
    use diffy::{ConflictStyle, MergeOptions};

    let mut options = MergeOptions::new();
    options
        .set_conflict_style(ConflictStyle::Diff3)
        .set_conflict_marker_length(7);
    let base_value = with_trailing_newline(base);
    let ours_value = with_trailing_newline(ours);
    let theirs_value = with_trailing_newline(theirs);
    match options.merge(&base_value, &ours_value, &theirs_value) {
        Ok(mut content) => {
            if !result_has_trailing_newline(base, ours, theirs) {
                content.pop();
            }
            TextMerge {
                clean: true,
                content,
            }
        }
        Err(conflicted) => TextMerge {
            clean: false,
            content: conflicted
                .replace("<<<<<<< ours", &format!("<<<<<<< {ours_label}"))
                .replace(">>>>>>> theirs", &format!(">>>>>>> {theirs_label}")),
        },
    }
}

/// Returns whether text contains a complete generated conflict block.
#[must_use]
pub fn has_conflict_markers(content: &str) -> bool {
    let opens = content
        .split_inclusive('\n')
        .any(|line| line.starts_with("<<<<<<< ") || line.starts_with("<<<<<<<\t"));
    opens
        && content
            .lines()
            .any(|line| line == "=======" || line == "=======\r")
}

/// Counts complete opening conflict markers without overflowing.
#[must_use]
pub fn conflict_hunks(content: &str) -> u32 {
    content
        .lines()
        .filter(|line| line.starts_with("<<<<<<< "))
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_merge_preserves_content_and_newline_choice() {
        assert_eq!(
            merge_text("a\nb", "a\nb", "a\nc", "fork", "main"),
            TextMerge {
                clean: true,
                content: "a\nc".to_owned(),
            }
        );
    }

    #[test]
    fn conflict_uses_labels_and_detectable_complete_markers() {
        let result = merge_text("a\n", "b\n", "c\n", "fork", "main");
        assert!(!result.clean);
        assert!(result.content.contains("<<<<<<< fork\n"));
        assert!(result.content.contains(">>>>>>> main\n"));
        assert!(has_conflict_markers(&result.content));
        assert_eq!(conflict_hunks(&result.content), 1);
        assert!(!has_conflict_markers("<<<<<<< fork\nincomplete\n"));
    }

    #[test]
    fn byte_merge_owns_gate_and_modify_delete_semantics() {
        let limits = ByteMergeLimits { max_bytes: 16 };
        assert_eq!(
            merge_bytes(Some(b"a\n"), Some(b"b\n"), None, "fork", "main", limits),
            Ok(ByteMerge::Conflicted {
                bytes: b"<<<<<<< fork (modified)\nb\n||||||| original\na\n=======\n>>>>>>> main (deleted)\n".to_vec(),
                hunks: 1,
                kind: ByteConflictKind::TheirsDeleted,
            })
        );
        assert_eq!(
            merge_bytes(None, Some(b"a\0b"), Some(b"c"), "fork", "main", limits),
            Err(ByteMergeError::Binary)
        );
        assert_eq!(
            merge_bytes(
                None,
                Some(b"0123456789abcdefg"),
                None,
                "fork",
                "main",
                limits
            ),
            Err(ByteMergeError::TooLarge)
        );
    }
}
