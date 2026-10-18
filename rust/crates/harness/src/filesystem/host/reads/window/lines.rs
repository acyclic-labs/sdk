//! Pure streaming line selection and UTF-8 classification. Self-contained so
//! the fuzz target compiles this exact production file.
use std::ops::Range;

/// Chunk-by-chunk equivalent of selecting the terminator-inclusive line
/// window `[offset, offset + limit)` of the whole text. It never buffers: each
/// `update` names the part of its chunk the caller retains.
pub struct LineScan {
    offset: u64,
    end: Option<u64>,
    /// Lines started so far; a line starts at byte 0 and after each `\n`
    /// that is followed by another byte.
    started: u64,
    at_line_start: bool,
    retaining: bool,
    done: bool,
    length: u64,
    zero: bool,
    utf8: bool,
    carry: [u8; 4],
    carried: usize,
}

impl LineScan {
    /// Selects lines `offset..offset + limit` (one-based); `limit` absent
    /// selects through the end of the file.
    #[must_use]
    pub fn new(offset: u64, limit: Option<u64>) -> Self {
        let valid = offset != 0 && limit != Some(0);
        Self {
            offset,
            end: limit.and_then(|limit| offset.checked_add(limit)),
            started: 0,
            at_line_start: true,
            retaining: false,
            done: !valid,
            length: 0,
            zero: false,
            utf8: true,
            carry: [0; 4],
            carried: 0,
        }
    }

    /// Observes the next chunk and returns the contiguous part of it that
    /// belongs to the window, if any.
    pub fn update(&mut self, chunk: &[u8]) -> Option<Range<usize>> {
        if chunk.is_empty() {
            return None;
        }
        self.length += chunk.len() as u64;
        self.zero |= chunk.contains(&0);
        self.validate(chunk);
        if self.done {
            return None;
        }
        let mut from = self.retaining.then_some(0);
        let mut to = chunk.len();
        let starts = self.at_line_start.then_some(0).into_iter().chain(
            chunk
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'\n')
                .map(|(index, _)| index + 1)
                .filter(|start| *start < chunk.len()),
        );
        for start in starts {
            self.started += 1;
            if self.started == self.offset {
                self.retaining = true;
                from = Some(start);
            }
            if Some(self.started) == self.end {
                to = start;
                self.retaining = false;
                self.done = true;
                break;
            }
        }
        self.at_line_start = chunk.last() == Some(&b'\n');
        from.filter(|from| *from < to).map(|from| from..to)
    }

    /// Incremental UTF-8 validation that tolerates a character split
    /// across chunk boundaries.
    fn validate(&mut self, mut chunk: &[u8]) {
        if !self.utf8 {
            return;
        }
        while self.carried > 0 {
            let Some((&byte, rest)) = chunk.split_first() else {
                return;
            };
            self.carry[self.carried] = byte;
            self.carried += 1;
            chunk = rest;
            match std::str::from_utf8(&self.carry[..self.carried]) {
                Ok(_) => self.carried = 0,
                Err(error) if error.error_len().is_some() || self.carried == 4 => {
                    self.utf8 = false;
                    return;
                }
                Err(_) => {}
            }
        }
        if let Err(error) = std::str::from_utf8(chunk) {
            let rest = &chunk[error.valid_up_to()..];
            if error.error_len().is_some() {
                self.utf8 = false;
            } else {
                self.carry[..rest.len()].copy_from_slice(rest);
                self.carried = rest.len();
            }
        }
    }

    /// Whether the window names at least one line (an empty file has only
    /// line 1), and the whole file's media type (`None` when not UTF-8).
    #[must_use]
    pub fn finish(&self) -> (bool, Option<&'static str>) {
        let utf8 = self.utf8 && self.carried == 0;
        let media = utf8.then_some(if self.zero {
            "application/octet-stream"
        } else {
            "text/plain"
        });
        let selects = if self.offset == 0 || self.end == Some(self.offset) {
            false
        } else if self.length == 0 {
            self.offset == 1
        } else {
            self.started >= self.offset
        };
        (selects, media)
    }
}

#[cfg(test)]
#[path = "lines_spec.rs"]
mod spec;

#[cfg(test)]
mod tests {
    use super::LineScan;
    use super::spec::{reference, scan_chunks};
    use proptest::prelude::*;

    fn sorted(mut cuts: Vec<usize>) -> Vec<usize> {
        cuts.sort_unstable();
        cuts
    }

    proptest! {
        #[test]
        fn chunked_selection_matches_whole_text_selection(
            text in "[ab\n\u{e9}\u{1F600}\0]{0,40}",
            offset in 0u64..12,
            limit in proptest::option::of(0u64..6),
            cuts in proptest::collection::vec(0usize..200, 0..6),
        ) {
            let (selected, media) = scan_chunks(text.as_bytes(), sorted(cuts), offset, limit);
            prop_assert_eq!(selected, reference(text.as_bytes(), offset, limit));
            let expected = if text.contains('\0') {
                "application/octet-stream"
            } else {
                "text/plain"
            };
            prop_assert_eq!(media, Some(expected));
        }

        #[test]
        fn chunked_validation_matches_whole_validation(
            bytes in proptest::collection::vec(any::<u8>(), 0..24),
            cuts in proptest::collection::vec(0usize..40, 0..6),
        ) {
            let (_, media) = scan_chunks(&bytes, sorted(cuts), 1, None);
            prop_assert_eq!(media.is_some(), std::str::from_utf8(&bytes).is_ok());
        }
    }

    #[test]
    fn a_window_retains_only_its_lines() {
        let text = "one\ntwo\nthree\n".repeat(1000);
        let (selected, _) = scan_chunks(text.as_bytes(), [5, 4096, 9000], 2, Some(1));
        assert_eq!(selected.as_deref(), Some(&b"two\n"[..]));
        // Control: an offset past the last line selects nothing.
        assert_eq!(scan_chunks(text.as_bytes(), [], 3001, None).0, None);
    }

    #[test]
    fn split_and_truncated_sequences_are_classified_exactly() {
        // A four-byte character split at every boundary is valid; each
        // truncation or stray continuation byte is not.
        let crab = "\u{1F980}".as_bytes();
        for cut in 0..=crab.len() {
            assert_eq!(scan_chunks(crab, [cut], 1, None).1, Some("text/plain"));
            assert_eq!(
                scan_chunks(&crab[..cut], [cut / 2], 1, None).1.is_some(),
                cut == 0 || cut == crab.len()
            );
        }
        assert_eq!(scan_chunks(&[b'a', 0x80, b'\n'], [1, 2], 1, None).1, None);
        assert_eq!(scan_chunks(&[0xE2, 0x82, 0x28], [1], 1, None).1, None);
    }

    #[test]
    fn negative_control_detects_discarded_utf8_carry() {
        let bytes = &"🦀".as_bytes()[..3];
        let mut scan = LineScan::new(1, None);
        for byte in bytes {
            scan.update(std::slice::from_ref(byte));
        }
        let agrees =
            |scan: &LineScan| scan.finish().1.is_some() == std::str::from_utf8(bytes).is_ok();
        assert!(agrees(&scan));
        // Mutate the production scanner, emulating loss of the validation
        // frontier at a chunk boundary. The UTF-8 oracle must reject it.
        scan.carried = 0;
        assert!(!agrees(&scan), "UTF-8 oracle accepted a dropped carry");
    }
}
