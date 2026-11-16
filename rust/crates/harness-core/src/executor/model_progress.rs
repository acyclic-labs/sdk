//! Detects a model stream that stops making progress: a long run of lines
//! already present in the recent window. A degenerate loop is a model failure,
//! not an output cap, so the turn settles with a cause the agent can see.

use crate::model::ModelEvent;
use std::collections::VecDeque;

/// Repeated bytes after the last new distinct line before the stream is
/// degenerate. A legitimate response introduces a new line well within this.
const REPEATED_BYTES: u64 = 64 * 1024;
/// Recent distinct lines retained to recognize a loop. A loop with more
/// distinct lines than this is not the degenerate repetition this detects.
const DISTINCT_LINES: usize = 256;
/// A single line longer than this is progress, not a repeated short line. Its
/// bytes are not retained; the line is treated as new when it ends.
const MAX_LINE_BYTES: usize = 8 * 1024;

#[derive(Default)]
pub(super) struct Progress {
    line: Vec<u8>,
    line_bytes: u64,
    line_overflow: bool,
    recent: VecDeque<u64>,
    repeated_bytes: u64,
}

impl Progress {
    /// Observe one streamed event. A restart abandons the previous attempt, so
    /// its output cannot make the replacement attempt look degenerate.
    pub(super) fn observe(&mut self, event: &ModelEvent) -> crate::Result<()> {
        match event {
            ModelEvent::Content { delta } | ModelEvent::Reasoning { delta } => self.text(delta),
            ModelEvent::Restarted => self.reset(),
            _ => {}
        }
        if self.repeated_bytes >= REPEATED_BYTES {
            return Err(crate::Error::ModelDiagnostic {
                code: crate::wire::ErrorCode::Invalid,
                message: "model stream repeated without progress".into(),
                custody: None,
            });
        }
        Ok(())
    }

    fn text(&mut self, text: &str) {
        for byte in text.bytes() {
            if byte == b'\n' {
                self.finish_line();
            } else if !self.line_overflow {
                if self.line.len() < MAX_LINE_BYTES {
                    self.line.push(byte);
                } else {
                    self.line_overflow = true;
                    self.line.clear();
                }
            }
            self.line_bytes = self.line_bytes.saturating_add(1);
        }
    }

    fn finish_line(&mut self) {
        if self.line_overflow {
            // A long line is progress; it cannot be a repeated short line.
            self.repeated_bytes = 0;
        } else {
            let hash = hash(&self.line);
            if self.recent.contains(&hash) {
                self.repeated_bytes = self
                    .repeated_bytes
                    .saturating_add(self.line_bytes.saturating_add(1));
            } else {
                if self.recent.len() == DISTINCT_LINES {
                    self.recent.pop_front();
                }
                self.recent.push_back(hash);
                self.repeated_bytes = 0;
            }
        }
        self.line.clear();
        self.line_bytes = 0;
        self.line_overflow = false;
    }

    fn reset(&mut self) {
        self.line.clear();
        self.line_bytes = 0;
        self.line_overflow = false;
        self.recent.clear();
        self.repeated_bytes = 0;
    }
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reasoning(delta: &str) -> ModelEvent {
        ModelEvent::Reasoning {
            delta: delta.into(),
        }
    }

    #[test]
    fn a_repeated_line_window_is_degenerate() {
        let mut progress = Progress::default();
        let line = "the same reasoning line\n";
        let mut error = None;
        for _ in 0..(REPEATED_BYTES as usize / line.len() + 1) {
            if let Err(cause) = progress.observe(&reasoning(line)) {
                error = Some(cause);
                break;
            }
        }
        assert!(matches!(
            error,
            Some(crate::Error::ModelDiagnostic {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
    }

    #[test]
    fn distinct_lines_and_long_lines_are_progress() -> crate::Result<()> {
        let mut progress = Progress::default();
        for index in 0..(REPEATED_BYTES as usize / 8) {
            progress.observe(&reasoning(&format!("line {index}\n")))?;
        }
        let long = "x".repeat(MAX_LINE_BYTES + 1);
        for _ in 0..(REPEATED_BYTES as usize / long.len() + 1) {
            progress.observe(&reasoning(&format!("{long}\n")))?;
        }
        Ok(())
    }

    #[test]
    fn a_restart_abandons_the_previous_attempt() -> crate::Result<()> {
        let mut progress = Progress::default();
        let line = "the same reasoning line\n";
        let half = REPEATED_BYTES as usize / line.len() / 2;
        for _ in 0..half {
            progress.observe(&reasoning(line))?;
        }
        progress.observe(&ModelEvent::Restarted)?;
        for _ in 0..half {
            progress.observe(&reasoning(line))?;
        }
        Ok(())
    }
}
