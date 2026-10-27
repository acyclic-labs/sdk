//! The live display of an agent's newest tool call, folded from the change
//! records of its `tools/` directory.

/// Bytes of the newest call's output a [`OutputTail`] retains.
const TAIL_BYTES: usize = 16 * 1024;
/// Largest output chunk a shell call stages.
pub(super) const CHUNK_BYTES: usize = crate::tools::OUTPUT_CHUNK_BYTES;

/// The newest call's output: its last [`TAIL_BYTES`] at most, decoded across
/// chunk boundaries.
#[derive(Default)]
pub(super) struct OutputTail {
    /// Next change record not yet absorbed.
    next: u64,
    /// The opened follow's replayed history ends here; nothing is chosen
    /// before it is drained, so a reopen reads only the newest call.
    replayed: Option<u64>,
    /// `tools/{turn}/{operation}` of the newest call.
    call: Option<String>,
    text: String,
    /// Bytes of a character a chunk split, for stdout and stderr.
    split: [Vec<u8>; 2],
}

/// Chunks to read for one absorb, committed with [`OutputTail::apply`].
pub(super) struct Plan {
    end: u64,
    call: Option<String>,
    /// Each chunk's path and whether it is stderr.
    pub(super) reads: Vec<(String, bool)>,
}

impl OutputTail {
    /// First change record sequence not yet absorbed.
    pub(super) const fn next(&self) -> u64 {
        self.next
    }

    /// Plans reading the newest call's chunks that `records`, each a
    /// sequence and its changed paths, announce at or after [`Self::next`],
    /// up to `end`. `accepted` is the follow's accepted watermark; until the
    /// first one opened is drained this returns `None` and plans nothing.
    pub(super) fn plan(
        &mut self,
        records: impl IntoIterator<Item = (u64, Vec<String>)>,
        end: u64,
        accepted: u64,
    ) -> Option<Plan> {
        if end < *self.replayed.get_or_insert(accepted) {
            return None;
        }
        let mut call = None;
        let mut chunks = Vec::new();
        for (sequence, paths) in records {
            if sequence < self.next {
                continue;
            }
            for path in paths {
                if let Some((owner, stderr)) = path
                    .split_once("/stdout/")
                    .map(|(owner, _)| (owner, false))
                    .or_else(|| path.split_once("/stderr/").map(|(owner, _)| (owner, true)))
                {
                    call = Some(owner.to_owned());
                    chunks.push((owner.to_owned(), path.clone(), stderr));
                } else if let Some(owner) = path.strip_suffix("/request.json") {
                    // A new call starts its display even before it writes.
                    call = Some(owner.to_owned());
                }
            }
        }
        let reads = chunks
            .into_iter()
            .filter(|(owner, _, _)| call.as_deref() == Some(owner))
            .map(|(_, path, stderr)| (path, stderr))
            .collect();
        Some(Plan { end, call, reads })
    }

    /// Commits `plan` once every one of its reads, `bytes` in order, settled:
    /// a failed or cancelled absorb changes nothing, so a retry reads the
    /// same announcements exactly once.
    pub(super) fn apply(&mut self, plan: Plan, bytes: Vec<Vec<u8>>) {
        if plan.call.is_some() && plan.call != self.call {
            self.call = plan.call;
            self.text.clear();
            self.split = Default::default();
        }
        for ((_, stderr), bytes) in plan.reads.iter().zip(bytes) {
            decode(
                &mut self.split[usize::from(*stderr)],
                &bytes,
                &mut self.text,
            );
            keep_tail(&mut self.text);
        }
        self.next = plan.end;
    }

    /// Clears the display when a turn settles; records before `end` are
    /// never shown again.
    pub(super) fn reset(&mut self, end: u64) {
        self.next = self.next.max(end);
        self.call = None;
        self.text.clear();
        self.split = Default::default();
    }

    pub(super) fn text(&self) -> &str {
        &self.text
    }
}

/// Appends `bytes` to `text`, completing the character `split` holds and
/// keeping a trailing incomplete one there for the next chunk.
fn decode(split: &mut Vec<u8>, bytes: &[u8], text: &mut String) {
    split.extend_from_slice(bytes);
    let mut start = 0;
    while start < split.len() {
        match std::str::from_utf8(&split[start..]) {
            Ok(valid) => {
                text.push_str(valid);
                start = split.len();
            }
            Err(error) => {
                let valid = start + error.valid_up_to();
                text.push_str(&String::from_utf8_lossy(&split[start..valid]));
                match error.error_len() {
                    Some(invalid) => {
                        text.push(char::REPLACEMENT_CHARACTER);
                        start = valid + invalid;
                    }
                    None => {
                        start = valid;
                        break;
                    }
                }
            }
        }
    }
    split.drain(..start);
}

/// Bounds the staged display to its last [`TAIL_BYTES`] on a char boundary.
fn keep_tail(text: &mut String) {
    if text.len() > TAIL_BYTES {
        let mut cut = text.len() - TAIL_BYTES;
        while !text.is_char_boundary(cut) {
            cut += 1;
        }
        text.drain(..cut);
    }
}

#[cfg(test)]
mod tests {
    use super::OutputTail;

    fn paths(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|path| (*path).to_owned()).collect()
    }

    /// Plans and applies one absorb whose reads return `bytes` in order.
    fn absorb(
        tail: &mut OutputTail,
        records: Vec<(u64, Vec<String>)>,
        end: u64,
        accepted: u64,
        bytes: &[&[u8]],
    ) -> Option<Vec<String>> {
        let plan = tail.plan(records, end, accepted)?;
        let reads = plan.reads.iter().map(|(path, _)| path.clone()).collect();
        tail.apply(plan, bytes.iter().map(|bytes| bytes.to_vec()).collect());
        Some(reads)
    }

    #[test]
    fn a_reopen_drains_replayed_history_and_reads_only_the_newest_call() {
        let mut tail = OutputTail::default();
        let old = (
            0,
            paths(&["tools/t1/a/request.json", "tools/t1/a/stdout/0"]),
        );
        let new = (
            1,
            paths(&["tools/t2/b/request.json", "tools/t2/b/stdout/0"]),
        );
        // The first frame replays only the old call; the follow opened at 2.
        assert!(absorb(&mut tail, vec![old.clone()], 1, 2, &[]).is_none());
        assert_eq!(tail.next(), 0);
        assert_eq!(
            absorb(&mut tail, vec![old, new], 2, 2, &[b"new"]),
            Some(vec!["tools/t2/b/stdout/0".to_owned()])
        );
        assert_eq!(tail.text(), "new");
        // Once drained, live records are absorbed as they arrive.
        let live = (2, paths(&["tools/t2/b/stdout/1"]));
        assert!(absorb(&mut tail, vec![live], 3, 4, &[b" more"]).is_some());
        assert_eq!(tail.text(), "new more");
    }

    #[test]
    fn a_settled_turn_clears_the_display_and_skips_its_records() {
        let mut tail = OutputTail::default();
        let first = (0, paths(&["tools/t1/a/stdout/0"]));
        assert!(absorb(&mut tail, vec![first.clone()], 1, 1, &[b"old"]).is_some());
        let late = (1, paths(&["tools/t1/a/stdout/1"]));
        tail.reset(2);
        assert_eq!(tail.text(), "");
        // Records the settled turn announced are not read again.
        assert_eq!(
            absorb(&mut tail, vec![first, late], 2, 2, &[]),
            Some(Vec::new())
        );
        assert_eq!(tail.text(), "");
        // The next turn's call starts its own display.
        let next = (2, paths(&["tools/t2/b/request.json"]));
        assert!(absorb(&mut tail, vec![next], 3, 3, &[]).is_some());
        let output = (3, paths(&["tools/t2/b/stdout/0"]));
        assert!(absorb(&mut tail, vec![output], 4, 4, &[b"fresh"]).is_some());
        assert_eq!(tail.text(), "fresh");
    }

    #[test]
    fn a_character_split_across_chunks_decodes_whole() {
        let mut tail = OutputTail::default();
        let bytes = "é✓".as_bytes();
        let records = vec![(0, paths(&["tools/t/a/stdout/0", "tools/t/a/stdout/1"]))];
        assert!(absorb(&mut tail, records, 1, 1, &[&bytes[..1], &bytes[1..3]]).is_some());
        assert_eq!(tail.text(), "é");
        let records = vec![(1, paths(&["tools/t/a/stdout/2"]))];
        assert!(absorb(&mut tail, records, 2, 2, &[&bytes[3..]]).is_some());
        assert_eq!(tail.text(), "é✓");
        // An invalid byte is replaced, and decoding resumes after it.
        let records = vec![(2, paths(&["tools/t/a/stderr/0"]))];
        assert!(absorb(&mut tail, records, 3, 3, &[b"\xffok"]).is_some());
        assert_eq!(tail.text(), "é✓\u{fffd}ok");
    }
}
