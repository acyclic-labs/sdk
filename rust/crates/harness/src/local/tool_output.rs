//! The live display of an agent's newest tool call or streaming answer,
//! folded from the change records of its `tools/` directory: a call's
//! `stdout/` and `stderr/` chunks, or a model dispatch's `reply/` chunks.

/// Bytes of the newest call's output a [`OutputTail`] retains.
const TAIL_BYTES: usize = 16 * 1024;
/// Largest output chunk a shell call stages.
pub(super) const CHUNK_BYTES: usize = 64 * 1024;

/// The newest call's output: its last [`TAIL_BYTES`] at most, decoded across
/// chunk boundaries.
#[derive(Default)]
pub(super) struct OutputTail {
    /// Next change record not yet absorbed.
    next: u64,
    /// The opened follow's replayed history ends here; nothing is chosen
    /// before it is drained, so a reopen reads only the newest call.
    replayed: Option<u64>,
    /// `tools/{turn}/{operation}` of the newest call or dispatch.
    call: Option<String>,
    /// Whether the newest is a model dispatch's answer text.
    reply: bool,
    text: String,
    /// Bytes of a character a chunk split, for stdout and stderr.
    split: [Vec<u8>; 2],
}

/// Chunks to read for one absorb, committed with [`OutputTail::apply`].
pub(super) struct Plan {
    end: u64,
    call: Option<String>,
    reply: bool,
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
        let mut reply = false;
        let mut chunks = Vec::new();
        for (sequence, paths) in records {
            if sequence < self.next {
                continue;
            }
            for path in paths {
                if let Some((owner, stderr, answer)) = path
                    .split_once("/stdout/")
                    .map(|(owner, _)| (owner, false, false))
                    .or_else(|| {
                        path.split_once("/stderr/")
                            .map(|(owner, _)| (owner, true, false))
                    })
                    .or_else(|| {
                        path.split_once("/reply/")
                            .map(|(owner, _)| (owner, false, true))
                    })
                {
                    call = Some(owner.to_owned());
                    reply = answer;
                    chunks.push((owner.to_owned(), path.clone(), stderr));
                } else if let Some(owner) = path.strip_suffix("/request.json") {
                    // A new call starts its display even before it writes.
                    call = Some(owner.to_owned());
                    reply = false;
                }
            }
        }
        let reads = chunks
            .into_iter()
            .filter(|(owner, _, _)| call.as_deref() == Some(owner))
            .map(|(_, path, stderr)| (path, stderr))
            .collect();
        Some(Plan {
            end,
            call,
            reply,
            reads,
        })
    }

    /// Commits `plan` once every one of its reads settled: `bytes` holds the
    /// last reads' bytes in order (see [`read_newest`]), and `cut` says
    /// output before them was never read, so they replace the display. A
    /// failed or cancelled absorb changes nothing, so a retry reads the same
    /// announcements exactly once.
    pub(super) fn apply(&mut self, plan: Plan, bytes: Vec<Vec<u8>>, cut: bool) {
        let switched = plan.call.is_some() && plan.call != self.call;
        if switched {
            self.call = plan.call;
            self.reply = plan.reply;
        }
        if switched || cut {
            self.text.clear();
            self.split = Default::default();
        }
        let skipped = plan.reads.len().saturating_sub(bytes.len());
        for (index, ((_, stderr), bytes)) in plan.reads[skipped..].iter().zip(bytes).enumerate() {
            // A cut can fall inside a character: its trailing bytes show nothing.
            let start = if cut && index == 0 {
                bytes
                    .iter()
                    .take_while(|byte| **byte & 0xc0 == 0x80)
                    .count()
            } else {
                0
            };
            decode(
                &mut self.split[usize::from(*stderr)],
                &bytes[start..],
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
        self.reply = false;
        self.text.clear();
        self.split = Default::default();
    }

    pub(super) fn text(&self) -> &str {
        &self.text
    }

    /// Whether [`Self::text`] is a streaming answer rather than tool output.
    pub(super) const fn reply(&self) -> bool {
        self.reply
    }
}

/// The bytes the display keeps of `reads`, newest last: files are read from
/// the newest back only as far as the last [`TAIL_BYTES`] reach, in the
/// ranges `read` serves (the bytes at an offset and the file's length), so
/// output of any size costs a bounded number of bounded reads. The flag says
/// output before the returned bytes was never read.
pub(super) async fn read_newest<F, R>(
    reads: &[(String, bool)],
    mut read: F,
) -> crate::Result<(Vec<Vec<u8>>, bool)>
where
    F: FnMut(String, u64) -> R,
    R: std::future::Future<Output = crate::Result<(Vec<u8>, u64)>>,
{
    let mut kept = Vec::new();
    let mut total = 0;
    let mut cut = false;
    for (path, _) in reads.iter().rev() {
        if total >= TAIL_BYTES {
            cut = true;
            break;
        }
        let keep = (TAIL_BYTES - total) as u64;
        let (bytes, skipped) = read_tail(keep, |offset| read(path.clone(), offset)).await?;
        total += bytes.len();
        kept.push(bytes);
        if skipped {
            cut = true;
            break;
        }
    }
    kept.reverse();
    Ok((kept, cut))
}

/// The last `keep` bytes at most of one file served in ranges by `read`,
/// and whether bytes before them were skipped: the first range read learns
/// the length, and reading resumes where the kept bytes start.
async fn read_tail<F, R>(keep: u64, mut read: F) -> crate::Result<(Vec<u8>, bool)>
where
    F: FnMut(u64) -> R,
    R: std::future::Future<Output = crate::Result<(Vec<u8>, u64)>>,
{
    let mut bytes = Vec::new();
    let mut offset = 0;
    let mut skipped = false;
    loop {
        let (range, length) = read(offset).await?;
        let start = length.saturating_sub(keep);
        let end = offset.saturating_add(range.len() as u64);
        if end < start {
            skipped = true;
            offset = start;
            continue;
        }
        let drop = usize::try_from(start.saturating_sub(offset))
            .unwrap_or(usize::MAX)
            .min(range.len());
        skipped |= drop > 0;
        bytes.extend_from_slice(&range[drop..]);
        offset = end;
        if range.is_empty() || offset >= length {
            return Ok((bytes, skipped));
        }
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
    use super::{OutputTail, TAIL_BYTES, read_newest};
    use std::{cell::Cell, collections::BTreeMap};

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
        tail.apply(
            plan,
            bytes.iter().map(|bytes| bytes.to_vec()).collect(),
            false,
        );
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

    #[tokio::test]
    async fn an_answer_of_several_mib_in_one_delta_reads_only_its_shown_tail() {
        // One model delta of 6 MiB, staged as one reply file, after an
        // earlier file the display no longer reaches.
        let answer: String = (0..6 * 1024 * 1024 / 4)
            .map(|n| ["a", "\u{e9}", "\u{2713}", "b"][n % 4])
            .collect();
        let files = BTreeMap::from([
            ("tools/t/d/reply/0".to_owned(), b"earlier".to_vec()),
            ("tools/t/d/reply/1".to_owned(), answer.clone().into_bytes()),
        ]);
        let reads = vec![
            ("tools/t/d/reply/0".to_owned(), false),
            ("tools/t/d/reply/1".to_owned(), false),
        ];
        let calls = Cell::new(0);
        let largest = Cell::new(0);
        // The host serves bounded ranges, as its read command does.
        let read = read_newest(&reads, |path, offset| {
            calls.set(calls.get() + 1);
            let file = &files[&path];
            let start = usize::try_from(offset).map_or(file.len(), |start| start.min(file.len()));
            let range = file[start..(start + 32 * 1024).min(file.len())].to_vec();
            largest.set(largest.get().max(range.len()));
            std::future::ready(Ok((range, file.len() as u64)))
        })
        .await;
        let Ok((bytes, cut)) = read else {
            panic!("the tail read failed");
        };
        assert!(cut);
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0].len(), TAIL_BYTES);
        // One read learns the length; then only the shown tail is read.
        assert!(calls.get() <= 3, "{} reads", calls.get());
        assert!(largest.get() <= 32 * 1024);
        let mut tail = OutputTail::default();
        let records = vec![(0, paths(&["tools/t/d/reply/0", "tools/t/d/reply/1"]))];
        let Some(plan) = tail.plan(records, 1, 1) else {
            panic!("the drained follow plans its reads");
        };
        tail.apply(plan, bytes, cut);
        assert!(tail.reply());
        assert!(answer.ends_with(tail.text()));
        assert!(tail.text().len() + 4 > TAIL_BYTES);
    }
}
