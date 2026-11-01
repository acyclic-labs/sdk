//! NUL-delimited Git porcelain parsing; path records never become status records.
/// Incrementally counts complete records without retaining file names.
pub(super) struct Summary {
    prefix: [u8; 3],
    used: usize,
    header: Vec<u8>,
    branch: Option<String>,
    counts: [u64; 4],
    skip_source: bool,
    complete: bool,
}

impl Summary {
    pub(super) fn new() -> Self {
        Self {
            prefix: [0; 3],
            used: 0,
            header: Vec::new(),
            branch: None,
            counts: [0; 4],
            skip_source: false,
            complete: true,
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        for fragment in bytes.split_inclusive(|byte| *byte == 0) {
            let (fragment, ended) = match fragment.strip_suffix(&[0]) {
                Some(record) => (record, true),
                None => (fragment, false),
            };
            if !self.skip_source {
                let copied = fragment.len().min(3 - self.used);
                self.prefix[self.used..self.used + copied].copy_from_slice(&fragment[..copied]);
                self.used += copied;
                if self.used == 3 && self.prefix == *b"## " {
                    if self.header.is_empty() {
                        self.header.extend_from_slice(b"branch ");
                    }
                    self.header.extend_from_slice(&fragment[copied..]);
                }
            }
            if ended {
                self.end_record();
            }
        }
    }

    fn end_record(&mut self) {
        if self.skip_source {
            self.skip_source = false;
        } else if self.used == 3 && self.prefix == *b"## " {
            let header = std::mem::take(&mut self.header);
            self.branch =
                Some(String::from_utf8(header).unwrap_or_else(|error| {
                    String::from_utf8_lossy(error.as_bytes()).into_owned()
                }));
        } else if self.used == 3 && self.prefix[2] == b' ' {
            let [index, tree, _] = self.prefix;
            match (index, tree) {
                (b'?', b'?') => self.counts[2] += 1,
                (b'!', b'!') => {}
                (b'D', b'D') | (b'A', b'A') | (b'U', _) | (_, b'U') => self.counts[3] += 1,
                _ => {
                    self.counts[0] += u64::from(index != b' ');
                    self.counts[1] += u64::from(tree != b' ');
                }
            }
            // A rename or copy carries its source path as the next record.
            self.skip_source = matches!(index, b'R' | b'C') || matches!(tree, b'R' | b'C');
        } else {
            self.complete &= self.used == 0;
        }
        self.used = 0;
    }

    pub(super) fn finish(self) -> String {
        use std::fmt::Write as _;
        let complete = self.complete && self.used == 0 && !self.skip_source;
        let mut summary = self.branch.unwrap_or_default();
        if !summary.is_empty() {
            summary.push_str("; ");
        }
        let mut separator = "";
        for (count, label) in
            self.counts
                .into_iter()
                .zip(["staged", "modified", "untracked", "conflicted"])
        {
            if count != 0 {
                write!(summary, "{separator}{count} {label}").expect("writing to a String");
                separator = ", ";
            }
        }
        if !complete {
            summary.push_str(separator);
            summary.push_str("more changes not counted");
        } else if separator.is_empty() {
            summary.push_str("clean");
        }
        summary
    }
}

#[cfg(test)]
pub(super) fn summarize(bytes: &[u8]) -> String {
    let mut summary = Summary::new();
    summary.push(bytes);
    summary.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: file names (including status-looking rename sources) cannot
        // affect accounting. Only complete NUL-terminated records are counted.
        #[test]
        fn filenames_do_not_change_counts(
            paths in prop::collection::vec("[^\x00]{1,30}", 0..20),
            tree_rename in any::<bool>(),
            chunk_size in 1_usize..128,
        ) {
            let mut bytes = b"## main\0".to_vec();
            for path in &paths {
                bytes.extend(if tree_rename { b" R " } else { b"R  " });
                bytes.extend(path.as_bytes());
                bytes.extend(b"\0?? source\0");
            }
            let expected = if paths.is_empty() { "branch main; clean".into() }
                else { format!("branch main; {} {}", paths.len(), if tree_rename { "modified" } else { "staged" }) };
            let mut streamed = Summary::new();
            for chunk in bytes.chunks(chunk_size) {
                streamed.push(chunk);
            }
            prop_assert_eq!(streamed.finish(), expected.as_str());
            prop_assert_eq!(summarize(&bytes), expected);
            bytes.extend(b"?? unfinished");
            let partial = summarize(&bytes);
            prop_assert!(partial.ends_with("more changes not counted"));
            prop_assert!(!partial.contains("untracked"));
        }

        #[test]
        fn every_input_is_total_and_independent_of_chunk_boundaries(
            bytes in prop::collection::vec(any::<u8>(), 0..1024),
            chunk_size in 1_usize..128,
        ) {
            let mut streamed = Summary::new();
            for chunk in bytes.chunks(chunk_size) {
                streamed.push(chunk);
            }
            prop_assert_eq!(streamed.finish(), summarize(&bytes));
        }
    }

    #[test]
    fn cut_records_and_rename_sources_are_never_complete_changes() {
        assert_eq!(
            summarize(b"## main\0?? unfinished"),
            "branch main; more changes not counted"
        );
        assert_eq!(summarize(b" R new\0?? old\0"), "1 modified");
        assert_eq!(summarize(b"R  new\0"), "1 staged, more changes not counted");
    }

    #[test]
    fn large_status_output_keeps_only_prefixes_and_branch_text() {
        let mut summary = Summary::new();
        for byte in b"## main\0" {
            summary.push(std::slice::from_ref(byte));
        }
        // More than the former whole-output bound, with status-looking names
        // split across records and read boundaries.
        for _ in 0..20_000 {
            summary.push(b"R");
            summary.push(b"  new\0?");
            summary.push(b"? source\0?? ");
            summary.push(b"untracked\0");
        }
        assert_eq!(
            summary.finish(),
            "branch main; 20000 staged, 20000 untracked"
        );
    }
}
