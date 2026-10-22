//! NUL-delimited Git porcelain parsing; path records never become status records.
/// Summarizes NUL-separated porcelain v1 records with a branch header.
pub(crate) fn summarize(porcelain: &[u8], truncated: bool) -> String {
    let mut records = porcelain.split_inclusive(|byte| *byte == 0);
    let mut branch = None;
    let [mut staged, mut modified, mut untracked, mut conflicted] = [0_u64; 4];
    let mut complete = !truncated;
    while let Some(record) = records.next() {
        let Some(record) = record.strip_suffix(&[0]) else {
            complete = false;
            break;
        };
        if let Some(header) = record.strip_prefix(b"## ") {
            branch = Some(String::from_utf8_lossy(header).into_owned());
            continue;
        }
        let [index, tree, b' ', ..] = record else {
            // An empty tail follows the final NUL; anything else is a cut record.
            complete &= record.is_empty();
            continue;
        };
        match (index, tree) {
            (b'?', b'?') => untracked += 1,
            (b'!', b'!') => {}
            (b'D', b'D') | (b'A', b'A') | (b'U', _) | (_, b'U') => conflicted += 1,
            _ => {
                staged += u64::from(*index != b' ');
                modified += u64::from(*tree != b' ');
            }
        }
        // A rename or copy carries its source path as the next record.
        if matches!(index, b'R' | b'C') || matches!(tree, b'R' | b'C') {
            complete &= records.next().is_some_and(|source| source.ends_with(&[0]));
        }
    }
    let mut summary = branch.map_or_else(String::new, |branch| format!("branch {branch}"));
    let mut counts = Vec::new();
    for (count, label) in [
        (staged, "staged"),
        (modified, "modified"),
        (untracked, "untracked"),
        (conflicted, "conflicted"),
    ] {
        if count != 0 {
            counts.push(format!("{count} {label}"));
        }
    }
    if !summary.is_empty() {
        summary.push_str("; ");
    }
    if counts.is_empty() && complete {
        summary.push_str("clean");
    } else {
        summary.push_str(&counts.join(", "));
    }
    if !complete {
        summary.push_str(if counts.is_empty() { "" } else { ", " });
        summary.push_str("more changes not counted");
    }
    summary
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
        ) {
            let mut bytes = b"## main\0".to_vec();
            for path in &paths {
                bytes.extend(if tree_rename { b" R " } else { b"R  " });
                bytes.extend(path.as_bytes());
                bytes.extend(b"\0?? source\0");
            }
            let expected = if paths.is_empty() { "branch main; clean".into() }
                else { format!("branch main; {} {}", paths.len(), if tree_rename { "modified" } else { "staged" }) };
            prop_assert_eq!(summarize(&bytes, false), expected);
            bytes.extend(b"?? unfinished");
            let partial = summarize(&bytes, true);
            prop_assert!(partial.ends_with("more changes not counted"));
            prop_assert!(!partial.contains("untracked"));
        }

        #[test]
        fn every_input_is_total(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
            let summary = summarize(&bytes, true);
            prop_assert!(summary.ends_with("more changes not counted"));
        }
    }

    #[test]
    fn cut_records_and_rename_sources_are_never_complete_changes() {
        assert_eq!(
            summarize(b"## main\0?? unfinished", true),
            "branch main; more changes not counted"
        );
        assert_eq!(summarize(b" R new\0?? old\0", false), "1 modified");
        assert_eq!(
            summarize(b"R  new\0", false),
            "1 staged, more changes not counted"
        );
    }
}
