use super::*;
use proptest::prelude::*;

fn partition(text: &str, cuts: &[bool]) {
    let mut stream = StreamText::default();
    let mut rows = Vec::new();
    let mut start = 0;
    for end in 1..=text.len() {
        if end != text.len() && !cuts.get(end - 1).copied().unwrap_or(false) {
            continue;
        }
        let changed = stream.push(&text.as_bytes()[start..end]);
        start = end;
        rows.truncate(changed);
        rows.extend(stream.rows(changed));
    }
    let mut unsplit = StreamText::default();
    let changed = unsplit.push(text.as_bytes());
    assert_eq!(rows, unsplit.rows(changed));
    assert_eq!(stream.work, text.len(), "stable bytes are parsed once");
}

#[test]
fn provisional_archive_row_is_invalidated() {
    let seed = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../harness-fuzz/corpus/ui_prompt/utf8-archive-row-repair"
    ));
    let text = format!("retained archive\n\n{seed}");
    let mut cuts = vec![false; text.len()];
    cuts[text.len() - 3] = true;
    partition(&text, &cuts);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn byte_partitions_preserve_rows_and_linear_work(
        text in "[a-z日é€🙂\\n*# ]{1,40}",
        cuts in prop::collection::vec(any::<bool>(), 0..200)
    ) { partition(&format!("retained archive\n\n{text}"), &cuts); }
}

#[test]
fn every_partition_repairs_same_length_utf8() {
    let provisional = String::from_utf8_lossy(&[0xe6]).into_owned();
    let repaired = "日";
    assert_ne!(
        format!("{provisional}{}", &repaired[provisional.len()..]),
        repaired,
        "the former length-only cursor misses this repair"
    );
    let text = "日a€";
    for mask in 0..(1usize << (text.len() - 1)) {
        let cuts: Vec<_> = (0..text.len() - 1)
            .map(|bit| mask & (1 << bit) != 0)
            .collect();
        partition(text, &cuts);
        partition(&format!("retained archive\n\n{text}"), &cuts);
    }
}
