use super::*;
use proptest::prelude::*;

fn partition(text: &str, cuts: &[bool]) {
    let mut stream = StreamText::default();
    let mut rows = Vec::new();
    for end in 1..=text.len() {
        if end != text.len() && !cuts.get(end - 1).copied().unwrap_or(false) {
            continue;
        }
        let bytes = &text.as_bytes()[..end];
        let pending = std::str::from_utf8(bytes).is_err();
        let visible = String::from_utf8_lossy(bytes);
        let changed = stream.push(&visible, usize::from(pending) * 3);
        rows.truncate(changed);
        rows.extend(stream.rows(changed));
    }
    let mut unsplit = StreamText::default();
    let changed = unsplit.push(text, 0);
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
