use super::*;
use crate::Markdown;
use proptest::prelude::*;

#[test]
fn nested_images_after_lazy_list_match_every_prefix() {
    let source = "x *[\\\n* 0\n![a *b* ![c](v)](v)](u)* z";
    replay(source, &(1..source.len()).collect::<Vec<_>>());
}

fn replay(source: &str, cuts: &[usize]) -> Vec<crate::Line> {
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    let mut start = 0;
    for end in cuts.iter().copied().chain([source.len()]) {
        if end <= start || !source.is_char_boundary(end) {
            continue;
        }
        let changed = markdown.push(source.get(start..end).expect("chunk boundaries"));
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (markdown.lines().slice(changed..).unwrap_or_default())
                .iter()
                .cloned(),
        );
        assert_eq!(
            consumer,
            crate::tests::full_parse(source.get(..end).expect("prefix boundary")),
            "prefix {end} of {source:?}"
        );
        start = end;
    }
    consumer
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn compiled_labels_preserve_fenced_styles_and_child_scopes(
        image in any::<bool>(),
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(String::from),
            prop::sample::select(vec![
                "a", " ", "*", "**", "_", "~", "~~", "`", "``", "\\", "\\*", "\\`", "\\\n",
                "[", "]", "!", "(", ")", "&amp;", "&#10;", "&#0;", "\n", "\t", "\0",
                "[x](v)", "![x](v)", "![a *b* ![c](v)](v)", "<https://x>", "<b>", "<!--x-->",
            ]).prop_map(str::to_owned),
        ], 0..32),
        destination in prop::sample::select(vec!["u", "<u>", "u\\)v", "a(b)c", "u \"title\"", "<u>\n'title'", "u&amp;v"]),
        splits in prop::collection::vec(any::<usize>(), 0..32),
    ) {
        let source = format!("x *{}[{}]({destination})* z", if image { "!" } else { "" }, parts.concat());
        let mut cuts: Vec<_> = splits.into_iter().map(|cut| cut % (source.len()+1)).collect();
        cuts.sort_unstable();
        replay(&source, &cuts);
        replay(&source, &(1..source.len()).collect::<Vec<_>>());
    }
}

#[test]
fn label_property_detects_dropping_compiled_children() {
    let source = "x ![a ![b](v)c](u) z";
    FORGET_CHILD.set(true);
    let mut markdown = Markdown::default();
    markdown.push(source.get(..source.len() - 1).expect("before ASCII suffix"));
    markdown.push(source.get(source.len() - 1..).expect("ASCII suffix"));
    FORGET_CHILD.set(false);
    assert_ne!(markdown.lines(), crate::tests::full_parse(source));
}

fn measure(count: usize, replay: bool) -> (usize, usize) {
    let source = format!("x {}*a*{} z", "![".repeat(count), "](u)".repeat(count));
    super::super::REPLAY.set(replay);
    let before = crate::work::rendering_work();
    let mut markdown = Markdown::default();
    markdown.push(&source);
    let used = crate::work::rendering_work() - before;
    super::super::REPLAY.set(false);
    (used, source.len())
}

#[test]
fn nested_label_compilation_skips_shared_children() {
    for count in [64, 128, 256, 1024] {
        let small = measure(count, false);
        let large = measure(count * 2, false);
        assert!(
            large.0 <= 128 * large.1 + 4096,
            "{} work for {} bytes",
            large.0,
            large.1
        );
        assert!(
            large.0 <= 2 * small.0 + 4096,
            "{} -> {} work",
            small.0,
            large.0
        );
    }
}

#[test]
fn label_counter_detects_replaying_compiled_material() {
    let small = measure(64, true);
    let large = measure(128, true);
    assert!(large.0 > 128 * large.1 + 4096);
    assert!(large.0 > 2 * small.0 + 4096);
}
