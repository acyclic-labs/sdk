use super::*;

fn check(source: &str, sizes: &[usize]) -> Vec<Line> {
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    let mut at = 0;
    let mut chunk = 0;
    while at < source.len() {
        let end = (at
            + sizes
                .get(chunk % sizes.len())
                .expect("nonempty chunk sizes"))
        .min(source.len());
        let changed = markdown.push(source.get(at..end).expect("ASCII chunk boundaries"));
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend_from_slice(
            markdown
                .lines()
                .get(changed..)
                .expect("valid change boundary"),
        );
        assert_eq!(
            consumer,
            full_parse(source.get(..end).expect("ASCII prefix boundary")),
            "prefix {end}: {source:?}"
        );
        at = end;
        chunk += 1;
    }
    assert_eq!(
        markdown.into_source(),
        source.replace("\r\n", "\n").replace('\r', "\n")
    );
    consumer
}

#[test]
fn deferred_tab_survives_plain_append() {
    let lines = check("hello world\tfoo", &[11, 1, 3]);
    let text: String = lines
        .iter()
        .flat_map(|line| &line.spans)
        .map(|span| span.text.as_str())
        .collect();
    assert_eq!(text, "hello world\tfoo");
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/whitespace.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn whitespace_chunk_splits_equal_full_render(
        parts in prop::collection::vec(prop_oneof![
            "[a-z]{1,8}", "[ \\t]{1,8}",
            prop::sample::select(vec!["\n", "\r", "\r\n"]).prop_map(str::to_owned),
        ], 0..16),
        sizes in prop::collection::vec(1usize..24, 1..12),
    ) {
        let source = format!("hello world{}foo", parts.concat());
        let _ = check(&source, &sizes);
    }
}
