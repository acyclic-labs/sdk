use acyclic_markdown::Markdown;

pub fn check(data: &[u8]) {
    check_without_row_replacement(data, false);
}

pub fn check_without_row_replacement(data: &[u8], omit_truncation: bool) {
    let source = String::from_utf8_lossy(data);
    let mut streamed = Markdown::default();
    let mut consumer = Vec::new();
    let mut offset = 0;
    let mut chunk = 0;
    while offset < source.len() {
        let size = usize::from(data.get(chunk % data.len().max(1)).copied().unwrap_or(0)) % 31 + 1;
        let mut end = (offset + size).min(source.len());
        while !source.is_char_boundary(end) {
            end += 1;
        }
        let changed = streamed.push(source.get(offset..end).unwrap_or_default());
        assert!(changed <= streamed.lines().len());
        if !omit_truncation {
            consumer.truncate(changed);
        }
        consumer.extend_from_slice(streamed.lines().get(changed..).unwrap_or_default());
        let mut one_shot = Markdown::default();
        one_shot.push(source.get(..end).unwrap_or_default());
        assert_eq!(
            consumer,
            one_shot.lines(),
            "changed Markdown rows were not replaced"
        );
        assert!(
            streamed
                .lines()
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| !span.text.contains(['\n', '\r']))
        );
        offset = end;
        chunk += 1;
    }
    let expected = streamed.lines().to_vec();
    let serialized = streamed.into_source();
    assert_eq!(serialized, source.replace("\r\n", "\n").replace('\r', "\n"));
    let mut replay = Markdown::default();
    replay.push(&serialized);
    assert_eq!(replay.lines(), expected);
    assert_eq!(replay.into_source(), serialized);
}
