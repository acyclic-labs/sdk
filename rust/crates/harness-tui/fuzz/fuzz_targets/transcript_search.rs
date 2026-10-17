#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some(at) = data.iter().position(|byte| *byte == 0) else {
        return;
    };
    let (Ok(query), Ok(text)) = (
        std::str::from_utf8(&data[..at]),
        std::str::from_utf8(&data[at + 1..]),
    ) else {
        return;
    };
    let found = acyclic_harness_tui::transcript_matches(text, query);
    assert!(
        found
            .iter()
            .all(|range| !range.is_empty() && text.get(range.clone()).is_some())
    );
    assert!(
        found
            .windows(2)
            .all(|pair| pair[0].start <= pair[1].start && pair[0] != pair[1])
    );
    let mut projection = acyclic_harness_tui::WrappedLines::default();
    projection.update(
        &[acyclic_harness_tui::ratatui::text::Line::raw(text)],
        0,
        80,
    );
    let mut search = acyclic_harness_tui::TranscriptSearch::default();
    search.update(&projection, query, 0, Default::default());
    while search.pending() {
        search.advance(&projection, 31);
    }
    assert_eq!(
        search.count(),
        acyclic_harness_tui::transcript_matches(projection.text(), query).len()
    );
    // Answer and tool caches remain separate; their borrowed source must match
    // an independently joined transcript, including matches across the seam.
    let split = text
        .char_indices()
        .map(|(at, _)| at)
        .take_while(|at| *at <= text.len() / 2)
        .last()
        .unwrap_or(0);
    let mut answer = acyclic_harness_tui::WrappedLines::default();
    let mut tools = acyclic_harness_tui::WrappedLines::default();
    answer.update(
        &[acyclic_harness_tui::ratatui::text::Line::raw(
            &text[..split],
        )],
        0,
        80,
    );
    tools.update(
        &[acyclic_harness_tui::ratatui::text::Line::raw(
            &text[split..],
        )],
        0,
        80,
    );
    let expected = format!("{}\n{}", answer.text(), tools.text());
    let rows = acyclic_harness_tui::TranscriptRows::new(&answer, &tools);
    let mut search = acyclic_harness_tui::TranscriptSearch::default();
    search.update(rows, query, 0, Default::default());
    while search.pending() {
        search.advance(rows, 31);
    }
    assert_eq!(
        search.count(),
        acyclic_harness_tui::transcript_matches(&expected, query).len()
    );
    if query.is_empty() {
        assert!(found.is_empty());
    }
});
