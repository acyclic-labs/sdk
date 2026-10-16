#![no_main]

use acyclic_markdown::Markdown;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let width = u16::from(data.first().copied().unwrap_or(80));
    let rows = usize::from(data.get(1).copied().unwrap_or(0) % 64) + 1;
    let columns = usize::from(data.get(2).copied().unwrap_or(0) % 24) + 1;
    let large_row = usize::from(data.get(3).copied().unwrap_or(0)) % (rows + 1);
    let cell = String::from_utf8_lossy(data.get(4..).unwrap_or_default())
        .replace('\\', "&#92;")
        .replace('|', "&#124;")
        .replace('\r', "&#13;")
        .replace('\n', "&#10;");
    let header = format!(
        "|{}{}|\n|{}\n",
        if large_row == 0 { cell.as_str() } else { "h" },
        "|h".repeat(columns - 1),
        "---|".repeat(columns)
    );
    let body: Vec<_> = (1..=rows)
        .map(|index| {
            if index == large_row {
                format!("|{cell}{}|\n", "|x".repeat(columns - 1))
            } else {
                format!("|{}\n", "x|".repeat(columns))
            }
        })
        .collect();
    let source = format!("{header}{}", body.concat());
    let mut streamed = Markdown::with_width(width);
    let mut prefix = header;
    let mut consumer = Vec::new();
    let mut push = |delta: &str, source: &str| {
        let changed = streamed.push(delta);
        assert!(changed <= streamed.lines().len());
        consumer.truncate(changed);
        consumer.extend_from_slice(&streamed.lines()[changed..]);
        let mut one_shot = Markdown::with_width(width);
        one_shot.push(source);
        assert_eq!(consumer, one_shot.lines());
        let retained: usize = streamed
            .lines()
            .iter()
            .flat_map(|line| &line.spans)
            .map(|span| span.text.capacity())
            .sum();
        assert!(retained <= 12 * (source.len() + (rows + 1) * usize::from(width.max(1))));
    };
    push(&prefix, &prefix);
    for row in body {
        prefix.push_str(&row);
        push(&row, &prefix);
    }
    assert_eq!(streamed.into_source(), source);

    // The shared whitespace shortcut must preserve tabs across arbitrary pushes.
    let mut whitespace = Markdown::with_width(width);
    let mut source = String::from("hello world");
    whitespace.push(&source);
    for byte in data.iter().copied().chain([0]) {
        let delta = ["a", " ", "\t", "\n", "\r", "b"][usize::from(byte % 6)];
        source.push_str(delta);
        whitespace.push(delta);
        let mut one_shot = Markdown::with_width(width);
        one_shot.push(&source);
        assert_eq!(whitespace.lines(), one_shot.lines());
    }
});
