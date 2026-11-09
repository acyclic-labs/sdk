use acyclic_harness_client::ui::blocks;

pub fn check(bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(parsed) = blocks::parse(text)
    {
        let mut end = 0;
        for block in parsed {
            assert!(block.range.start >= end);
            assert!(text.get(block.range.clone()).is_some());
            assert!(!block.kind.is_empty());
            end = block.range.end;
        }
    }
    // Independent writer oracle: arbitrary bytes become inert literal text;
    // neither a parser that rejects everything nor one that loses future kinds
    // can satisfy this, even if the unconstrained fuzz input was malformed.
    if bytes.len() > blocks::MAX_DOCUMENT_BYTES / 4 {
        return;
    }
    let mut body = String::new();
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut body, "{byte:02x}").expect("string write");
    }
    let document = format!(
        "````markdown\n```input hidden id=no\nignored\n```\n````\n\n~~~input future-kind id=answer recommended=b\n{body}\n~~~\n"
    );
    let parsed = blocks::parse(&document).expect("writer's valid CommonMark input");
    assert_eq!(parsed.len(), 1, "only the actual input fence is a question");
    let block = parsed.first().expect("one block");
    assert_eq!(block.kind, "future-kind");
    assert_eq!(
        block.attributes.get("id").map(String::as_str),
        Some("answer")
    );
    assert_eq!(block.body, format!("{body}\n"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn ui_blocks_retained_corpus() {
        for seed in [
            include_bytes!("corpus/ui_blocks/choose").as_slice(),
            include_bytes!("corpus/ui_blocks/unknown"),
            include_bytes!("corpus/ui_blocks/inert"),
            include_bytes!("corpus/ui_blocks/unclosed"),
            b"\xff\x00\r\n",
        ] {
            super::check(seed);
        }
    }
}
