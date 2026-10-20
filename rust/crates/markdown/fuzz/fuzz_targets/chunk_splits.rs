#![no_main]

use acyclic_markdown::Markdown;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut header = [0u8; 8];
    let header_bytes = data.len().min(header.len());
    header
        .get_mut(..header_bytes)
        .unwrap_or_default()
        .copy_from_slice(data.get(..header_bytes).unwrap_or_default());
    let rest = data.get(header_bytes..).unwrap_or_default();
    let flags_count = usize::try_from(u64::from_le_bytes(header))
        .unwrap_or(usize::MAX)
        .min(rest.len());
    let (flags, raw) = rest.split_at(flags_count);
    let source = String::from_utf8_lossy(raw);
    let cuts = source
        .char_indices()
        .skip(1)
        .enumerate()
        .filter_map(|(index, (offset, _))| {
            flags
                .get(index)
                .is_some_and(|flag| flag & 1 != 0)
                .then_some(offset)
        })
        .chain([source.len()]);
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    let mut start = 0;
    for end in cuts {
        let changed = markdown.push(source.get(start..end).expect("UTF-8 boundary"));
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (markdown.lines().slice(changed..).expect("change boundary"))
                .iter()
                .cloned(),
        );
        let mut full = Markdown::default();
        full.push(source.get(..end).expect("UTF-8 prefix"));
        assert_eq!(consumer, full.lines());
        start = end;
    }
    assert_eq!(
        markdown.into_source(),
        source.replace("\r\n", "\n").replace('\r', "\n")
    );
});
