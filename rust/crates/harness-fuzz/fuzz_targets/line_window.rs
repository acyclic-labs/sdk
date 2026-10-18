//! Drives the production streaming line scanner over arbitrary bytes split at
//! arbitrary chunk boundaries, against the whole-text specification.
#![no_main]
#[path = "../../harness/src/filesystem/host/reads/window/lines.rs"]
mod lines;
#[path = "../../harness/src/filesystem/host/reads/window/lines_spec.rs"]
mod spec;
use libfuzzer_sys::fuzz_target;
use lines::LineScan;

fuzz_target!(|data: &[u8]| {
    // Header: offset, limit (0xff = through the end), cut count, then cuts.
    let [offset, limit, count, rest @ ..] = data else {
        return;
    };
    let count = usize::from(*count % 8).min(rest.len());
    let (cuts, text) = rest.split_at(count);
    let mut cuts: Vec<usize> = cuts.iter().map(|cut| usize::from(*cut)).collect();
    cuts.sort_unstable();
    let offset = u64::from(*offset % 16);
    let limit = (*limit != 0xff).then_some(u64::from(*limit % 16));
    let (selected, media) = spec::scan_chunks(text, cuts, offset, limit);
    assert_eq!(media.is_some(), std::str::from_utf8(text).is_ok());
    if let Some(media) = media {
        let binary = text.contains(&0);
        assert_eq!(media == "application/octet-stream", binary);
        assert_eq!(selected, spec::reference(text, offset, limit));
    }
    // Byte-at-a-time chunking classifies identically.
    let mut scan = LineScan::new(offset, limit);
    for byte in text {
        let _ = scan.update(std::slice::from_ref(byte));
    }
    assert_eq!(scan.finish().1, media);
});
