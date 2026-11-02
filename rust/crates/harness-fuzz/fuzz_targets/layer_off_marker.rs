#![no_main]

use acyclic_harness_core::layers::{OffMarker, is_off};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    // The production classifier must preserve the whole-file grammar across
    // every chunk boundary, including a marker prefix with a non-marker tail.
    let expected = input.trim_ascii() == b"off: true";
    assert_eq!(is_off(input), expected);
    for size in [1, 2, 8, 9, 32, 1024, 16_384] {
        let mut marker = OffMarker::default();
        for chunk in input.chunks(size) {
            if !marker.push(chunk) {
                break;
            }
        }
        assert_eq!(marker.finish(), expected);
    }
});
