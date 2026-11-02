//! Allocation bounds for the production streaming input decoder.
#![cfg(feature = "tui")]

use acyclic_harness_tui::term::{Decoder, Multiplexer};

#[test]
fn multiplexer_frames_append_without_temporary_allocations() {
    for mux in [Multiplexer::None, Multiplexer::Tmux, Multiplexer::Screen] {
        let query = b"\x1b]11;?\x07";
        let expected = mux.wrap(query);
        let mut batch = Vec::with_capacity(1 + expected.len());
        batch.push(0x18);
        let allocations = acyclic_test_support::measure(|| mux.wrap_into(query, &mut batch));
        assert_eq!(allocations.count_total, 0, "caller-owned buffer credit");
        assert_eq!(batch[0], 0x18, "retain the original cancellation prefix");
        assert_eq!(&batch[1..], expected);

        batch.truncate(1);
        let allocations = acyclic_test_support::measure(|| {
            batch.extend_from_slice(&mux.wrap(query));
        });
        assert!(
            allocations.count_total > 0,
            "the removed temporary wrapper must cost an allocation"
        );
    }
}

fn count(decoder: &mut Decoder, input: &[u8], broken: bool) -> u64 {
    let mut events = Vec::new();
    let allocations = acyclic_test_support::measure(|| {
        assert!(decoder.feed(input).is_ok());
        if broken {
            for byte in input {
                std::hint::black_box(Box::new(*byte));
            }
        }
        events = decoder.take_events();
    });
    assert_eq!(events.len(), input.len());
    allocations.count_total
}

#[test]
fn ascii_allocations_are_bounded_and_buffers_are_reused() {
    for input in [b"x".repeat(512), b"x".repeat(4096)] {
        let mut decoder = Decoder::default();
        // Cold growth is logarithmic, rather than three allocations per byte.
        assert!(count(&mut decoder, &input, false) <= 16);
        // Only the owned result vector allocates after the decoder is warm.
        assert_eq!(count(&mut decoder, &input, false), 1);
    }
}

#[test]
fn negative_control_detects_per_byte_allocations() {
    assert!(count(&mut Decoder::default(), &[b'x'; 4096], true) > 16);
}
