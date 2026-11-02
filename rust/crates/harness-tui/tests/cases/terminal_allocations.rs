//! Allocation bounds for the production streaming input decoder.
#![cfg(feature = "tui")]

use std::{io, time::Instant};

use acyclic_harness_tui::term::{
    Decoder, Environment, ModeState, Multiplexer, Probe, ProbeIo, detect,
};

#[test]
fn csi_replies_borrow_parameters_and_reuse_frame_storage() {
    let replies = b"\x1b[?62;1;4;6;22c\x1b[?3u\x1b[?2026;2$y";
    let mut decoder = Decoder::default();
    decoder.feed(replies).unwrap();
    for chunk in [1, replies.len()] {
        let allocations = acyclic_test_support::measure(|| {
            for bytes in replies.chunks(chunk) {
                decoder.feed(bytes).unwrap();
            }
        });
        assert_eq!(allocations.count_total, 0);
        assert!(decoder.replies().sentinel);
        assert_eq!(decoder.replies().sixel, Some(true));
        assert_eq!(decoder.replies().keyboard_flags, Some(3));
        assert_eq!(decoder.replies().modes.get(&2026), Some(&ModeState::Reset));
        assert!(decoder.take_events().is_empty());
    }

    let admitted = decoder.replies().clone();
    for malformed in [
        b"\x1b[?0;9u".as_slice(),
        b"\x1b[?2026;1;9$y",
        b"\x1b[?2026;1:9$y",
        b"\x1b[?60000;1$y",
        b"\x1b[?2026;9$y",
        b"\x1b[?62;1:4c",
    ] {
        decoder.feed(malformed).unwrap();
        assert_eq!(decoder.replies(), &admitted);
    }
}

#[test]
fn passthrough_replies_reuse_storage_and_refuse_embedded_nesting() {
    // Generated protocol operands exercise the shared wire framer; these do
    // not claim to be a captured response from a particular terminal.
    let body = b"\x1b[?62;4c\x1b_Ga=q,x=1,i=314159;OK\x1b\\";
    let wrapped = Multiplexer::Tmux.wrap(body);
    for chunk in [1, 2, 7, wrapped.len()] {
        let mut decoder = Decoder::default();
        // Warm both retained buffers; the first outer envelope is larger
        // than the inner frame and can grow either alternating allocation.
        decoder.feed(&wrapped).unwrap();
        decoder.feed(&wrapped).unwrap();
        let measured = acyclic_test_support::measure(|| {
            for bytes in wrapped.chunks(chunk) {
                decoder.feed(bytes).unwrap();
            }
        });
        assert_eq!(measured.count_total, 0, "chunk {chunk}");
        assert!(decoder.replies().sentinel);
        assert_eq!(decoder.replies().sixel, Some(true));
        assert_eq!(decoder.replies().graphics, Some(true));
        assert!(decoder.take_events().is_empty());

        // Each decoded field still needs the exact original query ID. A
        // negative response settles the same capability without becoming input.
        decoder
            .feed(&Multiplexer::Tmux.wrap(b"\x1b_Gi=3141590;OK\x1b\\"))
            .unwrap();
        assert_eq!(decoder.replies().graphics, Some(true));
        decoder.take_events();
        decoder
            .feed(&Multiplexer::Tmux.wrap(b"\x1b_Gi=314159;ENOTSUP\x1b\\"))
            .unwrap();
        assert_eq!(decoder.replies().graphics, Some(false));
        assert!(decoder.take_events().is_empty());
    }

    let nested = Multiplexer::Tmux.wrap(b"\x1b_Gi=314159;OK\x1b\\");
    for prefix in [b"".as_slice(), b"\x1b[?62;4c"] {
        let mut body = prefix.to_vec();
        body.extend_from_slice(&nested);
        body.extend_from_slice(b"\x1b[?0u");
        let wrapped = Multiplexer::Tmux.wrap(&body);
        for chunk in [1, 7, wrapped.len()] {
            let mut decoder = Decoder::default();
            for bytes in wrapped.chunks(chunk) {
                decoder.feed(bytes).unwrap();
            }
            assert_eq!(decoder.replies().graphics, None, "nested reply refused");
            assert_eq!(decoder.replies().keyboard_flags, Some(0), "body progresses");
            assert_eq!(decoder.replies().sentinel, !prefix.is_empty());
            assert!(decoder.take_events().is_empty());
        }
    }

    // Omitting the outer envelope recreates the old recursive interpretation:
    // the same inner graphics reply would be accepted by the same decoder.
    let mut unwrapped = Decoder::default();
    unwrapped.feed(&nested).unwrap();
    assert_eq!(unwrapped.replies().graphics, Some(true));
}

#[test]
fn multiplexer_frames_append_without_temporary_allocations() {
    struct FailedSend<'a> {
        batch: &'a [u8],
        cancel: &'a [u8],
        sent: u8,
        before_cancel: u64,
        cancel_allocations: u64,
    }
    impl ProbeIo for FailedSend<'_> {
        fn send(&mut self, bytes: &[u8], _: Instant) -> io::Result<()> {
            let count = acyclic_test_support::current_allocations()
                .expect("detect runs inside the allocation measurement")
                .count_total;
            self.sent += 1;
            match self.sent {
                1 => {
                    assert_eq!(bytes, self.batch);
                    self.before_cancel = count;
                    Err(io::ErrorKind::BrokenPipe.into())
                }
                2 => {
                    assert_eq!(bytes, self.cancel);
                    self.cancel_allocations = count - self.before_cancel;
                    Ok(())
                }
                _ => panic!("startup failure must return after its cancellation send"),
            }
        }

        fn receive(&mut self, _: &mut [u8], _: Instant) -> io::Result<usize> {
            panic!("startup failure cannot wait for a reply")
        }
    }

    for (mux, marker, batch_allocations, cancel_allocations) in [
        (Multiplexer::None, None, 20, 1),
        (Multiplexer::Tmux, Some("TMUX"), 21, 3),
        (Multiplexer::Screen, Some("STY"), 20, 2),
    ] {
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

        let mut startup = Vec::new();
        let allocations = acyclic_test_support::measure(|| startup = Probe::batch(mux));
        // Fourteen numeric mode formats plus the initial batch and its growth:
        // six buffer allocations direct/screen, seven for the larger tmux batch.
        assert_eq!(allocations.count_total, batch_allocations, "{mux:?} batch");
        let mut cancel = vec![0x18];
        if mux != Multiplexer::None {
            mux.wrap_into(b"\x18", &mut cancel);
        }
        let mut transport = FailedSend {
            batch: &startup,
            cancel: &cancel,
            sent: 0,
            before_cancel: 0,
            cancel_allocations: 0,
        };
        let mut env = Environment::default();
        if let Some(marker) = marker {
            env.0
                .insert(marker.into(), "generated transport selection".into());
        }
        acyclic_test_support::measure(|| {
            assert_eq!(
                detect(&mut transport, &env).err().unwrap().kind(),
                io::ErrorKind::BrokenPipe,
            );
        });
        assert_eq!(transport.sent, 2);
        // Count only work between the failed original send and cancellation:
        // CAN storage plus its existing growth, excluding startup formatting.
        assert_eq!(
            transport.cancel_allocations, cancel_allocations,
            "{mux:?} cancel"
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
