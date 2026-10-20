//! Shrinking properties for wire decoding,
//! judged by the shared oracle: each invariant has a control injected into
//! the production results, verified with assertion deletion.
#![cfg(not(target_arch = "wasm32"))]

#[path = "../support/wire_input.rs"]
mod wire;
use acyclic_test_support::differential::oracle::{Observer, Oracle};
use proptest::prelude::*;

#[derive(Debug)]
enum WireFault {
    /// The client round trip drops the acknowledged generation.
    DroppedGeneration,
}

fn wire_oracle() -> Oracle<Vec<u8>, WireFault, wire::Seen> {
    let (name, check) = wire::INVARIANTS[0];
    Oracle::declare(
        |data: &Vec<u8>, fault: Option<&WireFault>, see: &mut Observer<'_, wire::Seen>| {
            let tamper: fn(&mut acyclic_harness::wire::ClientFrame) = match fault {
                Some(WireFault::DroppedGeneration) => |frame| {
                    if let Some(acyclic_harness::wire::client_frame::Frame::Acknowledge(ack)) =
                        &mut frame.frame
                    {
                        ack.generation.clear();
                    }
                },
                None => |_| {},
            };
            wire::observe(data, tamper, &mut |seen| see.see(seen))
        },
    )
    .invariant(name, check)
    .control(name, b"generation".to_vec(), WireFault::DroppedGeneration)
    .build()
}

#[test]
fn decoder_seed_corpus() {
    let wire_oracle = wire_oracle();
    for data in [
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/acknowledge").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/admission"),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/truncated-protobuf"),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/invalid-json"),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/oversized-journal-header"),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/utf8"),
        include_bytes!("../../../harness-fuzz/corpus/wire_frames/empty-request"),
    ] {
        // The fuzz entries themselves, then the judged observations.
        wire::check(data);
        wire_oracle.check(&data.to_vec());
    }
}

#[test]
fn wire_decode_arbitrary_bytes() {
    let wire_oracle = wire_oracle();
    proptest!(ProptestConfig::with_cases(64), |(data in prop::collection::vec(any::<u8>(), 0..4096))| {
        wire_oracle.check(&data);
    });
}
