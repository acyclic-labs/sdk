//! Shrinking properties for journal decoding and rejected-input cleanup,
//! judged by the shared oracle: each invariant has a control injected into
//! the production results, verified with assertion deletion.
#![cfg(not(target_arch = "wasm32"))]

#[path = "support/journal_input.rs"]
mod journal;
#[path = "support/wire_input.rs"]
mod wire;
use acyclic_harness::client::SessionRecord;
use acyclic_test_support::differential::oracle::{Observer, Oracle};
use journal::{Loaded, Stage};
use proptest::prelude::*;
use std::{io::Write as _, path::Path};

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

#[derive(Debug)]
enum JournalFault {
    /// The second repair of arbitrary bytes gains a record.
    UnstableRepair,
    /// A framed record loads as nothing.
    DroppedFramed,
    /// An oversized header leaves bytes behind.
    KeptOversized,
    /// Recovery drops the last committed record.
    DroppedRecord,
    /// The cut journal keeps a trailing byte after repair.
    UnrepairedTail,
    /// A corrupt record loads as empty.
    AcceptedCorrupt,
}

fn tail(path: &Path) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(&[0]).unwrap();
}

fn journal_tamper(fault: Option<&JournalFault>) -> journal::Tamper {
    match fault {
        None => |_, _, _| {},
        Some(JournalFault::UnstableRepair) => |stage, _, loaded: &mut Loaded| {
            if stage == Stage::Reload
                && let Ok(records) = loaded
            {
                records.push(SessionRecord::default());
            }
        },
        Some(JournalFault::DroppedFramed) => |stage, _, loaded: &mut Loaded| {
            if stage == Stage::Framed {
                *loaded = Ok(Vec::new());
            }
        },
        Some(JournalFault::KeptOversized) => |stage, path, _| {
            if stage == Stage::Oversized {
                tail(path);
            }
        },
        Some(JournalFault::DroppedRecord) => |stage, _, loaded: &mut Loaded| {
            if stage == Stage::Committed
                && let Ok(records) = loaded
            {
                records.pop();
            }
        },
        Some(JournalFault::UnrepairedTail) => |stage, path, _| {
            if stage == Stage::Cut {
                tail(path);
            }
        },
        Some(JournalFault::AcceptedCorrupt) => |stage, _, loaded: &mut Loaded| {
            if stage == Stage::Corrupt {
                *loaded = Ok(Vec::new());
            }
        },
    }
}

fn journal_oracle() -> Oracle<Vec<u8>, JournalFault, journal::Seen> {
    let declared = Oracle::declare(
        |data: &Vec<u8>, fault: Option<&JournalFault>, see: &mut Observer<'_, journal::Seen>| {
            journal::observe(data, journal_tamper(fault), &mut |seen| see.see(seen))
        },
    );
    journal::INVARIANTS
        .into_iter()
        .fold(declared, |oracle, (name, check)| {
            oracle.invariant(name, check)
        })
        .control("idempotent", Vec::new(), JournalFault::UnstableRepair)
        .control("framed", Vec::new(), JournalFault::DroppedFramed)
        .control("oversized", b"record".to_vec(), JournalFault::KeptOversized)
        .control("committed", b"record".to_vec(), JournalFault::DroppedRecord)
        .control("repaired", b"record".to_vec(), JournalFault::UnrepairedTail)
        .control("corrupt", b"record".to_vec(), JournalFault::AcceptedCorrupt)
        .build()
}

#[test]
fn decoder_seed_corpus() {
    let (wire_oracle, journal_oracle) = (wire_oracle(), journal_oracle());
    for data in [
        include_bytes!("../../harness-fuzz/corpus/wire_frames/acknowledge").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/admission"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/truncated-protobuf"),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/invalid-json"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/oversized-journal-header"),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/utf8"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/empty-request"),
    ] {
        // The fuzz entries themselves, then the judged observations.
        wire::check(data);
        journal::check(data);
        wire_oracle.check(&data.to_vec());
        journal_oracle.check(&data.to_vec());
    }
}

#[test]
fn wire_and_journal_decode_arbitrary_bytes() {
    let (wire_oracle, journal_oracle) = (wire_oracle(), journal_oracle());
    proptest!(ProptestConfig::with_cases(64), |(data in prop::collection::vec(any::<u8>(), 0..4096))| {
        wire_oracle.check(&data);
        journal_oracle.check(&data);
    });
}
