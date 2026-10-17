use super::super::verification::record;
use super::*;
use proptest::prelude::*;

fn encoded(
    operation: [u8; 16],
    reason: String,
) -> Result<(acyclic_stream::Record, SchedulerEvent)> {
    let event = SchedulerEvent::Rejected {
        operation_id: OperationId::from_bytes(operation),
        reason,
    };
    let canonical = crate::contract::canonical_json_bytes(&event)?;
    let bytes = encode(
        1,
        event.operation_id(),
        "intent-projection",
        *blake3::hash(&canonical).as_bytes(),
        canonical,
        1,
        &mut None,
    )?;
    Ok((record(bytes), event))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A verified frame's replay projection has exactly the digest of its
    // original event; encoding/decoding preserves the complete event.
    #[test]
    fn event_roundtrip_and_replayed_digest_agree(operation in any::<[u8; 16]>(), reason in "(?s).{0,32}") {
        let (record, expected) = encoded(operation, reason).unwrap();
        let (_, _, digest, _, event) = decode(&record, &mut None).unwrap();
        prop_assert_eq!(event, expected);
        let mut work = None;
        let mut quota = CodecQuota::new(&mut work, 0).unwrap();
        prop_assert_eq!(replayed_intent_digest(&record, &mut quota).unwrap(), digest);
        super::super::verification::check(&record.value).unwrap();
    }
}

#[cfg(feature = "filesystem")]
#[test]
fn replayed_intent_does_not_decode_the_event_again()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let (record, _) = encoded([0; 16], "previously verified".repeat(128))?;
    let digest = decode(&record, &mut None)?.2;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(256, 64 * 1024 * 1024);
    let io = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    let mut history = io.history_io();
    {
        let mut work = Some(&mut history as &mut dyn HistoryIo);
        let mut quota = CodecQuota::new(&mut work, 0)?;
        assert_eq!(replayed_intent_digest(&record, &mut quota)?, digest);
    }
    io.receipt().verify(budget)?;
    assert_eq!(io.owned_bytes()?, 0);

    let mut replay_budget = budget;
    replay_budget.items_examined = io.receipt().items_examined;
    let full = crate::filesystem::PreparationIo::from_budget(replay_budget, &cancellation)?;
    let mut history = full.history_io();
    assert!(decode(&record, &mut Some(&mut history as &mut dyn HistoryIo)).is_err());
    full.receipt().verify(replay_budget)?;
    Ok(())
}

#[test]
fn scheduler_intent_fuzz_corpus() -> Result<()> {
    for seed in [
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../harness-fuzz/corpus/scheduler_intents/rejected.json"
        ))
        .as_slice(),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../harness-fuzz/corpus/scheduler_intents/truncated-varint"
        ))
        .as_slice(),
    ] {
        super::super::verification::check(seed)?;
    }
    Ok(())
}
