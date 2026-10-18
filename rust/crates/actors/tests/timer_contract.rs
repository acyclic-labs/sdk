//! Timer source compatibility, stable identities and inbox wake behavior.
use acyclic_actors::timers::{MAX_TIMER_RECORD_BYTES, TimerError, TimerRecord, fire, schedule};
use acyclic_stream::{MemoryStream, StreamError, StreamPath, StreamProvider};
use futures::{StreamExt, poll};
use proptest::prelude::*;
#[path = "support/timer_oracle.rs"]
mod oracle;

fn timer(task: [u8; 16], operation: [u8; 16], deadline: u64) -> Result<TimerRecord, TimerError> {
    TimerRecord::new(
        uuid::Uuid::from_bytes(task).to_string(),
        uuid::Uuid::from_bytes(operation).to_string(),
        deadline,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Canonical source JSON round-trips at every u64 deadline without rounding.
    #[test]
    fn timer_codec_round_trips(task in any::<[u8;16]>(), operation in any::<[u8;16]>(), deadline in 1u64..=u64::MAX) {
        let record = timer(task, operation, deadline)?;
        oracle::check(&record);
        let mut checked = None;
        let allocation = acyclic_test_support::measure(|| checked = Some(record.validate()));
        prop_assert_eq!(allocation.count_total, 0, "UUID validation uses stack buffers");
        checked.ok_or_else(|| TestCaseError::fail("validation ran"))??;
        let mut encoded = None;
        let allocation = acyclic_test_support::measure(|| encoded = Some(record.encode()));
        prop_assert_eq!((allocation.count_total, allocation.bytes_total), (1, MAX_TIMER_RECORD_BYTES as u64));
        let bytes = encoded.ok_or_else(|| TestCaseError::fail("encoding ran"))??;
        prop_assert!(bytes.len() <= MAX_TIMER_RECORD_BYTES);
        prop_assert_eq!(TimerRecord::decode(&bytes)?, record);
    }

    /// Timer identity contains both IDs; deadlines never change retry identity.
    #[test]
    fn identities_are_stable_and_separate(task in any::<[u8;16]>(), operation in any::<[u8;16]>(), other in any::<[u8;16]>(), deadline in 1u64..=u64::MAX) {
        let original = timer(task, operation, deadline)?;
        let changed_deadline = timer(task, operation, 1)?;
        prop_assert_eq!(original.fire_key()?, changed_deadline.fire_key()?);
        let neighbor = timer(task, other, deadline)?;
        prop_assert_eq!(original.timer_id()? == neighbor.timer_id()?, operation == other);
        prop_assert_eq!(original.fire_key()? == neighbor.fire_key()?, operation == other);
        prop_assert!(original.fire_key()?.as_bytes().starts_with(b"timer-fire:harness/v2/timers/"));
        prop_assert!(original.fire_key()?.as_bytes().len() > 32, "cannot alias native 32-byte publication keys");
    }

    /// Every admitted untrusted byte string re-encodes to the same semantic record.
    #[test]
    fn accepted_bytes_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        if let Ok(record) = TimerRecord::decode(&bytes) {
            prop_assert_eq!(TimerRecord::decode(&record.encode()?)?, record);
        }
    }
}

#[test]
#[should_panic(expected = "positive timer deadline")]
fn oracle_detects_bypassed_deadline_admission() {
    let mut record = timer([1; 16], [2; 16], 1).expect("valid identity");
    record.deadline_unix_ms = 0;
    oracle::check(&record);
}

#[tokio::test]
async fn scheduling_delegates_validated_ids_to_the_scoped_publisher()
-> Result<(), Box<dyn std::error::Error>> {
    let record = timer([1; 16], [2; 16], 20)?;
    let result = schedule(
        async |task, operation, deadline| {
            Ok::<_, TimerError>((task.into_bytes(), operation.into_bytes(), deadline))
        },
        &record,
    )
    .await?;
    assert_eq!(result, ([1; 16], [2; 16], 20));
    let invalid = TimerRecord {
        deadline_unix_ms: 0,
        ..record
    };
    let mut called = false;
    let result = schedule(
        async |_, _, _| {
            called = true;
            Ok::<_, TimerError>(())
        },
        &invalid,
    )
    .await;
    assert_eq!(result, Err(TimerError::Deadline));
    assert!(!called);
    Ok(())
}

#[tokio::test]
async fn due_fire_wakes_follow_once_and_rejects_identity_reuse()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let record = timer([1; 16], [2; 16], 20)?;
    let inbox = StreamPath::new("actors/agent/inbox")?;
    provider
        .append(acyclic_stream::AppendRequest {
            path: inbox.clone(),
            records: vec![vec![0].into()],
            if_tail: None,
            idempotency_key: Some(acyclic_stream::IdempotencyKey::new(
                b"initialize-inbox".to_vec(),
            )?),
        })
        .await?;
    let mut follow = provider.follow(inbox.clone(), 1).await?;
    let mut next = Box::pin(follow.next());
    assert!(poll!(&mut next).is_pending());
    assert!(fire(&provider, &record, inbox.clone(), 19).await?.is_none());
    assert!(poll!(&mut next).is_pending());
    let first = fire(&provider, &record, inbox.clone(), 20).await?;
    let retry = fire(&provider, &record, inbox.clone(), u64::MAX).await?;
    assert_eq!(first, retry);
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), next)
        .await?
        .ok_or("follow ended")??;
    assert_eq!(TimerRecord::decode(&delivered.value)?, record);
    assert_eq!(provider.tail(inbox.clone()).await?, 2);
    let changed = TimerRecord {
        deadline_unix_ms: 21,
        ..record.clone()
    };
    assert_eq!(
        fire(&provider, &changed, inbox.clone(), 21).await,
        Err(TimerError::Stream(StreamError::IdempotencyMismatch))
    );
    assert_eq!(
        fire(
            &provider,
            &record,
            StreamPath::new("actors/other/inbox")?,
            20
        )
        .await,
        Err(TimerError::Stream(StreamError::IdempotencyMismatch))
    );
    assert_eq!(provider.tail(inbox).await?, 2);
    Ok(())
}

#[test]
fn malformed_records_and_checked_corpus() -> Result<(), Box<dyn std::error::Error>> {
    let original = timer([1; 16], [2; 16], u64::MAX)?;
    for invalid in [
        TimerRecord {
            deadline_unix_ms: 0,
            ..original.clone()
        },
        TimerRecord {
            task_id: "01010101010101010101010101010101".into(),
            ..original
        },
    ] {
        assert!(invalid.encode().is_err());
    }
    assert!(TimerRecord::decode(&vec![b' '; MAX_TIMER_RECORD_BYTES + 1]).is_err());
    let mut valid = 0;
    let mut invalid = 0;
    for entry in std::fs::read_dir(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/timer_record"),
    )? {
        let entry = entry?;
        let name = entry.file_name();
        let expected = match name.to_str() {
            Some("valid.json") => true,
            Some("zero.json" | "unknown.json" | "duplicate.json") => false,
            _ => return Err("unclassified timer corpus seed".into()),
        };
        let decoded = TimerRecord::decode(&std::fs::read(entry.path())?);
        assert_eq!(decoded.is_ok(), expected, "{}", name.to_string_lossy());
        match decoded {
            Ok(record) => {
                assert_eq!(TimerRecord::decode(&record.encode()?)?, record);
                valid += 1;
            }
            Err(_) => invalid += 1,
        }
    }
    assert_eq!((valid, invalid), (1, 3));
    Ok(())
}

#[test]
fn generated_timer_authoring_has_no_drift() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../typescript/packages/actors/src/generated/timers.ts");
    assert_eq!(
        std::fs::read_to_string(path)?,
        acyclic_actors::timers::typescript()
    );
    Ok(())
}
