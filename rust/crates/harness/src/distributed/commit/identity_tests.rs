use super::*;
use acyclic_stream::{CommitCondition, CommitId, CommitMutation, CommitRequest, StreamPath};
use proptest::prelude::*;

fn prepared() -> CommitRequest {
    CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: StreamPath::new("o").unwrap(),
                expected: 1,
            },
            CommitCondition::Absent {
                path: StreamPath::new("e").unwrap(),
            },
            CommitCondition::Head {
                path: StreamPath::new("f").unwrap(),
                expected: 1,
                commit_id: CommitId::from_bytes([2; 32]),
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: StreamPath::new("e").unwrap(),
                records: vec![b"a".to_vec().into()],
            },
            CommitMutation::Append {
                path: StreamPath::new("f").unwrap(),
                records: vec![b"b".to_vec().into()],
            },
        ],
        idempotency_key: StreamIdempotencyKey::new(vec![1; 32]).unwrap(),
    }
}

fn key(request: CommitRequest) -> [u8; 32] {
    let mut work = None;
    let mut request = (request, CodecQuota::from_retained(&mut work, 0));
    finalize_journal_request(&mut request, 0).unwrap();
    request.0.idempotency_key.as_bytes().try_into().unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn every_prepared_condition_and_record_changes_attempt_identity(byte in 3u8..=255) {
        for field in 0usize..6 {
        let original = prepared();
        let mut changed = original.clone();
        match field {
            0 => changed.conditions[0] = CommitCondition::Tail { path: StreamPath::new("o").unwrap(), expected: u64::from(byte) },
            1 => changed.conditions[1] = CommitCondition::Tail { path: StreamPath::new("e").unwrap(), expected: u64::from(byte) },
            2 => changed.conditions[2] = CommitCondition::Head { path: StreamPath::new("f").unwrap(), expected: u64::from(byte), commit_id: CommitId::from_bytes([2;32]) },
            3 => changed.conditions[2] = CommitCondition::Head { path: StreamPath::new("f").unwrap(), expected: 1, commit_id: CommitId::from_bytes([byte;32]) },
            index => {
                let CommitMutation::Append { records, .. } = &mut changed.mutations[index - 4] else { unreachable!() };
                records[0] = vec![byte, 0].into();
            }
        }
        prop_assert_ne!(key(original.clone()), key(changed.clone()));
        prop_assert_eq!(original.idempotency_key, changed.idempotency_key, "old caller-only identity must alias the changed transaction");
        }
    }
}

#[test]
fn finalization_funds_all_binary_hash_inputs_and_key_allocations() {
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1 << 24),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
    let mut history = io.history_io();
    let mut observed: Option<&mut dyn HistoryIo> = Some(&mut history);
    let mut request = (prepared(), CodecQuota::from_retained(&mut observed, 0));
    let before = io.receipt();
    finalize_journal_request(&mut request, 0).unwrap();
    let after = io.receipt();
    // SHA256: 32 domain bytes + 78 condition bytes + 54 mutation bytes.
    // BLAKE3: 31 domain bytes + 8 length + 32 caller bytes + 32 digest bytes.
    assert_eq!(after.bytes_hashed - before.bytes_hashed, 267);
    assert_eq!(after.bytes_copied - before.bytes_copied, 32);
    assert_eq!(after.bytes_encoded - before.bytes_encoded, 0);
    assert_eq!(
        after.allocation_operations - before.allocation_operations,
        2
    );
}
