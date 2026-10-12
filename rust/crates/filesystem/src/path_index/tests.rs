use super::*;
use crate::kernel::{FileKind, FilePayload};
use crate::{CancellationToken, Digest, ObjectId, WorkBudget};

fn fixture() -> (GenerationId, Vec<FileId>, Vec<(FileId, Vec<(NamespacePath, FileRecord)>)>) {
    let generation = GenerationId::new(Digest::from_bytes([7; 32]));
    let id = FileId::from_bytes([3; 16]);
    let limits = VolumeLimits::default();
    let path = NamespacePath::new(vec![
        LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'x'], limits.maximum_component_bytes).unwrap(),
        LogicalName::new(NameEncoding::WindowsUtf16Le, vec![0, 0xd8], limits.maximum_component_bytes).unwrap(),
    ], limits).unwrap();
    let record = FileRecord { file_id: id, kind: FileKind::Fifo, link_count: 1,
        metadata: ObjectId { kind: crate::storage::ObjectKind::Metadata, digest: Digest::from_bytes([9; 32]) },
        payload: FilePayload::Empty };
    (generation, vec![id, FileId::from_bytes([4; 16])], vec![(id, vec![(path, record)])])
}

#[test]
fn finite_codec_preserves_opaque_names_absence_and_candidate_identity() -> Result<(), FsError> {
    let token = CancellationToken::new();
    let budget = crate::performance::work_budget(65_536, 8 * 1024 * 1024);
    let mut accounting = KernelAccounting::standalone(budget, &token);
    let (generation, ids, records) = fixture();
    let (bytes, guard) = encode_records(generation, &ids, &records, &mut accounting)?;
    let bytes = bytes.unwrap();
    let decoded = decode(&bytes, generation, &ids, VolumeLimits::default(), &mut accounting)?.0.unwrap();
    assert_eq!(decoded, vec![(ids[0], records[0].1[0].0.clone())]);
    accounting.spent().verify(budget)?;
    assert!(decode(b"corrupt", generation, &ids, VolumeLimits::default(), &mut accounting)?.0.is_none());
    assert!(decode(&bytes, GenerationId::new(Digest::from_bytes([8; 32])), &ids,
        VolumeLimits::default(), &mut accounting)?.0.is_none());
    assert!(decode(&bytes, generation, &ids, VolumeLimits { maximum_path_depth: 1,
        ..VolumeLimits::default() }, &mut accounting)?.0.is_none());
    let table = ObjectId { kind: crate::storage::ObjectKind::FileTablePage, digest: Digest::from_bytes([1; 32]) };
    let first = cache_key(generation, ids[0], table, &ids, &mut accounting)?;
    assert_ne!(first, cache_key(generation, ids[1], table, &ids, &mut accounting)?);
    assert_ne!(first, cache_key(generation, ids[0], ObjectId { digest: Digest::from_bytes([2; 32]), ..table },
        &ids, &mut accounting)?);
    drop(decoded);
    drop(bytes);
    drop(guard);
    Ok(())
}

#[test]
fn cache_eviction_and_denial_preserve_admitted_values() -> Result<(), FsError> {
    let token = CancellationToken::new();
    let index = MemoryGenerationPathIndex::default();
    for ordinal in 0..=MAXIMUM_ENTRIES {
        let mut key = [0; 32];
        key[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
        index.store(key, b"value", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token))?;
    }
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token);
    assert!(index.load([0; 32], &mut accounting)?.0.is_none());
    let key = [9; 32];
    index.store(key, b"original", &mut accounting)?;
    let denied = WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED };
    assert!(index.store(key, b"replacement", &mut KernelAccounting::standalone(denied, &token)).is_err());
    assert_eq!(index.load(key, &mut accounting)?.0.as_deref(), Some(b"original".as_slice()));
    index.store([0xff; 32], &vec![0; MAXIMUM_VALUE_BYTES + 1], &mut accounting)?;
    assert!(index.load([0xff; 32], &mut accounting)?.0.is_none());
    token.cancel();
    assert!(index.load(key, &mut accounting).is_err());
    Ok(())
}

#[test]
fn denied_codec_and_cache_restore_the_callers_residency() -> Result<(), FsError> {
    let token = CancellationToken::new();
    let (generation, ids, records) = fixture();
    let bytes = encode_records(generation, &ids, &records,
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token))?.0.unwrap();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    ledger.claim_bytes(11, 0, &mut work, WorkBudget::UNBOUNDED)
        .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
    for budget in [WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }]
    {
        work = WorkCounters::UNCHARGED;
        assert!(decode(&bytes, generation, &ids, VolumeLimits::default(),
            &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &token)).is_err());
        assert_eq!(ledger.live_bytes(), 11);
    }
    let index = MemoryGenerationPathIndex::default();
    index.store([1; 32], b"cached", &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token))?;
    work = WorkCounters::UNCHARGED;
    assert!(index.load([1; 32], &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work,
        WorkBudget { peak_allocation_bytes: 11, ..WorkBudget::UNBOUNDED }, &token)).is_err());
    assert_eq!(ledger.live_bytes(), 11);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_cache_and_codec_retire_physical_buffers_with_their_original_guards() -> Result<(), FsError> {
    let token = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &token);
    let index = MemoryGenerationPathIndex::default();
    index.store([1; 32], b"original", &mut accounting)?;
    let resident = original.live_allocation_bytes();
    let (bytes, guard) = index.load([1; 32], &mut accounting)?;
    assert_eq!(bytes.as_deref(), Some(b"original".as_slice()));
    assert!(original.live_allocation_bytes() > resident);
    drop(bytes); drop(guard);
    assert_eq!(original.live_allocation_bytes(), resident);
    let (generation, ids, records) = fixture();
    let (bytes, guard) = encode_records(generation, &ids, &records, &mut accounting)?;
    let bytes = bytes.unwrap();
    let encoded = original.live_allocation_bytes();
    let (paths, paths_guard) = decode(&bytes, generation, &ids, VolumeLimits::default(), &mut accounting)?;
    assert_eq!(paths.unwrap(), vec![(ids[0], records[0].1[0].0.clone())]);
    drop(paths_guard);
    assert_eq!(original.live_allocation_bytes(), encoded);
    drop(bytes); drop(guard); drop(index);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &token,
    ).map_err(|(error, _)| error)?;
    let baseline = denied.live_allocation_bytes();
    let bytes = encode_records(generation, &ids, &records,
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token))?.0.unwrap();
    let result = decode(&bytes, generation, &ids, VolumeLimits::default(),
        &mut KernelAccounting::native(&denied, &token));
    assert!(result.is_err());
    drop(result);
    assert_eq!(denied.live_allocation_bytes(), baseline);
    Ok(())
}
