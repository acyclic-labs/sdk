use super::*;
#[cfg(not(target_arch = "wasm32"))]
use super::prepared::record_prepared_work;

pub(super) fn planned_commit_id(
    decision: u64,
    digest: [u8; 32],
    meter: WireMeter<'_>,
) -> std::io::Result<CommitId> {
    let mut hash = Sha256::new();
    hash_update(&mut hash, b"acyclic.stream.commit.v1\0", meter, &mut None)?;
    hash_update(&mut hash, &decision.to_le_bytes(), meter, &mut None)?;
    hash_update(&mut hash, &digest, meter, &mut None)?;
    Ok(CommitId::from_bytes(hash.finalize().into()))
}

pub(crate) fn append_digest(
    request: &AppendRequest,
    meter: WireMeter<'_>,
) -> std::io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    let mut late = None;
    hash_update(&mut hash, b"acyclic.stream.request.v1\0", meter, &mut late)?;
    hash_update(&mut hash, b"append", meter, &mut late)?;
    digest_path(&mut hash, &request.path, meter, &mut late)?;
    hash_update(&mut hash, &request.if_tail.unwrap_or(u64::MAX).to_le_bytes(), meter, &mut late)?;
    digest_records(&mut hash, &request.records, meter, &mut late)?;
    Ok(hash.finalize().into())
}

pub(crate) fn fork_digest(request: &ForkRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.request.v1\0");
    hash.update(b"fork");
    for path in [&request.source, &request.destination] {
        hash.update((path.as_str().len() as u64).to_le_bytes());
        hash.update(path.as_str().as_bytes());
    }
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash.finalize().into()
}

pub(super) fn hash_update(
    hash: &mut Sha256,
    bytes: &[u8],
    meter: WireMeter<'_>,
    late: &mut Option<&mut Option<std::io::Error>>,
) -> std::io::Result<()> {
    if late.is_none() {
        check(meter)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = meter { meter.admit_codec(0, 0, bytes.len() as u64)?; }
    }
    hash.update(bytes);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = meter {
        if let Some(late) = late.as_deref_mut() {
            record_prepared_work(meter, late, 0, bytes.len() as u64);
        } else {
            meter.record_codec(0, 0, bytes.len() as u64)?;
        }
    }
    Ok(())
}

pub(super) fn digest_record_item(meter: WireMeter<'_>, late: &mut Option<&mut Option<std::io::Error>>) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(late) = late.as_deref_mut() {
        record_prepared_work(meter.expect("prepared digest meter"), late, 1, 0);
        return Ok(());
    }
    #[cfg(target_arch = "wasm32")]
    let _ = late;
    record_item(meter)
}

pub(super) fn digest_path(hash: &mut Sha256, path: &StreamPath, meter: WireMeter<'_>, late: &mut Option<&mut Option<std::io::Error>>) -> std::io::Result<()> {
    hash_update(hash, &(path.as_str().len() as u64).to_le_bytes(), meter, late)?;
    hash_update(hash, path.as_str().as_bytes(), meter, late)
}

pub(super) fn digest_records(hash: &mut Sha256, records: &[OwnedBytes], meter: WireMeter<'_>, late: &mut Option<&mut Option<std::io::Error>>) -> std::io::Result<()> {
    hash_update(hash, &(records.len() as u64).to_le_bytes(), meter, late)?;
    for record in records {
        if late.is_none() { admit_item(meter)?; }
        hash_update(hash, &(record.len() as u64).to_le_bytes(), meter, late)?;
        hash_update(hash, record, meter, late)?;
        digest_record_item(meter, late)?;
    }
    Ok(())
}

/// One canonical Commit writer.
pub(crate) fn commit_digest(request: &CommitRequest, meter: WireMeter<'_>, mut late: Option<&mut Option<std::io::Error>>) -> std::io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash_update(&mut hash, b"acyclic.stream.request.v1\0", meter, &mut late)?;
    hash_update(&mut hash, b"commit", meter, &mut late)?;
    for condition in &request.conditions {
        if late.is_none() { admit_item(meter)?; }
        match condition {
            CommitCondition::Tail { path, expected } => {
                hash_update(&mut hash, &[1], meter, &mut late)?;
                digest_path(&mut hash, path, meter, &mut late)?;
                hash_update(&mut hash, &expected.to_le_bytes(), meter, &mut late)?;
            }
            CommitCondition::Absent { path } => {
                hash_update(&mut hash, &[2], meter, &mut late)?;
                digest_path(&mut hash, path, meter, &mut late)?;
            }
        }
        digest_record_item(meter, &mut late)?;
    }
    for mutation in &request.mutations {
        if late.is_none() { admit_item(meter)?; }
        match mutation {
            CommitMutation::Append { path, records } => {
                hash_update(&mut hash, &[1], meter, &mut late)?;
                digest_path(&mut hash, path, meter, &mut late)?;
                digest_records(&mut hash, records, meter, &mut late)?;
            }
            CommitMutation::Fork { source, destination, at_tail, records } => {
                hash_update(&mut hash, &[2], meter, &mut late)?;
                digest_path(&mut hash, source, meter, &mut late)?;
                digest_path(&mut hash, destination, meter, &mut late)?;
                hash_update(&mut hash, &at_tail.to_le_bytes(), meter, &mut late)?;
                digest_records(&mut hash, records, meter, &mut late)?;
            }
        }
        digest_record_item(meter, &mut late)?;
    }
    Ok(hash.finalize().into())
}

pub(super) fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Tail { path, .. } | CommitCondition::Absent { path } => path,
    }
}

pub(super) fn mutation_path(mutation: &CommitMutation) -> &StreamPath {
    match mutation {
        CommitMutation::Fork { destination, .. } => destination,
        CommitMutation::Append { path, .. } => path,
    }
}
