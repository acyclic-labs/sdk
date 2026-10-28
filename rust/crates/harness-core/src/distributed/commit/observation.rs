use super::*;

pub(in crate::distributed) fn observe_commit(
    outcome: &acyclic_stream::CommitOutcome,
    acknowledgment: (u64, u64),
    phase: Option<&mut CommitPhase>,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    use acyclic_stream::CommitOutcome;
    if let Some(phase) = phase {
        *phase = match outcome {
            acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
            acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Uncommitted,
        };
    }
    let Some(observer) = quota.work().as_deref_mut() else {
        return Ok(());
    };
    let CommitOutcome::Committed(receipt) = outcome else {
        return Ok(());
    };
    let result = observer.acknowledged(acknowledgment.0, acknowledgment.1);
    if !operations::original_native_decode_mode(observer) {
        let bytes = (receipt.appends.capacity()
            * std::mem::size_of::<acyclic_stream::AppendRange>()
            + receipt.forks.capacity() * std::mem::size_of::<acyclic_stream::ForkReceipt>())
            as u64;
        observer.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    result
}

pub(super) fn observe_envelope(
    envelope: &acyclic_stream::CommittedEnvelope,
    phase: Option<&mut CommitPhase>,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    use acyclic_stream::CommittedMutation;
    if let Some(phase) = phase {
        *phase = CommitPhase::Committed;
    }
    let Some(observer) = quota.work().as_deref_mut() else {
        return Ok(());
    };
    let native = operations::original_native_decode_mode(observer);
    let mut result = Ok(());
    let mut retained = 0;
    let mut slots = envelope.mutations.capacity() * std::mem::size_of::<CommittedMutation>();
    for mutation in &envelope.mutations {
        let records = match mutation {
            CommittedMutation::Append(append) => &append.records,
            CommittedMutation::Fork(fork) => &fork.records,
        };
        slots += records.capacity() * std::mem::size_of::<acyclic_stream::Record>();
        for record in records {
            let bytes = record.payload_bytes() as u64;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let received = observer.receive_native_record(bytes);
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let received = observer.receive_record(bytes);
            if received.is_ok() && !native {
                retained += bytes;
            }
            result = result.and(received);
        }
    }
    if !native {
        let held = observer.retain_owned(slots as u64);
        if held.is_ok() {
            retained += slots as u64;
        }
        result = result.and(held);
    }
    quota.adopt(retained)?;
    result
}

pub(super) fn receipt(
    envelope: &acyclic_stream::CommittedEnvelope,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<acyclic_stream::CommitReceipt> {
    let append_count = envelope
        .mutations
        .iter()
        .filter(|mutation| matches!(mutation, acyclic_stream::CommittedMutation::Append(_)))
        .count();
    let mut appends = Vec::new();
    let mut forks = Vec::new();
    quota.reserve(append_count, &mut appends)?;
    quota.reserve(envelope.mutations.len() - append_count, &mut forks)?;
    acyclic_stream::CommitReceipt::project(envelope, appends, forks, |bytes, copied| {
        if !copied && let Some(io) = quota.work().as_deref_mut() {
            io.copy_bytes(bytes as u64)?;
        }
        Ok::<(), Error>(())
    })
}
