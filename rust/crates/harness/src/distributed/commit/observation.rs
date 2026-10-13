use super::*;

pub(in crate::distributed) fn observe_commit(
    outcome: &acyclic_stream::CommitOutcome,
    direct_acknowledgment: bool,
    phase: Option<&mut CommitPhase>,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    use acyclic_stream::{CommitOutcome, CommittedMutation};
    if let Some(phase) = phase {
        *phase = match outcome {
            acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
            acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Uncommitted,
        };
    }
    let Some(observer) = quota.work().as_deref_mut() else {
        return Ok(());
    };
    let native = operations::original_native_decode_mode(observer);
    let CommitOutcome::Committed(envelope) = outcome else {
        return Ok(());
    };
    let mut result = Ok(());
    let mut retained = 0;
    for mutation in &envelope.mutations {
        let records = match mutation {
            CommittedMutation::Append(append) => &append.records,
            CommittedMutation::Fork(fork) => &fork.records,
        };
        for record in records {
            let bytes = record.value.len() as u64;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let received = observer.receive_native_record(bytes);
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let received = observer.receive_record(bytes);
            if received.is_ok() && !native {
                retained += bytes;
            }
            result = result.and(received);
            if direct_acknowledgment {
                result = result.and(observer.acknowledged(bytes, 1));
            }
        }
    }
    quota.adopt(retained)?;
    result
}
