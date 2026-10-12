use super::*;

pub(super) fn observe_commit(
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
    let Some(observer) = quota.work().as_deref() else {
        return Ok(());
    };
    let native = operations::original_native_decode_mode(observer);
    let CommitOutcome::Committed(envelope) = outcome else {
        return Ok(());
    };
    let mut failure = None;
    for mutation in &envelope.mutations {
        let records = match mutation {
            CommittedMutation::Append(append) => &append.records,
            CommittedMutation::Fork(fork) => &fork.records,
        };
        for record in records {
            let bytes = record.value.len() as u64;
            let observer = quota.work().as_deref_mut().expect("observed receipt");
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let received = if native {
                observer.receive_native_record(bytes)
            } else {
                observer.receive_record(bytes)
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let received = observer.receive_record(bytes);
            match received {
                Ok(()) if !native => quota.adopt(bytes)?,
                Ok(()) => {}
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
            if direct_acknowledgment
                && let Err(error) = quota
                    .work()
                    .as_deref_mut()
                    .expect("observed receipt")
                    .acknowledged(bytes, 1)
            {
                failure.get_or_insert(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}
