use super::*;
use std::sync::atomic::Ordering;

impl<P: StreamProvider> LostSessionAck<P> {
    pub(super) async fn before_commit(
        &self,
        request: &acyclic_stream::CommitRequest,
    ) -> Result<(u8, u8), StreamError> {
        self.io.writes.fetch_add(1, Ordering::SeqCst);
        for mutation in &request.mutations {
            if let acyclic_stream::CommitMutation::Append { records, .. } = mutation {
                self.measure_write(records);
            }
        }
        if self.forbid_writes.load(Ordering::SeqCst) {
            self.observation_writes.fetch_add(1, Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        if request.mutations.iter().any(|mutation| matches!(mutation,
            acyclic_stream::CommitMutation::Append { path, .. } if path.as_str() == COORDINATOR_PATH))
            && self.location_fault.compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        {
            return Err(StreamError::Unavailable);
        }
        if self.execution_race.swap(false, Ordering::SeqCst) {
            let (path, tail) = request
                .conditions
                .iter()
                .find_map(|condition| match condition {
                    acyclic_stream::CommitCondition::Absent { path }
                        if path.as_str().starts_with("harness/v2/execution/") =>
                    {
                        Some((path.clone(), 0))
                    }
                    acyclic_stream::CommitCondition::Tail { path, expected }
                        if path.as_str().starts_with("harness/v2/execution/") =>
                    {
                        Some((path.clone(), *expected))
                    }
                    _ => None,
                })
                .ok_or(StreamError::Unavailable)?;
            self.inner
                .append(acyclic_stream::AppendRequest {
                    path,
                    records: vec![OwnedBytes::from_static(b"dispatch won").into()],
                    if_tail: Some(tail),
                    idempotency_key: None,
                })
                .await?;
        }
        let selected = request.conditions.iter().any(|condition| matches!(condition,
            acyclic_stream::CommitCondition::Tail { path, expected } if path.as_str().starts_with("harness/v2/execution/") && *expected == self.execution_tail.load(Ordering::SeqCst)));
        let fault = if selected {
            self.execution_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault != 0 {
            self.execution_faults.fetch_add(1, Ordering::SeqCst);
        }
        if fault == ExecutionFaultMode::Before as u8 {
            return Err(StreamError::Unavailable);
        }
        let aggregate = request.mutations.iter().any(|mutation| matches!(mutation,
            acyclic_stream::CommitMutation::Append { path, .. } if path.as_str().starts_with("harness/v2/aggregate-operations/")));
        let aggregate_fault = if aggregate {
            self.aggregate_commits.fetch_add(1, Ordering::SeqCst);
            self.aggregate_commit_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if aggregate_fault == 1 {
            return Err(StreamError::Unavailable);
        }
        Ok((fault, aggregate_fault))
    }

    pub(super) fn after_commit(
        &self,
        outcome: &mut acyclic_stream::CommitOutcome,
        fault: u8,
        aggregate_fault: u8,
    ) -> Result<(), StreamError> {
        if let acyclic_stream::CommitOutcome::Committed(envelope) = outcome {
            self.measure_received(envelope);
        }
        if aggregate_fault == 3 {
            self.hide_receipt.store(true, Ordering::SeqCst);
        }
        if matches!(aggregate_fault, 2 | 3) {
            return Err(StreamError::Unavailable);
        }
        corrupt_aggregate_receipt(outcome, aggregate_fault);
        if fault == ExecutionFaultMode::AfterHidden as u8 {
            self.hide_receipt.store(true, Ordering::SeqCst);
        }
        if fault != 0
            || self.commit_lose_ack.swap(false, Ordering::SeqCst)
            || self.lose_ack.swap(false, Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        Ok(())
    }
}
