use super::*;

/// Disposable, authenticated prefix state. Field order drops maps before their
/// original allocation custody. The journal lock is acquired before provider,
/// task-file index or preparation-ledger locks; none may reenter this journal.
#[derive(Default)]
pub(super) struct VerifiedSummary {
    pub summary: ExecutionSummary,
    pub funding: Option<PreparationIo>,
}
impl VerifiedSummary {
    pub fn bind(&mut self, operation: OperationId, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let original = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        let same = match (
            original,
            self.funding
                .as_ref()
                .and_then(PreparationIo::original_native_work),
        ) {
            (None, None) => true,
            (Some(a), Some(b)) => a.shares_invocation_context(b),
            _ => false,
        };
        #[cfg(target_arch = "wasm32")]
        let same = self.funding.is_none();
        if !same
            || self
                .summary
                .operation
                .is_some_and(|found| found != operation)
        {
            self.clear();
        }
        if self.funding.is_none() {
            self.funding = PreparationIo::temporary(io)?;
        }
        Ok(())
    }
    pub fn clear(&mut self) {
        self.summary = ExecutionSummary::default();
        self.funding = None;
    }
}

pub(super) fn matches_anchor(
    after: u64,
    expected: acyclic_stream::CommitId,
    sequence: u64,
    actual: acyclic_stream::CommitId,
) -> bool {
    sequence.checked_add(1) == Some(after) && actual == expected
}

#[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
mod model;
#[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
pub use model::verify_cached_summary_publication;
#[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
pub(super) use model::{AppendProbe, model_future};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn cached_cursor_rejects_loss_and_replacement(sequence in 0u64..u64::MAX, bytes in any::<[u8;32]>()) {
            let commit=acyclic_stream::CommitId::from_bytes(bytes);
            prop_assert!(matches_anchor(sequence+1,commit,sequence,commit));
            let mut changed=bytes; changed[0]^=1;
            let changed=acyclic_stream::CommitId::from_bytes(changed);
            prop_assert!(!matches_anchor(sequence+1,commit,sequence,changed));
            prop_assert!(!matches_anchor(sequence+1,commit,sequence+1,commit));
            let missing_fence_check=sequence.checked_add(1)==Some(sequence+1);
            prop_assert!(missing_fence_check,"negative control must accept replaced history");
        }
    }
    #[test]
    fn changing_original_ledger_drops_cached_fields_before_their_funding() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1 << 24);
        let first = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let second = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let io = PreparationIo::from_native_work(&first).unwrap();
        let next = PreparationIo::from_native_work(&second).unwrap();
        let operation = OperationId::from_bytes([1; 16]);
        let mut cache = VerifiedSummary::default();
        cache.bind(operation, Some(&io)).unwrap();
        cache.summary.operation = Some(operation);
        cache
            .summary
            .accept_page(
                vec![(
                    ExecutionRecord {
                        operation_id: operation,
                        sequence: 1,
                        idempotency_key: "retry".into(),
                        event: ExecutionEvent::Started {
                            request_digest: [1; 32],
                        },
                    },
                    None,
                )],
                Some(acyclic_stream::CommitId::from_bytes([9; 32])),
                Limits::default(),
                cache.funding.as_ref(),
            )
            .unwrap();
        assert!(
            first.live_allocation_bytes()
                > acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        cache.bind(operation, Some(&io)).unwrap();
        assert_eq!(cache.summary.tail, 1);
        cache.bind(operation, Some(&next)).unwrap();
        assert_eq!(cache.summary.tail, 0);
        assert_eq!(
            first.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        drop(cache);
        assert_eq!(
            second.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
    }
}
