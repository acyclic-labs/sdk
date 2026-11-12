//! Native adapter over the original allocation-ledger handoff.
use super::*;

/// The accepted native producer keeps the original ledger until settlement.
/// Cancellation abandons reply delivery; it cannot reactivate the caller's
/// standalone branch while native work can still charge it.
pub struct NativeAccounting {
    /// The caller's original work owner, including its cancellation and budget.
    pub work: crate::NativeMaterializationWork,
    baseline: Option<WorkCounters>,
}

impl NativeAccounting {
    /// Returns a completed output to the same caller without another admission.
    /// Ordinary callers resume ownership of the already charged scratch; native
    /// callers keep the same original allocation guard.
    pub fn return_allocation(
        &self,
        mut allocation: crate::NativeAllocation,
        bytes: u64,
    ) -> Result<KernelOwnedAllocation, crate::FsError> {
        if self.baseline.is_some() {
            allocation.transfer_original_bytes(&self.work.state, bytes)?;
            return Ok(KernelOwnedAllocation::Standalone);
        }
        if allocation.bytes() != bytes || !allocation.work().shares_allocation_context(&self.work) {
            return Err(crate::FsError::AccountingHandoffActive);
        }
        Ok(KernelOwnedAllocation::NativeOriginal(allocation))
    }

    /// Transfers admission to a native producer without granting new credit.
    /// The caller must not use `accounting` until `restore` succeeds. Dropping
    /// this owner abandons delivery and leaves that branch fenced while the
    /// physical producer or its output retains the original ledger.
    pub fn take(accounting: &mut KernelAccounting<'_>) -> Result<Self, crate::FsError> {
        accounting.check()?;
        if let Some(work) = accounting.native_work() {
            return Ok(Self {
                work: work.clone(),
                baseline: None,
            });
        }
        let backing = RetainedAllocation::claim(
            crate::NativeMaterializationWork::accounting_state_bytes(),
            1,
            accounting,
        )?;
        let RetainedAllocationInner::Standalone { _lease: backing } = backing.inner else {
            return Err(crate::FsError::AccountingHandoffActive);
        };
        let (allocations, counters, budget, stop) = accounting
            .standalone_parts()
            .ok_or(crate::FsError::AccountingHandoffActive)?;
        let mut state = crate::demand::DemandWork::new(budget, stop);
        let (owned, owned_counters, _, _) = state.allocation_parts();
        *owned = allocations.handoff(counters, budget)?;
        *owned_counters = *counters;
        let baseline = *counters;
        let meter = crate::demand::NativeDemandReadMeter::with_backing(state, backing);
        let meter = Arc::new(meter);
        let super::super::LedgerState::Suspended(_, observer) = &mut allocations.state else {
            unreachable!("the original branch was suspended by handoff")
        };
        *observer = Some(Arc::clone(&meter));
        Ok(Self {
            work: crate::NativeMaterializationWork::from_meter(meter),
            baseline: Some(baseline),
        })
    }

    /// Only the producer's terminal result permits restoring admission.
    /// Restore only after the accepted producer has physically completed.
    pub fn restore(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        let Some(baseline) = self.baseline else {
            if !accounting
                .native_work()
                .is_some_and(|work| work.shares_ledger(&self.work))
            {
                return Err(crate::FsError::AccountingHandoffActive);
            }
            return Ok(());
        };
        self.work.state(|state| {
            let (allocations, counters, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::FsError::AccountingHandoffActive)?;
            if budget != state.budget || *counters != baseline {
                return Err(crate::FsError::AccountingHandoffActive);
            }
            state.receipt();
            let (owned, owned_counters, _, _) = state.allocation_parts();
            let original = owned.handoff(owned_counters, budget)?;
            allocations.resume(original)?;
            *counters = state.work;
            counters.verify(budget)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{
        AppendRequest, HeadProjection, HeadsRequest, IdempotencyKey, MemoryStream,
        NativeStreamProvider, RevisionCut, StreamPath, StreamProviderExt as _,
    };

    #[tokio::test]
    async fn stream_projection_keeps_original_credit_through_native_handoff_and_decode()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(serde::Deserialize)]
        struct Text {
            text: String,
        }
        let stream = MemoryStream::default();
        let path = StreamPath::new("computed/input")?;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "text": "selected\ntext é",
            "transcript": [{"parts": ["unselected".repeat(1024)]}]
        }))?;
        stream
            .append(AppendRequest {
                path: path.clone(),
                records: vec![bytes.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let cancellation = crate::CancellationToken::new();
        let budget = WorkBudget {
            items_examined: 1_000_000,
            ..WorkBudget::UNBOUNDED
        };
        let mut caller = KernelAccounting::standalone(budget, &cancellation);
        // Independent leases promote the ordinary ledger once. That control
        // allocation lives as long as the caller, rather than as read payload.
        drop(RetainedAllocation::claim(0, 0, &mut caller)?);
        let baseline = caller
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes();
        caller.charge_items(7)?;
        let owner = NativeAccounting::take(&mut caller)?;
        assert!(matches!(
            caller.check(),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        let result = stream
            .native_heads(
                HeadsRequest {
                    at: None,
                    paths: [path].into(),
                    retained: Default::default(),
                    projection: HeadProjection::History,
                    durable_through: None,
                    children: None,
                    limit: 1,
                    at_least: RevisionCut::default(),
                },
                owner.work.runtime_meter(),
            )
            .await??;
        let record = result.value().heads[0]
            .as_ref()
            .ok_or("missing head")?
            .last
            .as_ref()
            .ok_or("missing record")?;
        let mut producer = KernelAccounting::native(&owner.work, &cancellation);
        let decoded = producer.decode_json::<Text>(record.value.as_ref())?;
        assert_eq!(decoded.text, "selected\ntext é");
        drop(producer);
        let completed = owner.work.receipt();
        owner.restore(&mut caller)?;
        assert_eq!(caller.spent().items_examined, completed.items_examined);
        assert_eq!(
            caller.remaining()?.items_examined,
            budget.items_examined - completed.items_examined
        );
        drop(result);
        // The decoded value remains funded after both the producer and the
        // stream result are gone, and returns its credit when it is dropped.
        let held = caller
            .standalone_parts()
            .ok_or("missing original ledger")?
            .0
            .live_bytes();
        assert!(held >= baseline + decoded.text.len() as u64);
        drop(decoded);
        assert_eq!(
            caller
                .standalone_parts()
                .ok_or("missing original ledger")?
                .0
                .live_bytes(),
            baseline
        );
        let remaining = caller.remaining()?.items_examined;
        caller.charge_items(remaining)?;
        assert!(matches!(
            caller.charge_items(1),
            Err(crate::FsError::Work(WorkError::BudgetExceeded {
                counter: "items_examined",
                ..
            }))
        ));
        Ok(())
    }
}
