use super::*;
use crate::materializer::NativeResourceAllowance;

/// One original read cleanup reservation, held by its actual consumer.
/// Completion publishes into the existing original accounting aggregate.
pub(crate) struct ViewReadCompletion {
    shared: Arc<NativeAllocationAccounting>,
    resource: Option<Arc<NativeResourceAllowance>>,
    completed: bool,
}
const CLEANUP: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};
impl DemandWork {
    pub(crate) fn reserve_view_read(&mut self) -> Result<ViewReadCompletion, DemandError> {
        let shared = self.allocation_accounting()?;
        self.reserve(CLEANUP)?;
        Ok(ViewReadCompletion {
            shared,
            resource: self.resource(),
            completed: false,
        })
    }
}
impl NativeAllocationAccounting {
    pub(super) fn take_view_reads(&self) -> (u64, u64, bool) {
        (
            self.view_completed.swap(0, Ordering::AcqRel),
            self.view_unused.swap(0, Ordering::AcqRel),
            self.view_failed.swap(false, Ordering::AcqRel),
        )
    }
}
impl ViewReadCompletion {
    pub(crate) fn completed(&mut self) {
        self.completed = true;
    }
}
impl Drop for ViewReadCompletion {
    fn drop(&mut self) {
        if let Some(resource) = &self.resource {
            let refunded = resource.refund(CLEANUP, None);
            let recorded = if self.completed {
                resource.completed(CLEANUP, 0)
            } else {
                Ok(())
            };
            if refunded.and(recorded).is_err() {
                self.shared.view_failed.store(true, Ordering::Release);
            }
        }
        let count = if self.completed {
            &self.shared.view_completed
        } else {
            &self.shared.view_unused
        };
        // Every live reservation withheld one item from this same finite ledger.
        count.fetch_add(1, Ordering::Release);
    }
}
