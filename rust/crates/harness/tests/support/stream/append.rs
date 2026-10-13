use super::*;
use std::sync::atomic::Ordering;

impl<P: StreamProvider> LostSessionAck<P> {
    pub(super) fn before_append(
        &self,
        request: &acyclic_stream::AppendRequest,
    ) -> Result<u8, StreamError> {
        self.io.writes.fetch_add(1, Ordering::SeqCst);
        self.measure_write(&request.records);
        if self.forbid_writes.load(Ordering::SeqCst) {
            self.observation_writes.fetch_add(1, Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        if request.path.as_str() == COORDINATOR_PATH
            && self
                .location_fault
                .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(StreamError::Unavailable);
        }
        let fault = if request
            .path
            .as_str()
            .starts_with("harness/v3/coordinator/intent-locations/")
        {
            self.location_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(StreamError::Unavailable);
        }
        Ok(fault)
    }

    pub(super) fn after_append(
        &self,
        outcome: &mut AppendOutcome,
        fault: u8,
    ) -> Result<(), StreamError> {
        if fault >= 2 {
            if fault == 3 {
                self.hide_location_read.store(true, Ordering::SeqCst);
            }
            return Err(StreamError::Unavailable);
        }
        if matches!(outcome, AppendOutcome::Committed(_))
            && self.lose_ack.swap(false, Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        if let AppendOutcome::Committed(receipt) = outcome {
            match self.append_receipt_fault.swap(0, Ordering::SeqCst) {
                1 => receipt.start = receipt.end,
                2 => receipt.end = receipt.start,
                3 => receipt.tail = receipt.start,
                _ => {}
            }
        }
        Ok(())
    }
}
