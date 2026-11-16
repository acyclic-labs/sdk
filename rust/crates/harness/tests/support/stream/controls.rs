use super::*;

pub(super) const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ExecutionFaultMode {
    Before = 1,
    AfterHidden = 3,
}
#[derive(Default)]
pub struct StreamIo {
    pub append_paths: std::sync::Mutex<Vec<acyclic_stream::StreamPath>>,
    pub reads: std::sync::atomic::AtomicU64,
    pub writes: std::sync::atomic::AtomicU64,
    pub records: std::sync::atomic::AtomicU64,
    pub write_bytes: std::sync::atomic::AtomicU64,
    pub inspections: std::sync::atomic::AtomicU64,
    pub commit_reads: std::sync::atomic::AtomicU64,
    pub receipt_bytes: std::sync::atomic::AtomicU64,
    pub other_calls: std::sync::atomic::AtomicU64,
}
#[derive(Default)]
pub struct LostSessionAck<P = MemoryStream> {
    pub inner: P,
    pub io: StreamIo,
    pub native_only: std::sync::atomic::AtomicBool,
    pub lose_ack: std::sync::atomic::AtomicBool,
    pub append_receipt_fault: std::sync::atomic::AtomicU8,
    pub hide_receipt: std::sync::atomic::AtomicBool,
    pub location_fault: std::sync::atomic::AtomicU8,
    pub hide_location_read: std::sync::atomic::AtomicBool,
    pub forbid_writes: std::sync::atomic::AtomicBool,
    pub observation_reads: std::sync::atomic::AtomicUsize,
    pub observation_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub observation_tails: std::sync::atomic::AtomicUsize,
    pub observation_maximum: std::sync::atomic::AtomicU32,
    pub observation_writes: std::sync::atomic::AtomicUsize,
    pub execution_race: std::sync::atomic::AtomicBool,
    pub commit_lose_ack: std::sync::atomic::AtomicBool,
    pub execution_fault: std::sync::atomic::AtomicU8,
    pub execution_tail: std::sync::atomic::AtomicU64,
    pub execution_faults: std::sync::atomic::AtomicUsize,
    pub hide_effect_node_read: std::sync::atomic::AtomicBool,
    pub hide_effect_transition_position: std::sync::atomic::AtomicU64,
    pub history_read_fault: std::sync::atomic::AtomicU8,
    pub history_cancel_after_tail: std::sync::Mutex<Option<acyclic_fs::CancellationToken>>,
    pub message_head_race: std::sync::Mutex<Option<acyclic_stream::CommitRequest>>,
    pub aggregate_commit_fault: std::sync::atomic::AtomicU8,
    pub aggregate_commits: std::sync::atomic::AtomicUsize,
    pub stale_projection_head_tail: std::sync::atomic::AtomicU64,
    pub hide_aggregate_read: std::sync::atomic::AtomicBool,
    pub owned_commit_pause: std::sync::Mutex<Option<CommitPause>>,
}

impl<P> LostSessionAck<P> {
    pub fn reset_io(&self) {
        self.io.append_paths.lock().unwrap().clear();
        use std::sync::atomic::Ordering;
        for counter in [
            &self.io.reads,
            &self.io.writes,
            &self.io.records,
            &self.io.write_bytes,
            &self.io.inspections,
            &self.io.commit_reads,
            &self.io.receipt_bytes,
            &self.io.other_calls,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
        self.observation_bytes.store(0, Ordering::SeqCst);
        self.observation_tails.store(0, Ordering::SeqCst);
    }
    pub(super) fn measure_received(&self, envelope: &acyclic_stream::CommittedEnvelope) {
        let bytes = envelope
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                acyclic_stream::CommittedMutation::Append(append) => Some(
                    append
                        .records
                        .iter()
                        .map(|record| record.payload_bytes() as u64)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum();
        self.io
            .receipt_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }
    pub(super) fn measure_write(&self, records: &[acyclic_stream::AppendRecord]) {
        use std::sync::atomic::Ordering;
        self.io
            .records
            .fetch_add(records.len() as u64, Ordering::SeqCst);
        self.io.write_bytes.fetch_add(
            records
                .iter()
                .map(|record| record.payload_bytes() as u64)
                .sum(),
            Ordering::SeqCst,
        );
    }
}
