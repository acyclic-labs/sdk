use super::swarm_wire::{Counts, Fault};
use acyclic_stream::*;
use futures::StreamExt as _;
use std::sync::{Arc, Mutex};

pub(super) struct Observed {
    pub memory: Arc<MemoryStream>,
    pub counts: Arc<Mutex<Counts>>,
    pub fault: Fault,
    pub agents: usize,
}

struct Lease(Arc<Mutex<Counts>>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).live_full -= 1;
    }
}

impl Observed {
    fn observed(&self, path: &StreamPath, records: RecordStream) -> RecordStream {
        if path.as_str().ends_with("/status") {
            // Fault the actual provider open invoked by session::serve, before
            // any SwarmClient delivery or fixture callback can run.
            let copies = match self.fault {
                Fault::LogarithmicProviderWork => {
                    64 * self.agents.next_power_of_two().ilog2() as usize
                }
                Fault::QuadraticProviderWork => self.agents,
                _ => 0,
            };
            for _ in 0..copies {
                std::hint::black_box(vec![0u8; 64]);
            }
        }
        // Instrumentation has one lock; it is never nested or held over a poll.
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        counts.server_follows += 1;
        let lease =
            (path.as_str().ends_with("/files") && path.as_str() != "swarm/files").then(|| {
                counts.live_full += 1;
                counts.peak_full = counts.peak_full.max(counts.live_full);
                Lease(self.counts.clone())
            });
        drop(counts);
        let counts = self.counts.clone();
        futures::stream::unfold(
            (records, lease, counts),
            |(mut records, lease, counts)| async move {
                let record = records.next().await?;
                if record.is_ok() {
                    counts
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .server_records += 1;
                }
                Some((record, (records, lease, counts)))
            },
        )
        .boxed()
    }
}

#[async_trait::async_trait]
impl StreamProvider for Observed {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.memory.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.memory.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.memory.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.memory.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.memory.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.memory.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.memory.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let records = self.memory.follow(path.clone(), from).await?;
        Ok(self.observed(&path, records))
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        let records = self
            .memory
            .follow_checked(path.clone(), from, preceding)
            .await?;
        Ok(self.observed(&path, records))
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.memory.children(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.memory.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.memory.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.memory.read_commit(id).await
    }
}
