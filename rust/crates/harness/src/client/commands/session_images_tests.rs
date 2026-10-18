use super::*;
use acyclic_stream::*;
use futures::FutureExt as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Pause at both sides of the actual provider commit. Dropping submit at the
// latter cut loses its acknowledgement and all client-side follow/cache work.
#[derive(Default)]
struct CrashStream {
    inner: MemoryStream,
    armed: AtomicBool,
    step: AtomicUsize,
    cut: AtomicUsize,
    omit_consumption: AtomicBool,
}
impl CrashStream {
    async fn crash_cut(&self) {
        if self.armed.load(Ordering::SeqCst)
            && self.step.fetch_add(1, Ordering::SeqCst) == self.cut.load(Ordering::SeqCst)
        {
            std::future::pending::<()>().await;
        }
    }
}
#[async_trait::async_trait]
impl StreamProvider for CrashStream {
    async fn inspect_idempotency(
        &self,
        k: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(k).await
    }
    async fn tail(&self, p: StreamPath) -> std::result::Result<u64, StreamError> {
        self.inner.tail(p).await
    }
    async fn bounds(&self, p: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        self.inner.bounds(p).await
    }
    async fn watermarks(
        &self,
        p: StreamPath,
    ) -> std::result::Result<StreamWatermarks, StreamError> {
        self.inner.watermarks(p).await
    }
    async fn append(&self, r: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        self.inner.append(r).await
    }
    async fn fork(&self, r: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        self.inner.fork(r).await
    }
    async fn read(&self, r: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.inner.read(r).await
    }
    async fn follow(
        &self,
        p: StreamPath,
        n: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.inner.follow(p, n).await
    }
    async fn follow_checked(
        &self,
        p: StreamPath,
        n: u64,
        c: CommitId,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.inner.follow_checked(p, n, c).await
    }
    async fn children(&self, r: ChildrenRequest) -> std::result::Result<ChildStream, StreamError> {
        self.inner.children(r).await
    }
    async fn commit(
        &self,
        mut r: CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        self.crash_cut().await;
        // The removed two-publication path exposes precisely this prefix.
        if self.armed.load(Ordering::SeqCst) && self.omit_consumption.load(Ordering::SeqCst) {
            for mutation in &mut r.mutations {
                if let CommitMutation::Append { records, .. } = mutation {
                    records.truncate(1);
                }
            }
        }
        let result = self.inner.commit(r).await;
        self.crash_cut().await;
        result
    }
    async fn read_commit(
        &self,
        c: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(c).await
    }
}

use super::image_test_support::{Reader, image};

async fn scenario(cut: usize, peer: u8, broken: bool) {
    let provider = Arc::new(CrashStream::default());
    let volume = Arc::new(
        files::TaskVolume::open(
            provider.clone(),
            StreamPath::new("image-crash-cuts").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([1; 16]);
    let mut commands = CommandSession::open(volume.clone(), session, 65_536, 64)
        .await
        .unwrap();
    commands
        .write_state(
            CommandFileRoot::Config,
            "image-input.json",
            &json!({
        "default":"test/model", "models":{"test/model":{
            "maximum_bytes":4096,"maximum_total_bytes":8192,"maximum_images":4,
            "maximum_dimension":4096,"maximum_tokens":1024}}}),
        )
        .await
        .unwrap();
    commands
        .write_state(
            CommandFileRoot::Session,
            "attachments.json",
            &json!([image(1)]),
        )
        .await
        .unwrap();
    let draft = commands
        .state_key(CommandFileRoot::Session, "attachments.json")
        .unwrap();
    if peer == 1 {
        volume
            .append(
                volume.head().await.unwrap(),
                &files::FileValue::new(draft.clone(), serde_json::to_vec(&vec![image(2)]).unwrap()),
            )
            .await
            .unwrap();
    }
    provider.cut.store(cut, Ordering::SeqCst);
    provider.omit_consumption.store(broken, Ordering::SeqCst);
    provider.armed.store(true, Ordering::SeqCst);
    let result = commands.submit("describe", &Reader).now_or_never();
    if peer != 1 && cut < 2 {
        assert!(result.is_none(), "crash cut was not reached");
    }
    drop(commands);
    provider.armed.store(false, Ordering::SeqCst);
    if peer == 2 {
        volume
            .append(
                volume.head().await.unwrap(),
                &files::FileValue::new(draft.clone(), serde_json::to_vec(&vec![image(2)]).unwrap()),
            )
            .await
            .unwrap();
    }
    let fresh = Arc::new(volume.reopen().await.unwrap());
    let mut reopened = CommandSession::open(fresh, session, 65_536, 64)
        .await
        .unwrap();
    let images = crate::client::host_images::draft_images(&reopened).unwrap();
    let request = reopened.take_request();
    if peer != 0 {
        assert_eq!(images, vec![image(2)], "peer draft was lost");
    } else if let Some(request) = &request {
        let resolution: CommandResolution = serde_json::from_slice(&request.bytes).unwrap();
        assert!(
            matches!(resolution, CommandResolution::Prompt { mentions, .. } if mentions.len()==1 && mentions[0].file==image(1))
        );
        assert!(
            images.is_empty(),
            "accepted request left a consumed image in the draft"
        );
    } else {
        assert_eq!(images, vec![image(1)]);
    }
    // The next prompt after reopening must never resend an already accepted image.
    reopened.submit("next", &Reader).await.unwrap();
    if request.is_some() && peer == 0 {
        let next = reopened.pending.back().unwrap();
        let resolution: CommandResolution = serde_json::from_slice(&next.bytes).unwrap();
        assert!(
            matches!(resolution, CommandResolution::Prompt { mentions, .. } if mentions.is_empty())
        );
    }
}

#[tokio::test]
async fn image_consumption_survives_commit_crash_cuts_and_peer_writes() {
    for cut in 0..3 {
        for peer in 0..3 {
            scenario(cut, peer, false).await;
        }
    }
}
#[tokio::test]
#[should_panic(expected = "accepted request left a consumed image in the draft")]
async fn image_nonatomic_consumption_negative_control() {
    scenario(1, 0, true).await;
}
