//! Receipt fault injection around the ordinary stream backend, not a process owner.
use super::*;
use tokio::sync::Mutex;
#[derive(Default)]
struct ReceiptFaultStream {
    inner: MemoryStream,
    fault: Mutex<Option<(&'static str, bool)>>,
    hide_next_inspection: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl StreamProvider for ReceiptFaultStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        if self
            .hide_next_inspection
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, acyclic_stream::StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, acyclic_stream::StreamError> {
        self.inner.children_page(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let mut armed = self.fault.lock().await;
        let selected = armed.as_ref().is_some_and(|(kind, _)| request.mutations.iter().any(|mutation| {
            matches!(mutation, acyclic_stream::CommitMutation::Append { path, records }
                if path.as_str().starts_with("harness/v2/native-process/") && records.iter().any(|record| serde_json::from_slice::<Value>(record).is_ok_and(|value| value.get("kind").and_then(Value::as_str) == Some(*kind))))
        }));
        let fault = if selected { armed.take() } else { None };
        drop(armed);
        if fault.is_some_and(|(_, lost_reply)| !lost_reply) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let result = self.inner.commit(request).await?;
        if fault.is_some() {
            // Hide the immediate keyed observation as well, so this is
            // an unknown commit rather than a recovered lost reply.
            self.hide_next_inspection
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(result)
    }
}

pub(super) async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    for (kind, lost_reply) in [
        ("launch", false),
        ("launch", true),
        ("observed", false),
        ("observed", true),
    ] {
        let stream = Arc::new(ReceiptFaultStream::default());
        *stream.fault.lock().await = Some((kind, lost_reply));
        super::run_on(
            true,
            StreamClient::new(stream.clone()),
            Some((kind, lost_reply)),
            false,
        )
        .await?;
        if stream.fault.lock().await.is_some() {
            return Err("native receipt fault was not exercised".into());
        }
        println!("MCP {kind} receipt fault (lost acknowledgement: {lost_reply}) passed");
    }
    super::run_on(
        true,
        StreamClient::new(Arc::new(MemoryStream::default())),
        None,
        true,
    )
    .await?;
    println!(
        "MCP applied call with lost response, bounded cleanup and receipt-only recovery passed"
    );
    Ok(())
}
