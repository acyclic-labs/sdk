//! Controlled completion order over the original child's real stream records.
use super::*;
use futures::{FutureExt as _, channel::oneshot};
use std::sync::Mutex;

struct DelayedInbox {
    inner: acyclic_stream::testing::Counted<MemoryStream>,
    inbox: acyclic_stream::StreamPath,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    refuse_actor: bool,
}

impl DelayedInbox {
    fn new(
        stream: &MemoryStream,
        child: &AgentPath,
        refuse_actor: bool,
    ) -> Result<(oneshot::Sender<()>, Arc<Self>)> {
        let (release, receiver) = oneshot::channel();
        Ok((
            release,
            Arc::new(Self {
                inner: acyclic_stream::testing::Counted::new(stream.clone()),
                inbox: child.inbox()?,
                release: Mutex::new(Some(receiver)),
                refuse_actor,
            }),
        ))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for DelayedInbox {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, StreamError> {
        if request.paths.contains(&self.inbox) {
            let receiver = self.release.lock().unwrap().take().unwrap();
            receiver.await.map_err(|_| StreamError::Unavailable)?;
        } else if self.refuse_actor {
            return Err(StreamError::Unavailable);
        }
        self.inner.heads(request).await
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<acyclic_stream::WriteOutcome, StreamError>> {
        self.inner.write(request)
    }

    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for DelayedInbox {}

pub(super) async fn check(
    stream: &Arc<MemoryStream>,
    child: &AgentPath,
    expected: &Settlement,
    advance: impl std::future::Future<Output = Result>,
) -> Result {
    let within = within();
    for cancel in [false, true] {
        let (release, provider) = DelayedInbox::new(stream, child, false)?;
        let mut observation = Box::pin(settled(&provider, &within, child));
        assert!(observation.as_mut().now_or_never().is_none());
        assert_eq!(
            provider.inner.requests().control(),
            1,
            "actor checkpoint waited for inbox observation"
        );
        if cancel {
            drop(release);
        } else {
            release.send(()).expect("original inbox observer is active");
        }
        let result = observation
            .now_or_never()
            .expect("inbox producer release did not finish settlement");
        if cancel {
            assert!(matches!(
                result,
                Err(Error::Stream(StreamError::Unavailable))
            ));
        } else {
            assert_eq!(result?, Some(expected.clone()));
            let requests = provider.inner.requests();
            assert_eq!(requests.control(), 2, "one inbox and one actor head");
            assert_eq!(requests.data(), 0, "actor head retains its checkpoint");
        }
    }

    // An actor refusal ends the observation while the inbox producer is held.
    let (_release, provider) = DelayedInbox::new(stream, child, true)?;
    assert!(
        matches!(
            settled(&provider, &within, child).now_or_never(),
            Some(Err(Error::Actor(_)))
        ),
        "actor refusal waited for the inbox producer"
    );

    // Capture the real actor checkpoint first, then commit a new real input.
    let (release, provider) = DelayedInbox::new(stream, child, false)?;
    let mut observation = Box::pin(settled(&provider, &within, child));
    assert!(observation.as_mut().now_or_never().is_none());
    advance.await?;
    release.send(()).expect("original inbox observer is active");
    assert_eq!(
        observation
            .now_or_never()
            .expect("inbox producer release did not finish settlement")?,
        None,
        "an older actor checkpoint covered a newer inbox cut"
    );
    Ok(())
}
