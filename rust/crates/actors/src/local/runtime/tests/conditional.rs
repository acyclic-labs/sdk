//! Interleave real publications at the original provider boundary. No response
//! is manufactured: both owners use the same MemoryStream and conditional cut.
use super::*;
use acyclic_stream::{
    CommittedEnvelope, ExplicitStreamProvider, FollowRequest, FollowStream, Heads, HeadsRequest,
    IdempotencyObservation, ProviderContext, ReadRequest, WriteOutcome, WriteRequest,
};
use std::sync::Mutex;

enum Competing {
    SameProposal,
    Publication(CommitRequest),
}

#[derive(Default)]
struct Interleaved {
    source: MemoryStream,
    before_write: Mutex<Option<Competing>>,
    after_page: Mutex<Option<CommitRequest>>,
}

impl Interleaved {
    async fn publish(&self, request: CommitRequest) -> Result<(), StreamError> {
        match self.source.commit(request).await? {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(()),
            acyclic_stream::CommitOutcome::Conflict(_) => Err(StreamError::InvalidResponse),
        }
    }
}

#[async_trait::async_trait]
impl ExplicitStreamProvider for Interleaved {
    fn context(&self) -> ProviderContext<'_> {
        self.source.context()
    }

    fn credential_provider(&self) -> Option<&acyclic_auth::credential::CredentialProvider> {
        self.source.credential_provider()
    }

    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.source.durability_barrier().await
    }

    async fn heads(&self, request: HeadsRequest) -> Result<Heads, StreamError> {
        let is_page = request.children.is_some();
        let result = self.source.heads(request).await?;
        let competing = is_page
            .then(|| self.after_page.lock().unwrap().take())
            .flatten();
        if let Some(request) = competing {
            self.publish(request).await?;
        }
        Ok(result)
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.source.inspect_idempotency(key).await
    }

    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, Result<WriteOutcome, StreamError>> {
        Box::pin(async move {
            let competing = self.before_write.lock().unwrap().take();
            if let Some(competing) = competing {
                let publication = match competing {
                    Competing::Publication(publication) => publication,
                    Competing::SameProposal => {
                        let WriteRequest::Commit(mut publication) = request.clone() else {
                            return Err(StreamError::InvalidArgument);
                        };
                        publication.idempotency_key = IdempotencyKey::unique();
                        publication
                    }
                };
                self.publish(publication).await?;
            }
            self.source.write(request).await
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.source.read(request).await
    }

    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.source.follow(request).await
    }

    async fn read_commit(
        &self,
        commit: acyclic_stream::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.source.read_commit(commit).await
    }
}

fn runtime() -> Result<Runtime<Counted<Interleaved>>, Box<dyn std::error::Error>> {
    Ok(Runtime::new(
        Store::new(Arc::new(Counted::new(Interleaved::default())), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ))
}

#[tokio::test]
async fn dispatcher_creation_returns_the_competing_owners_actual_result() -> TestResult {
    let runtime = runtime()?;
    let provider = runtime.store().provider();
    let (store, address) = runtime.dispatcher_store()?;
    *provider.inner().before_write.lock().unwrap() = Some(Competing::SameProposal);
    let before = provider.requests();
    assert!(matches!(
        runtime.dispatcher(&store, &address).await,
        Err(Error::Conflict)
    ));
    let original = provider.requests().since(before);
    assert_eq!(original.control(), 1);
    assert_eq!(original.data(), 1);
    // The competitor committed. A second invocation observes that state once,
    // without creating another checkpoint or replaying the losing proposal.
    let before = provider.requests();
    let opened = runtime.dispatcher(&store, &address).await?;
    assert_eq!(opened.tail, 1);
    assert_eq!(
        opened.state().and_then(|state| state.cursor("activation")),
        Some(0)
    );
    let observed = provider.requests().since(before);
    assert_eq!(observed.control(), 1);
    assert_eq!(observed.data(), 0);
    Ok(())
}

#[tokio::test]
async fn pause_returns_a_lost_cut_before_an_explicit_new_request() -> TestResult {
    let runtime = runtime()?;
    let store = runtime.store();
    let provider = store.provider();
    let address = spawn_with(store, "pause-conflict", &[]).await?;
    let cut = store.load(&address).await?;
    let mut claimed = cut.state().ok_or("missing state")?.clone();
    claimed.epoch = 1;
    let claim = store
        .prepare(&cut, &claimed, Vec::new(), key("competing-claim")?)?
        .into_request()?;
    *provider.inner().before_write.lock().unwrap() = Some(Competing::Publication(claim));
    let before = provider.requests();
    assert!(matches!(
        runtime.pause(&address, true).await,
        Err(Error::Conflict)
    ));
    let original = provider.requests().since(before);
    assert_eq!(original.control(), 1);
    assert_eq!(original.data(), 1);
    let observed = store.load(&address).await?;
    assert_eq!(observed.state().map(State::epoch), Some(1));
    assert_eq!(observed.state().map(State::paused), Some(false));
    let before = provider.requests();
    runtime.pause(&address, true).await?;
    let explicit = provider.requests().since(before);
    assert_eq!(explicit.control(), 1);
    assert_eq!(explicit.data(), 1);
    let paused = store.load(&address).await?;
    assert_eq!(paused.state().map(State::epoch), Some(1));
    assert_eq!(paused.state().map(State::paused), Some(true));
    assert_eq!(paused.tail, observed.tail + 1);
    Ok(())
}

#[tokio::test]
async fn restoration_returns_a_changed_page_cut_without_restarting() -> TestResult {
    let runtime = runtime()?;
    let store = runtime.store();
    let provider = store.provider();
    let mut initial = PreparedPublish::foreign(key("restore/initial")?);
    // The first page has 256 children. A real new child between the captured
    // first page and the second must invalidate the original traversal.
    for index in 0..257 {
        for effect in store.create(&actor(&format!("restore-{index}"))?, BTreeMap::new(), None)? {
            initial.join(effect)?;
        }
    }
    store.submit(initial).await?;
    let mut competing = PreparedPublish::foreign(key("restore/competing")?);
    for effect in store.create(&actor("restore-new")?, BTreeMap::new(), None)? {
        competing.join(effect)?;
    }
    *provider.inner().after_page.lock().unwrap() = Some(competing.into_request()?);
    let before = provider.requests();
    assert!(matches!(
        runtime.restore().await,
        Err(Error::Stream(StreamError::HierarchyChanged))
    ));
    let original = provider.requests().since(before);
    assert_eq!(original.control(), 2);
    assert_eq!(original.data(), 0);
    assert_eq!(runtime.counts().0, 256);
    // Explicit restart restores the entire current set. Names from the first
    // partial traversal retain one registration apiece.
    let before = provider.requests();
    runtime.restore().await?;
    assert_eq!(provider.requests().since(before).control(), 2);
    assert_eq!(runtime.counts().0, 258);
    Ok(())
}
