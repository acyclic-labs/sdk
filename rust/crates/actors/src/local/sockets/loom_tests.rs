//! Exactly one Close per socket, model-checked over the production close and
//! message commits on Stream's loom-instrumented commit locks. Run with
//! `RUSTFLAGS="--cfg loom"` and the `model-checking` feature.
use super::*;
use acyclic_stream::{MemoryStream, StreamProviderExt as _};
use futures::TryStreamExt as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn values(provider: &MemoryStream, path: &StreamPath) -> Result<Vec<Bytes>> {
    let records = block_on(async {
        provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 0,
                limit: PAGE,
                at_least: RevisionCut::default(),
            })
            .await?
            .try_collect::<Vec<_>>()
            .await
    })?;
    Ok(records
        .into_iter()
        .map(|record| Bytes::copy_from_slice(record.value.as_ref()))
        .collect())
}

/// Two closers race one peer message on an open socket. `broken` strips the
/// socket's Tail condition, the negative control.
fn race(broken: bool) -> Result {
    let provider = MemoryStream::default();
    let shared = Arc::new(StripsTail {
        inner: provider.clone(),
        strip: broken,
    });
    let store = Store::new(Arc::clone(&shared), StreamPath::new("loom/actors")?);
    let layout = Layout::new(&store)?;
    let target = Target {
        actor: ActorReference::new(b"account", b"sockets", b"loom")?,
        inbox: StreamPath::new("loom/inbox")?,
    };
    let socket = layout.socket("1-0")?;
    // The committed Open: socket record 0 and its inbox invocation.
    assert!(block_on(commit(
        &store,
        layout.key(&[b"open"])?,
        vec![
            append(
                socket.clone(),
                vec![Bytes::from_static(b"opened").into()],
                vec![CommitCondition::Absent {
                    path: socket.clone()
                }]
            ),
            append(
                target.inbox.clone(),
                vec![Bytes::from_static(b"open").into()],
                Vec::new()
            ),
        ],
    ))?);
    let peer = loom::thread::spawn({
        let store = Store::new(shared, store.root().clone());
        let layout = layout.clone();
        let target = target.clone();
        move || {
            block_on(commit_close(
                &store,
                &layout,
                "1-0",
                &target,
                Some(1),
                &Close::new(1006, "peer went away", SocketCloseInitiator::Peer),
            ))
            .map_err(|error| error.to_string())
        }
    });
    block_on(commit_message(
        &store,
        &layout,
        "1-0",
        &target,
        1,
        SocketPayload::Text {
            text: "frame".into(),
        },
    ))?;
    block_on(commit_close(
        &store,
        &layout,
        "1-0",
        &target,
        Some(1),
        &Close::new(1000, "actor closed", SocketCloseInitiator::Actor),
    ))?;
    peer.join().map_err(|_| "peer closer panicked")??;
    let inbox = values(&provider, &target.inbox)?;
    let kinds: Vec<&str> = inbox
        .iter()
        .skip(1)
        .map(|value| crate::sockets::input_kind(value).unwrap_or("other"))
        .collect();
    assert_eq!(
        kinds.iter().filter(|kind| **kind == "close").count(),
        1,
        "socket delivered other than exactly one Close: {kinds:?}"
    );
    assert_eq!(
        kinds.last(),
        Some(&"close"),
        "an event followed the Close: {kinds:?}"
    );
    assert_eq!(values(&provider, &socket)?.len(), 2);
    Ok(())
}

#[test]
fn concurrent_closes_and_messages_deliver_exactly_one_final_close() {
    loom::model(|| {
        let result = race(false);
        assert!(result.is_ok(), "{result:?}");
    });
}

#[test]
#[should_panic(expected = "socket delivered other than exactly one Close")]
fn without_the_socket_tail_condition_both_closes_commit() {
    loom::model(|| {
        let result = race(true);
        assert!(result.is_ok(), "{result:?}");
    });
}

/// Delegates to MemoryStream; when `strip` is set it drops every Tail
/// condition from commits, the structural guard this proof depends on.
struct StripsTail {
    inner: MemoryStream,
    strip: bool,
}

#[async_trait::async_trait]
impl StreamProvider for StripsTail {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }
    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, StreamError> {
        self.inner.heads(request).await
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    fn write(
        &self,
        mut request: acyclic_stream::WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<acyclic_stream::WriteOutcome, StreamError>> {
        if self.strip
            && let acyclic_stream::WriteRequest::Commit(commit) = &mut request
        {
            commit
                .conditions
                .retain(|condition| !matches!(condition, CommitCondition::Tail { .. }));
        }
        self.inner.write(request)
    }
}
