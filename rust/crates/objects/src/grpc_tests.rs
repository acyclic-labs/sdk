//! Read-your-writes over the gRPC adapter, against a replica that applies the
//! primary's writes only when told.
use super::*;
use crate::MemoryObjects;
use acyclic_stream::AppliedRevisions;
use std::sync::Mutex;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[path = "grpc_context_tests.rs"]
mod context;

/// Writes commit on the primary; reads are served from a view that holds only
/// what `catch_up` applied, at the replica's own applied revision.
struct Replica {
    primary: MemoryObjects,
    view: MemoryObjects,
    applied: AppliedRevisions,
    pending: Mutex<Vec<OwnedBytes>>,
}

impl Replica {
    async fn new() -> Result<Arc<Self>, Error> {
        let primary = MemoryObjects::memory();
        primary.flush(None).await?;
        let authority = primary.revisions().await?.expect("memory issues revisions");
        let applied = AppliedRevisions::new(
            authority.scope().clone(),
            authority.current().incarnation,
            0,
        );
        Ok(Arc::new(Self {
            primary,
            view: MemoryObjects::memory(),
            applied,
            pending: Mutex::default(),
        }))
    }

    async fn catch_up(&self) -> Result<(), Error> {
        let pending = std::mem::take(&mut *self.pending.lock().expect("pending"));
        for body in pending {
            self.view.put(body, Vec::new()).await?;
        }
        let primary = self
            .primary
            .revisions()
            .await?
            .expect("memory issues revisions");
        self.applied.advance(primary.current().index);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Blobs for Replica {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        self.pending.lock().expect("pending").push(body.clone());
        self.primary.put(body, references).await
    }
    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        self.primary.put_manifest(manifest).await
    }
    async fn get(&self, digest: Digest, range: ByteRange, maximum: u64) -> Result<Object, Error> {
        let served = self.applied.current();
        Ok(self
            .view
            .get(digest, range, maximum)
            .await?
            .served_at(Some(served)))
    }
    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let served = self.applied.current();
        let mut head = self.view.head(digest).await?;
        head.revision = Some(served);
        Ok(head)
    }
    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        self.primary.delete(digest).await
    }
    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        let served = self.applied.current();
        let mut page = self.view.list(after, limit).await?;
        page.revision = Some(served);
        Ok(page)
    }
    async fn flush(&self, only: Option<&[Digest]>) -> Result<(), Error> {
        self.primary.flush(only).await
    }
    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        self.primary.retain(root, references).await
    }
    async fn release(&self, root: &RootId) -> Result<crate::Released, Error> {
        self.primary.release(root).await
    }
    async fn collect(&self) -> Result<Collection, Error> {
        self.primary.collect().await
    }
    async fn usage(&self) -> Result<Usage, Error> {
        self.primary.usage().await
    }
    async fn revisions(&self) -> Result<Option<&AppliedRevisions>, Error> {
        Ok(Some(&self.applied))
    }
}

/// A client of `replica` over an in-process HTTP/2 transport.
fn client(replica: &Arc<Replica>) -> GrpcObjects {
    let replica = Arc::clone(replica);
    let resolve: AccountResolver = Arc::new(move |_| Ok(Arc::clone(&replica) as Arc<dyn Blobs>));
    client_over(
        resolve,
        CredentialProvider::new(|| async { Ok("fixture".to_owned()) }),
    )
}

fn client_over(resolve: AccountResolver, credential: CredentialProvider) -> GrpcObjects {
    let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
            let resolve = Arc::clone(&resolve);
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = futures::stream::once(async { Ok::<_, std::io::Error>(server) });
                    let _ = tonic::transport::Server::builder()
                        .add_service(BlobsServer::new(resolve).into_service())
                        .serve_with_incoming(incoming)
                        .await;
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }),
    );
    GrpcObjects::over(channel, credential)
}

#[tokio::test]
async fn caller_supplied_channel_refreshes_credentials_for_each_rpc() -> TestResult {
    let provider = Arc::new(MemoryObjects::memory());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let resolve: AccountResolver = Arc::new(move |metadata| {
        seen.lock().expect("credentials").push(
            metadata
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
        Ok(provider.clone())
    });
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let refreshed = calls.clone();
    let credential = CredentialProvider::new(move || {
        let index = refreshed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move { Ok(format!("credential-{index}")) }
    });
    let client = client_over(resolve, credential);
    client
        .put(OwnedBytes::from_static(b"first"), Vec::new())
        .await?;
    client
        .put(OwnedBytes::from_static(b"second"), Vec::new())
        .await?;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(
        *observed.lock().expect("credentials"),
        vec![
            Some("Bearer credential-0".to_owned()),
            Some("Bearer credential-1".to_owned()),
        ]
    );
    Ok(())
}

async fn written(writer: &GrpcObjects, body: &'static [u8]) -> Result<(Digest, Revision), Error> {
    let put = writer.put(body.into(), Vec::new()).await?;
    let verified = put
        .verified()
        .ok_or(Error::from(wire::ErrorCode::Unavailable))?;
    Ok((
        verified.digest,
        verified
            .revision
            .clone()
            .ok_or(Error::from(wire::ErrorCode::Unavailable))?,
    ))
}

/// Settles everything ready on the current-thread runtime without the read.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn a_read_after_a_write_waits_for_the_lagging_replica() -> TestResult {
    let replica = Replica::new().await?;
    let writer = client(&replica);
    let (digest, committed) = written(&writer, b"fresh").await?;
    assert!(replica.applied.current().index < committed.index);
    // Negative control: a client that never saw the write reads the stale view.
    let stale = client(&replica).head(digest).await;
    assert_eq!(
        stale.map_err(|error| error.code),
        Err(wire::ErrorCode::NotFound)
    );

    let reader = writer.clone();
    let read = tokio::spawn(async move { reader.head(digest).await });
    settle().await;
    assert!(!read.is_finished(), "a read below its floor must wait");
    replica.catch_up().await?;
    let head = read.await??;
    assert_eq!(head.digest, digest);
    assert!(
        head.revision
            .is_some_and(|served| served.index >= committed.index)
    );
    // Lists forward the same floor.
    let page = writer.list(None, 10).await?;
    assert_eq!(page.digests, vec![digest]);
    Ok(())
}

#[tokio::test]
async fn an_explicit_minimum_carries_a_write_across_clients() -> TestResult {
    let replica = Replica::new().await?;
    let (digest, committed) = written(&client(&replica), b"shared").await?;
    let other = client(&replica);
    // Negative control: without the minimum the other client reads stale.
    assert_eq!(
        other.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::NotFound)
    );
    other.revisions().observe(&committed);
    let reader = other.clone();
    let read = tokio::spawn(async move { reader.get(digest, ByteRange::FULL, 64).await });
    settle().await;
    assert!(
        !read.is_finished(),
        "an explicit minimum must hold the read"
    );
    replica.catch_up().await?;
    assert_eq!(read.await??.body().as_ref(), b"shared");
    Ok(())
}

#[tokio::test]
async fn a_foreign_minimum_cannot_retire_a_required_floor() -> TestResult {
    let replica = Replica::new().await?;
    let (digest, committed) = written(&client(&replica), b"owned").await?;
    replica.catch_up().await?;
    // Negative control: the account's own revision is accepted.
    let own = client(&replica);
    own.revisions().observe(&committed);
    assert_eq!(own.head(digest).await?.digest, digest);

    let foreign = client(&replica);
    foreign
        .revisions()
        .observe(&AppliedRevisions::in_process().at(1));
    assert_eq!(
        foreign.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    // Another incarnation of the same scope is foreign too.
    let reset = client(&replica);
    reset.revisions().observe(&Revision {
        incarnation: committed.incarnation.wrapping_add(1),
        ..committed.clone()
    });
    assert_eq!(
        reset.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    // Observing both incarnations cannot silently replace either requirement.
    own.revisions().observe(&Revision {
        incarnation: committed.incarnation.wrapping_add(1),
        ..committed.clone()
    });
    assert_eq!(
        own.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    // A refusal proves no retirement. The original minimum still refuses.
    assert_eq!(
        foreign.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    // Explicit routing selects a relevant authority, without deleting another
    // view's requirements or interpreting its foreign index as progress.
    foreign.revisions().observe(&committed);
    let scoped = foreign.with_revision_scope(committed.scope.clone());
    assert_eq!(scoped.head(digest).await?.digest, digest);
    assert_eq!(
        foreign.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    Ok(())
}

#[tokio::test]
async fn the_call_deadline_bounds_the_wait() -> TestResult {
    let replica = Replica::new().await?;
    let writer = client(&replica).with_transfer_timeout(Duration::from_millis(300))?;
    let (digest, _) = written(&writer, b"late").await?;
    let started = tokio::time::Instant::now();
    let read = writer.get(digest, ByteRange::FULL, 64).await;
    assert_eq!(
        read.map_err(|error| error.code),
        Err(wire::ErrorCode::DeadlineElapsed)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    // Negative control: the same deadline admits a read whose floor applies in time.
    let reader = writer.clone();
    let read = tokio::spawn(async move { reader.get(digest, ByteRange::FULL, 64).await });
    settle().await;
    replica.catch_up().await?;
    assert_eq!(read.await??.body().as_ref(), b"late");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn credential_refresh_keeps_the_original_deadline_and_cancellation() -> TestResult {
    use acyclic_auth::credential::{
        CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Refresh(Arc<AtomicUsize>);
    impl Drop for Refresh {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    for cancel in [false, true] {
        let replica = Replica::new().await?;
        let mut objects = client(&replica).with_transfer_timeout(Duration::from_secs(17))?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let dropped = Arc::new(AtomicUsize::new(0));
        let refreshed = Arc::clone(&dropped);
        objects.credential = CredentialCache::new(
            move |_| {
                let entered = Arc::clone(&entered);
                let refreshed = Arc::clone(&refreshed);
                async move {
                    let _refresh = Refresh(refreshed);
                    entered.lock().unwrap().take().unwrap().send(()).unwrap();
                    std::future::pending::<Result<CredentialSnapshot, _>>().await
                }
            },
            || CredentialValidity {
                now_unix_seconds: 1,
                revision: Some(CredentialRevision::from([1; 32])),
            },
        )
        .into();
        let beginning = tokio::time::Instant::now();
        let mut request = Box::pin(objects.put(b"held".as_slice().into(), Vec::new()));
        tokio::select! {
            result = &mut request => panic!("stalled refresh completed: {result:?}"),
            result = started => result?,
        }
        if cancel {
            drop(request);
        } else {
            tokio::time::advance(Duration::from_secs(17)).await;
            assert_eq!(
                request.await.map_err(|error| error.code),
                Err(wire::ErrorCode::DeadlineElapsed),
            );
            assert_eq!(beginning.elapsed(), Duration::from_secs(17));
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(
            replica.pending.lock().unwrap().is_empty(),
            "no RPC before refresh"
        );

        // Both refusals leave a new, valid source usable on the same channel.
        objects.credential = CredentialProvider::new(|| async { Ok("fixture".into()) });
        objects.put(b"ready".as_slice().into(), Vec::new()).await?;
        assert_eq!(replica.pending.lock().unwrap().len(), 1);
    }
    Ok(())
}

#[test]
fn grpc_timeouts_parse_exactly() {
    assert_eq!(grpc_timeout("300m"), Some(Duration::from_millis(300)));
    assert_eq!(grpc_timeout("2S"), Some(Duration::from_secs(2)));
    assert_eq!(grpc_timeout("1H"), Some(Duration::from_secs(3600)));
    for invalid in ["", "m", "123456789S", "1x", "-1S", "1.5S"] {
        assert_eq!(grpc_timeout(invalid), None, "{invalid}");
    }
}

#[tokio::test]
async fn a_refused_wait_keeps_its_own_failure() -> TestResult {
    let replica = Replica::new().await?;
    let (digest, committed) = written(&client(&replica), b"pending").await?;
    // The replica has not applied `committed`, so its reply sits below it.
    let mixed = client(&replica);
    mixed.revisions().observe(&committed);
    mixed
        .revisions()
        .observe(&AppliedRevisions::in_process().at(1));
    assert_eq!(
        mixed.head(digest).await.map_err(|error| error.code),
        Err(wire::ErrorCode::RevisionForeign)
    );
    // Negative control: once applied, the same account's minimum serves.
    replica.catch_up().await?;
    let own = client(&replica);
    own.revisions().observe(&committed);
    assert_eq!(own.head(digest).await?.digest, digest);
    Ok(())
}

#[tokio::test]
async fn a_read_without_its_revision_is_refused_under_a_minimum() -> TestResult {
    let replica = Replica::new().await?;
    let objects = client(&replica);
    let floor = RevisionCut::of(AppliedRevisions::in_process().at(1));
    assert_eq!(
        objects
            .observed(Ok(tonic::Response::new(())), Some(&floor))
            .map(|_| ())
            .map_err(|error| error.code),
        Err(wire::ErrorCode::Unavailable)
    );
    // Negative controls: without a minimum the same reply is accepted.
    assert!(objects.observed(Ok(tonic::Response::new(())), None).is_ok());
    let empty = RevisionCut::default();
    assert!(
        objects
            .observed(Ok(tonic::Response::new(())), Some(&empty))
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn a_transfer_deadline_is_the_callers_alone() -> TestResult {
    let replica = Replica::new().await?;
    // Hours-long transfers are the caller's call, up to what grpc-timeout carries.
    let writer = client(&replica).with_transfer_timeout(Duration::from_secs(4 * 3600))?;
    let (digest, _) = written(&writer, b"slow").await?;
    replica.catch_up().await?;
    assert_eq!(
        writer
            .get(digest, ByteRange::FULL, 64)
            .await?
            .body()
            .as_ref(),
        b"slow"
    );
    client(&replica).with_transfer_timeout(MAX_DEADLINE)?;
    // Negative controls: no deadline, and one the protocol cannot carry.
    for refused in [Duration::ZERO, MAX_DEADLINE + Duration::from_secs(3600)] {
        assert!(matches!(
            client(&replica).with_transfer_timeout(refused),
            Err(ConnectError::InvalidConfiguration)
        ));
    }
    Ok(())
}
