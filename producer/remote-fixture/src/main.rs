use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use rcgen::generate_simple_self_signed;
use tokio::{fs, net::TcpListener, time::sleep};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Status};
use tonic::transport::{Identity, Server, ServerTlsConfig};

use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest,
    ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest,
    RecordStream, StreamBounds, StreamError, StreamPath, StreamProvider,
};
use acyclic_stream::grpc::Service;
use acyclic_stream::wire::stream_service_server::StreamServiceServer;

struct FollowGuard {
    active: Arc<AtomicU64>,
    closed_marker: PathBuf,
}

struct ChildGuard {
    closed_marker: PathBuf,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.closed_marker, b"closed\n");
    }
}

impl Drop for FollowGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        let _ = std::fs::write(&self.closed_marker, b"closed\n");
    }
}

struct CountingProvider {
    inner: Arc<MemoryStream>,
    active: Arc<AtomicU64>,
    opened_marker: PathBuf,
    closed_marker: PathBuf,
    child_opened_marker: PathBuf,
    child_closed_marker: PathBuf,
}

#[async_trait]
impl StreamProvider for CountingProvider {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let records = self.inner.follow(path, from).await?;
        self.active.fetch_add(1, Ordering::AcqRel);
        let _ = std::fs::write(&self.opened_marker, b"open\n");
        let guard = FollowGuard {
            active: Arc::clone(&self.active),
            closed_marker: self.closed_marker.clone(),
        };
        Ok(stream::unfold((records, guard), |(mut records, guard)| async move {
            records.next().await.map(|item| (item, (records, guard)))
        })
        .boxed())
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let children = self.inner.children(request).await?;
        let _ = std::fs::write(&self.child_opened_marker, b"open\n");
        let guard = ChildGuard {
            closed_marker: self.child_closed_marker.clone(),
        };
        Ok(stream::unfold((children, guard), |(mut children, guard)| async move {
            sleep(Duration::from_secs(30)).await;
            children.next().await.map(|item| (item, (children, guard)))
        })
        .boxed())
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.inner.children_page(request).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.inner.commit(request).await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.inner.commit_before(request, deadline_unix_millis).await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

async fn write_endpoint_files(
    endpoint_file: &Path,
    certificate_file: &Path,
    endpoint: &str,
    certificate: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(endpoint_file, format!("{endpoint}\n")).await?;
    fs::write(certificate_file, certificate).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::args().nth(1).ok_or("missing fixture directory")?);
    fs::create_dir_all(&root).await?;
    let endpoint_file = root.join("endpoint.txt");
    let certificate_file = root.join("ca.pem");
    let opened_marker = root.join("follow-open");
    let closed_marker = root.join("follow-closed");
    let child_opened_marker = root.join("child-open");
    let child_closed_marker = root.join("child-closed");
    let stop_marker = root.join("stop");
    let _ = fs::remove_file(&opened_marker).await;
    let _ = fs::remove_file(&closed_marker).await;
    let _ = fs::remove_file(&child_opened_marker).await;
    let _ = fs::remove_file(&child_closed_marker).await;
    let _ = fs::remove_file(&stop_marker).await;

    let certified = generate_simple_self_signed(["localhost".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let private_key_pem = certified.signing_key.serialize_pem();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = format!("https://localhost:{}", address.port());
    write_endpoint_files(&endpoint_file, &certificate_file, &endpoint, &certificate_pem).await?;

    let provider = CountingProvider {
        inner: Arc::new(MemoryStream::default()),
        active: Arc::new(AtomicU64::new(0)),
        opened_marker,
        closed_marker,
        child_opened_marker,
        child_closed_marker,
    };
    let service = Service::new(Arc::new(provider));
    let service = StreamServiceServer::with_interceptor(service, |request: Request<()>| {
        let authorized = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some("Bearer fixture-token");
        if authorized {
            Ok(request)
        } else {
            Err(Status::unauthenticated("fixture bearer mismatch"))
        }
    });
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(certificate_pem, private_key_pem)),
            )?
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
    });

    while !stop_marker.exists() {
        sleep(Duration::from_millis(50)).await;
    }
    server.abort();
    let _ = server.await;
    Ok(())
}
