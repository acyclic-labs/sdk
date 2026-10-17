//! The surviving local owner serves the SDK Streams API, never harness RPCs.
use super::*;
use crate::sync::owned::Arc as StateArc;
use crate::task_volume::{Component, FileKey, FileValue, TaskFile, TaskVolume};
use acyclic_stream::StreamProvider as _;
use acyclic_stream::grpc::Service as StreamService;
use fs2::FileExt as _;
use futures::StreamExt as _;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{
    Status,
    body::Body,
    codegen::{Service, http},
    server::NamedService,
    transport::{Identity, Server, ServerTlsConfig},
};

const CONTROL: &str = "owner/control";

fn incoming(
    listener: tokio::net::TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::try_unfold(listener, |listener| async move {
        let (socket, _) = listener.accept().await?;
        // Tonic cannot configure sockets supplied by a custom incoming stream.
        // Small TLS/HTTP2 records must not wait for a peer's delayed ACK.
        socket.set_nodelay(true)?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    })
}

#[derive(Clone)]
struct Owner {
    registry: Arc<registry::Registry>,
    control: Arc<LocalStream>,
    secret: [u8; 32],
    shutdown: Arc<TaskVolume<LocalStream>>,
    instance: uuid::Uuid,
}

#[derive(Clone)]
struct Follows(Owner);

impl NamedService for Follows {
    const NAME: &'static str =
        <acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer<
            acyclic_stream::session::Service<LocalStream>,
        > as NamedService>::NAME;
}

impl Service<http::Request<Body>> for Follows {
    type Response = <Owner as Service<http::Request<Body>>>::Response;
    type Error = Infallible;
    type Future = <Owner as Service<http::Request<Body>>>::Future;
    fn poll_ready(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Infallible>> {
        self.0.poll_ready(context)
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        self.0.call(request)
    }
}

impl NamedService for Owner {
    const NAME: &'static str = <acyclic_stream::wire::stream_service_server::StreamServiceServer<
        StreamService<LocalStream>,
    > as NamedService>::NAME;
}

impl Service<http::Request<Body>> for Owner {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let streaming = response::streaming(request.uri().path());
        let owner = self.clone();
        let key = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Error::Unauthorized("local Streams credential is absent".into()))
            .and_then(|token| credential::verify(&owner.secret, token));
        Box::pin(async move {
            let selected: Result<_> = async {
                let key = key?;
                if key == "owner" {
                    return Ok(provider::ClientStream::control(Arc::clone(&owner.control)));
                }
                let harness = owner.registry.get(&key).await?;
                Ok(provider::ClientStream::workspace(harness.task_stream()))
            }
            .await;
            let provider = match selected {
                Ok(provider) => provider,
                Err(error) => return Ok(Status::permission_denied(error.to_string()).into_http()),
            };
            let response = if request.uri().path().split('/').nth(1) == Some(Follows::NAME) {
                let mut service = acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer::new(
                    acyclic_stream::session::Service::new(Arc::new(provider), "local".into(), Default::default()),
                );
                service.call(request).await?
            } else {
                let mut service =
                    acyclic_stream::wire::stream_service_server::StreamServiceServer::new(
                        StreamService::new(Arc::new(provider)),
                    );
                service.call(request).await?
            };
            if streaming {
                let shutdown = Arc::clone(&owner.shutdown);
                let instance = owner.instance;
                Ok(response::close_on_stop(response, async move { stopped(&shutdown, instance).await }))
            } else {
                Ok(response)
            }
        })
    }
}

fn stop_key(instance: uuid::Uuid) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new("owner").map_err(storage)?,
        file: TaskFile::Request(Component::new(instance.to_string()).map_err(storage)?),
    })
}

pub(super) async fn stopped<P: acyclic_stream::StreamProvider + ?Sized>(
    control: &TaskVolume<P>,
    instance: uuid::Uuid,
) -> Result<()> {
    super::super::client::follow_file(control, stop_key(instance)?)
        .await
        .map_err(storage)?
        .await
        .map_err(storage)
        .map(|_| ())
}

/// Runs the existing local ownership boundary until an authenticated stop file.
pub fn run_local_owner() -> impl Future<Output = Result<()>> {
    Box::pin(serve())
}

async fn serve() -> Result<()> {
    let ready = acyclic_native_runtime::take_service_ready_signal().map_err(storage)?;
    let root = directory()?;
    let lock = bootstrap::lock(&root, "instance.lock")?;
    match lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
        Err(error) => return Err(storage(error)),
    }
    let control = Arc::new(
        LocalStream::open(root.join("stream"), Default::default())
            .await
            .map_err(storage)?,
    );
    let files = Arc::new(
        TaskVolume::open(
            Arc::clone(&control),
            StreamPath::new(CONTROL).map_err(storage)?,
        )
        .await
        .map_err(storage)?,
    );
    let secret = bootstrap::secret(&root)?;
    let instance = uuid::Uuid::new_v4();
    let registry = Arc::new(registry::Registry::new(
        root.clone(),
        Arc::clone(&files),
        instance,
    ));
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(storage)?;
    let certificate =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .map_err(storage)?;
    let pem = certificate.cert.pem();
    let address = Address {
        endpoint: format!("https://{}", listener.local_addr().map_err(storage)?),
        certificate: pem.clone(),
        instance,
    };
    bootstrap::publish(
        &root.join("address.json"),
        &serde_json::to_vec(&address).map_err(storage)?,
        true,
    )?;
    let owner = Owner {
        registry: Arc::clone(&registry),
        control,
        secret,
        shutdown: Arc::clone(&files),
        instance,
    };
    let server = Server::builder()
        .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
            pem,
            certificate.signing_key.serialize_pem(),
        )))
        .map_err(storage)?
        .add_service(owner.clone())
        .add_service(Follows(owner));
    let incoming = incoming(listener);
    if let Some(ready) = ready {
        let _ = ready.signal();
    }
    let shutdown = Arc::clone(&files);
    let result = server
        .serve_with_incoming_shutdown(Box::pin(incoming), async move {
            let _ = stopped(&shutdown, instance).await;
        })
        .await;
    let drained = registry.drain().await;
    drop(registry);
    drop(files);
    drop(lock);
    result.map_err(storage)?;
    drained
}

/// Requests shutdown through the same SDK file publication path and awaits drain.
pub async fn stop_local_owner() -> Result<()> {
    let root = directory()?;
    let address = Address::from_bytes(&read(&root.join("address.json"))?)?;
    let key = bootstrap::secret(&root)?;
    let client = Arc::new(connect(&address, &credential::credential(&key, "owner")).await?);
    let files = TaskVolume::open_existing(client, StreamPath::new(CONTROL).map_err(storage)?)
        .await
        .map_err(storage)?;
    let file = FileValue::new(stop_key(address.instance)?, b"{}".as_slice());
    super::super::publish_request(&files, &file, None, None)
        .await
        .map_err(storage)?;
    drop(files);
    let lock = bootstrap::lock(&root, "instance.lock")?;
    tokio::task::spawn_blocking(move || lock.lock_exclusive().map_err(storage))
        .await
        .map_err(storage)??;
    Ok(())
}

pub(super) async fn drive(
    harness: StateArc<LocalHarness>,
    stop: Arc<TaskVolume<LocalStream>>,
    instance: uuid::Uuid,
) -> Result<()> {
    let mut children = harness
        .task_stream()
        .children(acyclic_stream::ChildrenRequest {
            parent: Some(StreamPath::new(super::super::local_paths::REQUESTS).map_err(storage)?),
            limit: 64,
        })
        .await
        .map_err(storage)?;
    let mut tasks = futures::stream::FuturesUnordered::new();
    let stopping = stopped(&stop, instance);
    tokio::pin!(stopping);
    let result = loop {
        tokio::select! {
            biased;
            result = &mut stopping => break result,
            _ = workers::completed(&mut tasks), if !tasks.is_empty() => {}
            child = children.next() => {
                let path = match child {
                    Some(Ok(child)) => child.path,
                    Some(Err(error)) => break Err(storage(error)),
                    None => break Err(Error::Storage("local task discovery ended".into())),
                };
                match super::super::local_paths::task(super::super::local_paths::REQUESTS,&path) {
                    Ok(task) => {
                        let harness = StateArc::clone(&harness);
                        let stop = Arc::clone(&stop);
                        // A task failure cannot select the owner's stop path:
                        // each isolated attempt completes with (), after its effects settle.
                        tasks.push(workers::start(async move {
                            if let Err(error) = drive_task(harness, task, stop, instance).await {
                                tracing::warn!(%task, %error, "request task quarantined");
                            }
                        }));
                    }
                    Err(error) => tracing::warn!(%error, "invalid request namespace quarantined"),
                }
            }
        }
    };
    if result.is_err() {
        // Fail closed on a broken provider follow, then drain every active
        // effect before releasing the workspace's sole owner.
        let file = FileValue::new(stop_key(instance)?, b"{}".as_slice());
        super::super::publish_request(&stop, &file, None, None)
            .await
            .map_err(storage)?;
    }
    workers::drain(&mut tasks).await;
    result
}

async fn drive_task(
    harness: StateArc<LocalHarness>,
    task: crate::TaskId,
    stop: Arc<TaskVolume<LocalStream>>,
    instance: uuid::Uuid,
) -> Result<()> {
    if harness
        .registered_until(task, stopped(&stop, instance))
        .await?
    {
        return Ok(());
    }
    let volumes = harness.external_volumes(task).await?;
    let mut files = request_feed::follow(
        harness.task_stream().as_ref(),
        volumes.requests.path().clone(),
    )
    .await?;
    let stopping = stopped(&stop, instance);
    tokio::pin!(stopping);
    loop {
        let request = tokio::select! {
            biased;
            stopped = &mut stopping => return stopped,
            request = files.next() => request
                .ok_or_else(|| Error::Storage("local request follow ended".into()))??,
        };
        if request.task_id() != task {
            continue;
        }
        let result = harness
            .external_request_until(&request, stopped(&stop, instance))
            .await?;
        let Some(result) = result else {
            return Ok(());
        };
        harness.external_child(&request, &result).await?;
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
