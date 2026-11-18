//! Transport identity and account metadata across the public Stream client.
use acyclic_stream::{
    IdempotencyKey, StreamProvider,
    grpc::{ChannelConnector, Client, ConnectError, CredentialError, CredentialProvider, Service},
};
use acyclic_stream_wire::wire;
use futures::{StreamExt as _, stream};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tonic::{Request, transport::Endpoint};

#[tokio::test]
async fn request_metadata_reaches_unary_sessions_and_resumed_streams()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use wire::stream_service_server::StreamServiceServer;
    let memory = Arc::new(acyclic_stream::MemoryStream::default());
    let path = "trust/v1/test";
    acyclic_stream::Stream::new(memory.clone(), path)?
        .append("record", IdempotencyKey::unique())
        .await
        .map_err(|error| format!("append: {error:?}"))?;
    let headers = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let (seen, admitted) = (headers.clone(), requests.clone());
    let served = memory.clone();
    let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
            let (seen, admitted, served) = (seen.clone(), admitted.clone(), served.clone());
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    // Both services observe every RPC's metadata.
                    let observe = move |request: Request<()>| {
                        assert_eq!(
                            request.metadata().get("x-cloud-account").unwrap(),
                            "account-fixture"
                        );
                        admitted.fetch_add(1, Ordering::SeqCst);
                        if request.metadata().contains_key("authorization") {
                            seen.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(request)
                    };
                    let session = acyclic_stream::session::Service::new(
                        served.clone(),
                        acyclic_stream::session::testing::authority(),
                        "fixture".into(),
                        acyclic_stream::session::Limits::default(),
                    );
                    tonic::transport::Server::builder()
                        .add_service(StreamServiceServer::with_interceptor(
                            Service::new(served),
                            observe.clone(),
                        ))
                        .add_service(
                            wire::stream_session_service_server::StreamSessionServiceServer::with_interceptor(
                                session, observe,
                            ),
                        )
                        .serve_with_incoming(stream::once(async {
                            Ok::<_, std::io::Error>(server)
                        }))
                        .await
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }),
    );
    // Every routed node is this one server, reached over the same channel.
    let home = channel.clone();
    let route: ChannelConnector = Arc::new(move |_| {
        let home = home.clone();
        Box::pin(async move { Ok(home) })
    });
    let client = Client::from_authenticated_channel(
        channel,
        CredentialProvider::transport_identity(),
        route,
    );
    for (key, value) in [
        ("invalid key", "value"),
        ("authorization", "Bearer old"),
        ("x-data-bin", "value"),
        ("x-data", "bad\nvalue"),
        ("x-data", "\u{e9}"),
    ] {
        assert!(matches!(
            client.clone().with_request_metadata([(key, value)]),
            Err(ConnectError::InvalidMetadata)
        ));
    }
    let client = Arc::new(client.with_request_metadata([("x-cloud-account", "account-fixture")])?);
    client.inspect_idempotency(IdempotencyKey::unique()).await?;
    // The memory server admits sessions in-band; a platform server admits
    // trust paths by the channel's transport identity instead.
    client
        .session_client()
        .map_err(|error| format!("session: {error:?}"))?
        .register_credential(0, acyclic_stream::session::testing::credential())
        .map_err(|error| format!("register: {error:?}"))?;
    let source: &dyn acyclic_stream_trust::trust_feed::Source = &client;
    let tail = source
        .tail(path)
        .await
        .map_err(|error| format!("tail: {error:?}"))?;
    let mut records = source
        .follow(path, 0)
        .await
        .map_err(|error| format!("follow: {error:?}"))?;
    let first = records
        .next()
        .await
        .expect("one record")
        .map_err(|error| format!("record: {error:?}"))?;
    assert_eq!(first.value.as_ref(), b"record");
    assert_eq!(tail, 1);
    drop(records);
    acyclic_stream::Stream::new(memory.clone(), path)?
        .append("resumed", IdempotencyKey::unique())
        .await?;
    let mut resumed = source.follow(path, 1).await?;
    assert_eq!(
        resumed
            .next()
            .await
            .expect("resumed record")?
            .value
            .as_ref(),
        b"resumed"
    );
    assert!(requests.load(Ordering::SeqCst) > 0);
    assert_eq!(headers.load(Ordering::SeqCst), 0);
    // Negative control: a bearer client over the same server is observed.
    let bearer = Arc::new(client.with_credential("fixture")?);
    // It holds no in-band session credential, so the server may refuse it;
    // the header is observed either way.
    let _refused = acyclic_stream_trust::trust_feed::Source::tail(&bearer, path).await;
    assert!(headers.load(Ordering::SeqCst) > 0);
    // Transports that cannot omit authorization refuse rather than send none.
    assert!(matches!(
        CredentialProvider::transport_identity().resolve().await,
        Err(CredentialError::TransportIdentity)
    ));
    Ok(())
}
