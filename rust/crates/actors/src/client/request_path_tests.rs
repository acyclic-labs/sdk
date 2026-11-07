use super::*;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tonic::{
    body::Body,
    codegen::{Service, http},
};

#[derive(Clone)]
struct Echo(Arc<AtomicUsize>);
impl tonic::server::NamedService for Echo {
    const NAME: &'static str = "acyclic.actors.v1.ActorsService";
}
impl tonic::server::UnaryService<wire::InvokeActorRequest> for Echo {
    type Response = wire::InvokeActorResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;
    fn call(&mut self, request: tonic::Request<wire::InvokeActorRequest>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        let expected = if request.get_ref().body.first() == Some(&8) {
            "Bearer renewed"
        } else {
            "Bearer fixture"
        };
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some(expected)
        );
        let request = request.into_inner();
        std::future::ready(Ok(tonic::Response::new(wire::InvokeActorResponse {
            deployment_envelope: Vec::new(),
            status: 200,
            body: request.body,
            headers: request.headers,
        })))
    }
}
impl Service<http::Request<Body>> for Echo {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/acyclic.actors.v1.ActorsService/InvokeActor"
        );
        let service = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .unary(service, request)
                .await)
        })
    }
}

#[tokio::test]
async fn request_path_binary_calls_keep_admission_credentials_and_response_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("http://{}", incoming.local_addr()?);
    let calls = Arc::new(AtomicUsize::new(0));
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Echo(Arc::clone(&calls)))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect()
            .await?;
        let client = Client::from_channel(
            channel.clone(),
            CredentialProvider::new(|| std::future::ready(Ok("fixture".into()))),
        );
        let request = wire::InvokeActorRequest {
            deployment_envelope: Vec::new(),
            invocation_order: None,
            namespace: None,
            account_id: Vec::new(),
            location_hint: None,
            binding_name: None,
            rpc_method: None,
            actor_id: "actor-a".into(),
            method: "POST".into(),
            url: "/work".into(),
            body: vec![7; 65_536],
            headers: vec![],
        };
        for _ in 0..2 {
            let response = wire::InvokeActorResponse::decode(
                client
                    .call("InvokeActor", &request.encode_to_vec())
                    .await?
                    .as_slice(),
            )?;
            assert_eq!(response.status, 200);
            assert_eq!(response.body, request.body);
        }
        let renewed = wire::InvokeActorRequest {
            deployment_envelope: Vec::new(),
            body: vec![8],
            ..request.clone()
        };
        let response = wire::InvokeActorResponse::decode(
            client
                .call_with_credential("InvokeActor", &renewed.encode_to_vec(), "renewed")
                .await?
                .as_slice(),
        )?;
        assert_eq!(response.body, vec![8]);
        client.call("InvokeActor", &request.encode_to_vec()).await?;
        let invalid = wire::InvokeActorRequest {
            deployment_envelope: Vec::new(),
            actor_id: String::new(),
            ..request
        };
        assert!(matches!(
            client.call("InvokeActor", &invalid.encode_to_vec()).await,
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            client.call("InvokeActor", &[0xff]).await,
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            client.call("DeployNamespace", &[]).await,
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            client.call("Unknown", &[]).await,
            Err(Error::Configuration(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let renewals = Arc::new(AtomicUsize::new(0));
        let resolved = renewals.clone();
        let client = client.with_credential_provider(CredentialProvider::new(move || {
            resolved.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok("renewed".into()))
        }));
        let typed = domain::InvokeActorRequest::try_from(renewed.clone())?;
        assert_eq!(client.invoke_actor(typed.clone()).await?.body(), &[8]);
        let output = client.call("InvokeActor", &renewed.encode_to_vec()).await?;
        assert_eq!(
            wire::InvokeActorResponse::decode(output.as_slice())?.body,
            vec![8]
        );
        assert_eq!(renewals.load(Ordering::Relaxed), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 6);

        // An ended budget wins before input decoding and credential callbacks.
        let client = client.with_context(acyclic_auth::operation::Context::new(
            1,
            std::future::ready(()),
            std::future::pending(),
        ));
        for bytes in [&[0xff][..], &renewed.encode_to_vec()] {
            assert!(matches!(
                client.call("InvokeActor", bytes).await,
                Err(Error::Stopped(acyclic_auth::operation::Stopped::Deadline))
            ));
        }
        assert!(matches!(
            client.invoke_actor(typed.clone()).await,
            Err(Error::Stopped(acyclic_auth::operation::Stopped::Deadline))
        ));
        assert_eq!(renewals.load(Ordering::Relaxed), 2);

        // Cancellation during credential resolution drops that wait. Dropping
        // its sole producer has the same typed outcome as an explicit signal.
        for explicit in [false, true] {
            let (stop, stopped) = futures::channel::oneshot::channel::<()>();
            let stop = Arc::new(std::sync::Mutex::new(Some(stop)));
            let resolved = Arc::clone(&renewals);
            let client = Client::from_channel(
                channel.clone(),
                CredentialProvider::new(move || {
                    resolved.fetch_add(1, Ordering::Relaxed);
                    let stop = stop.lock().unwrap().take().unwrap();
                    if explicit {
                        let _ = stop.send(());
                    } else {
                        drop(stop);
                    }
                    std::future::pending()
                }),
            )
            .with_context(acyclic_auth::operation::Context::new(
                10,
                std::future::pending(),
                async {
                    let _ = stopped.await;
                },
            ));
            assert!(matches!(
                client.invoke_actor(typed.clone()).await,
                Err(Error::Stopped(acyclic_auth::operation::Stopped::Cancelled))
            ));
        }
        assert_eq!(renewals.load(Ordering::Relaxed), 4);
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        // Observe the shipped request path at its credential event, before
        // protobuf serialization. Both direct and namespace calls must move
        // payload storage; admission work cannot grow with the body length.
        for bound in [false, true] {
            let mut admission_bytes = Vec::new();
            for length in [1, 65_536] {
                let (release, credential) = tokio::sync::oneshot::channel();
                let credential = Arc::new(std::sync::Mutex::new(Some(credential)));
                let observed = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
                let measured = Arc::clone(&observed);
                let client = Arc::new(Client::from_channel(
                    channel.clone(),
                    CredentialProvider::new(move || {
                        measured.store(
                            acyclic_test_support::current_allocations()
                                .unwrap()
                                .bytes_total,
                            Ordering::Relaxed,
                        );
                        let credential = credential.lock().unwrap().take().unwrap();
                        async move {
                            credential
                                .await
                                .map_err(|_| acyclic_auth::credential::CredentialError::Invalid)
                        }
                    }),
                ));
                let namespace =
                    crate::namespace::Namespace::new(b"account", b"ClassOne", Arc::clone(&client))?;
                let stub = namespace.get_by_name("actor-a", None)?;
                let body = vec![7; length];
                let mut work = std::pin::pin!(async {
                    let response = if bound {
                        stub.fetch("POST".into(), "/work".into(), body, vec![])
                            .await?
                    } else {
                        let request = domain::InvokeActorRequest::new(
                            domain::ActorId::new("actor-a".into())?,
                            "POST".into(),
                            "/work".into(),
                            body,
                            vec![],
                        );
                        client.invoke_actor(request).await?
                    };
                    Ok::<_, Box<dyn std::error::Error>>(response)
                });
                acyclic_test_support::measure(|| {
                    assert!(
                        work.as_mut()
                            .poll(&mut Context::from_waker(std::task::Waker::noop()))
                            .is_pending()
                    );
                });
                let bytes = observed.load(Ordering::Relaxed);
                assert_ne!(
                    bytes,
                    u64::MAX,
                    "request must reach its credential producer"
                );
                admission_bytes.push(bytes);
                release.send("fixture".into()).unwrap();
                assert_eq!(work.await?.body(), vec![7; length]);
            }
            eprintln!("request admission bound={bound} bytes={admission_bytes:?}");
            assert_eq!(
                admission_bytes[0], admission_bytes[1],
                "admitted request must not copy payload storage"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 10);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result
}
