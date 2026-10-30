use super::*;
use futures::FutureExt;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn request_credentials_never_mutate_the_original_client() {
    let original = Client::from_channel(
        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
        authorization("fixture").unwrap(),
    );
    let strategy = prop::collection::vec("[a-zA-Z0-9_-]{1,128}", 1..16);
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(128))
        .run(&strategy, |tokens| {
            for token in tokens {
                let renewed = original.with_credential(&token).unwrap();
                let request = futures::executor::block_on(renewed.request(())).unwrap();
                let metadata = request.metadata().get("authorization").unwrap();
                prop_assert_eq!(metadata.to_str().unwrap(), format!("Bearer {token}"));
                prop_assert!(metadata.is_sensitive());
                let fixed = futures::executor::block_on(original.request(())).unwrap();
                prop_assert_eq!(
                    fixed
                        .metadata()
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    "Bearer fixture"
                );
                for delimiter in ['\r', '\n', '\0'] {
                    let invalid = format!("{token}{delimiter}");
                    prop_assert!(original.with_credential(&invalid).is_err());
                }
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn credential_provider_renews_each_physical_retry_without_changing_the_body() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,32}", 2..8),
            |tokens| {
                runtime.block_on(async {
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let values = tokens.clone();
                    let client = Client::from_channel(
                        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
                        authorization("stale").unwrap(),
                    )
                    .with_credential_provider(CredentialProvider::new(move || {
                        let value = values.get(calls.fetch_add(1, Ordering::Relaxed)).cloned();
                        futures::future::ready(
                            value.ok_or(acyclic_auth::credential::CredentialError::Invalid),
                        )
                    }));
                    let mut sent = 0;
                    let (response, _) = client
                        .unary(
                            wire::TailRequest {
                                path: "same/path".into(),
                                ..Default::default()
                            },
                            |_, request| {
                                assert_eq!(request.get_ref().path, "same/path");
                                let header = request.metadata().get("authorization").unwrap();
                                assert!(header.is_sensitive());
                                assert_eq!(
                                    header.to_str().unwrap(),
                                    format!("Bearer {}", tokens.get(sent).unwrap())
                                );
                                sent += 1;
                                Box::pin(futures::future::ready(if sent < tokens.len() {
                                    Err(Status::unavailable("retry"))
                                } else {
                                    Ok(Response::new(wire::TailResponse {
                                        tail: 7,
                                        ..Default::default()
                                    }))
                                }))
                            },
                        )
                        .await
                        .unwrap();
                    prop_assert_eq!(response.tail, 7);
                    prop_assert_eq!(sent, tokens.len());
                    prop_assert_eq!(index.load(Ordering::Relaxed), tokens.len());
                    // Exhaustion fails before transport; the static credential cannot be used.
                    let result: Result<((), Completion), StreamError> = client
                        .unary((), |_, _| {
                            unreachable!("provider failure reached transport")
                        })
                        .await;
                    prop_assert!(matches!(result, Err(StreamError::AccessDenied)));
                    Ok(())
                })
            },
        )
        .unwrap();
}

#[test]
fn session_request_authorization_is_fresh_sensitive_and_has_no_stale_fallback() {
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,32}", 1..8),
            |tokens| {
                let calls = Arc::new(AtomicUsize::new(0));
                let index = calls.clone();
                let values = tokens.clone();
                let provider = CredentialProvider::new(move || {
                    futures::future::ready(
                        values
                            .get(index.fetch_add(1, Ordering::SeqCst))
                            .cloned()
                            .ok_or(CredentialError::Invalid),
                    )
                });
                let stale = authorization("stale").unwrap();
                for (index, token) in tokens.iter().enumerate() {
                    let (value, _) = futures::executor::block_on(request_authorization(
                        Some(&stale),
                        Some(&provider),
                    ))
                    .unwrap();
                    let value = value.expect("bearer attached");
                    prop_assert_eq!(value.to_str().unwrap(), format!("Bearer {token}"));
                    prop_assert!(value.is_sensitive());
                    prop_assert_eq!(calls.load(Ordering::SeqCst), index + 1);
                }
                let failed = futures::executor::block_on(request_authorization(
                    Some(&stale),
                    Some(&provider),
                ));
                prop_assert_eq!(failed.unwrap_err().code(), Code::Unauthenticated);
                Ok(())
            },
        )
        .unwrap();
}

#[tokio::test]
async fn wire_rejection_and_owner_denial_keep_exact_credential_custody() -> tests::TestResult {
    use acyclic_auth::credential::{
        CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
    };
    use wire::stream_service_server::StreamServiceServer;
    for (code, replace_before_reply) in [
        (Code::Unauthenticated, false),
        (Code::PermissionDenied, false),
        (Code::Unauthenticated, true),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let revision = CredentialRevision::from([1; 32]);
        let cache = CredentialCache::new(
            move |_| {
                futures::future::ready(CredentialSnapshot::new(
                    format!("synthetic-{}", count.fetch_add(1, Ordering::SeqCst)),
                    CredentialRevision::from([1; 32]),
                    100,
                ))
            },
            move || CredentialValidity {
                now_unix_seconds: 1,
                revision: Some(CredentialRevision::from([1; 32])),
            },
        );
        let old = cache.resolve().await?;
        let source = cache.clone();
        let retained = old.clone();
        let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
            tower::service_fn(move |_| {
                let source = source.clone();
                let retained = retained.clone();
                async move {
                    let (client, server) = tokio::io::duplex(64 * 1024);
                    tokio::spawn(async move {
                        let service = StreamServiceServer::with_interceptor(
                            Service::new(Arc::new(crate::MemoryStream::default())),
                            move |_: Request<()>| {
                                if replace_before_reply {
                                    source
                                        .reject(&retained)
                                        .map_err(|_| Status::internal("test source failed"))?;
                                    futures::executor::block_on(source.resolve())
                                        .map_err(|_| Status::internal("test source failed"))?;
                                }
                                Err(Status::new(code, "synthetic service refusal"))
                            },
                        );
                        tonic::transport::Server::builder()
                            .add_service(service)
                            .serve_with_incoming(stream::once(async {
                                Ok::<_, std::io::Error>(server)
                            }))
                            .await
                    });
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
                }
            }),
        );
        let client = crate::testing::Counted::new(Client::from_channel_with_credential(
            channel,
            cache.clone().into(),
        ));
        let source = client.credential_provider().expect("forwarded Auth source");
        let changed = source.watch().await?.expect("trusted source notification");
        let admitted = source.resolve().await?;
        let binding = admitted.binding().expect("trusted lease binding");
        assert_eq!(binding.revision, revision);
        assert_eq!(binding.expires_at_unix_seconds, 100);
        assert!(matches!(
            client.inspect_idempotency(IdempotencyKey::unique()).await,
            Err(StreamError::AccessDenied)
        ));
        if code == Code::PermissionDenied {
            assert!(changed.now_or_never().is_none());
            admitted.check().await?;
            cache.check(&old)?;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert!(changed.now_or_never().is_some());
            assert!(admitted.check().await.is_err());
            assert!(cache.check(&old).is_err());
            let current = cache.resolve().await?;
            cache.check(&current)?;
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn transport_identity_follows_without_any_authorization() -> tests::TestResult {
    use futures::StreamExt as _;
    use wire::stream_service_server::StreamServiceServer;
    let memory = Arc::new(crate::MemoryStream::default());
    let path = "trust/v1/test";
    crate::Stream::new(memory.clone(), path)?
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
                        admitted.fetch_add(1, Ordering::SeqCst);
                        if request.metadata().contains_key("authorization") {
                            seen.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(request)
                    };
                    let session = crate::session::Service::new(
                        served.clone(),
                        crate::session::testing::authority(),
                        "fixture".into(),
                        crate::session::Limits::default(),
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
    let client = Arc::new(Client::from_authenticated_channel(
        channel,
        CredentialProvider::transport_identity(),
        route,
    ));
    // The memory server admits sessions in-band; a platform server admits
    // trust paths by the channel's transport identity instead.
    client
        .session_client()
        .map_err(|error| format!("session: {error:?}"))?
        .register_credential(0, crate::session::testing::credential())
        .map_err(|error| format!("register: {error:?}"))?;
    let source: &dyn crate::trust_feed::Source = &client;
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
    assert!(requests.load(Ordering::SeqCst) > 0);
    assert_eq!(headers.load(Ordering::SeqCst), 0);
    // Negative control: a bearer client over the same server is observed.
    let bearer = Arc::new(client.with_credential("fixture")?);
    // It holds no in-band session credential, so the server may refuse it;
    // the header is observed either way.
    let _refused = crate::trust_feed::Source::tail(&bearer, path).await;
    assert!(headers.load(Ordering::SeqCst) > 0);
    // Transports that cannot omit authorization refuse rather than send none.
    assert!(matches!(
        CredentialProvider::transport_identity().resolve().await,
        Err(CredentialError::TransportIdentity)
    ));
    Ok(())
}
