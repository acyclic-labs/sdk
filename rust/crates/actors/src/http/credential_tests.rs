use super::*;
use acyclic_auth::credential::{
    CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
};
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn credential_custody_survives_held_successful_http_bodies()
-> Result<(), Box<dyn std::error::Error>> {
    for change in 0..6 {
        let clock = Arc::new(AtomicU64::new(1));
        let revision = Arc::new(AtomicU64::new(1));
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_clock = clock.clone();
        let observed_revision = revision.clone();
        let refreshed = calls.clone();
        let cache = CredentialCache::new(
            move |_| {
                refreshed.fetch_add(1, Ordering::SeqCst);
                std::future::ready(CredentialSnapshot::new(
                    "synthetic-http".into(),
                    CredentialRevision::from([1; 32]),
                    100,
                ))
            },
            move || CredentialValidity {
                now_unix_seconds: observed_clock.load(Ordering::SeqCst),
                revision: (observed_revision.load(Ordering::SeqCst) != 0).then(|| {
                    CredentialRevision::from([observed_revision.load(Ordering::SeqCst) as u8; 32])
                }),
            },
        );
        let provider: CredentialProvider = cache.clone().into();
        provider.resolve().await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let cancellation = tokio_util::sync::CancellationToken::new();
        let client = Client::with_credential_provider(
            &format!("http://{}", listener.local_addr()?),
            provider.clone(),
            1024,
        )?
        .with_context(acyclic_auth::operation::Context::new(
            u64::MAX,
            std::future::pending(),
            cancellation.clone().cancelled_owned(),
        ));
        let body = encode(
            &client.descriptors,
            "acyclic.actors.v1.InspectActorResponse",
            &wire::InspectActorResponse::default(),
        )?;
        let (held, ready) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = async {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                assert_ne!(socket.read_buf(&mut request).await?, 0);
            }
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await?;
            held.send(()).unwrap();
            released.await?;
            // Cancellation may close the client before the response body is sent.
            let sent = socket.write_all(&body).await;
            if change != 5 {
                sent?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let change_source = async {
            ready.await.unwrap();
            match change {
                1 => revision.store(2, Ordering::SeqCst),
                2 => revision.store(0, Ordering::SeqCst),
                3 => clock.store(100, Ordering::SeqCst),
                4 => cache.invalidate().unwrap(),
                5 => cancellation.cancel(),
                _ => {}
            }
            release.send(()).unwrap();
        };
        let request = wire::InspectActorRequest {
            actor_id: "actor-a".into(),
            ..Default::default()
        };
        let (result, server_result, ()) = tokio::join!(
            client.call::<_, wire::InspectActorResponse>(
                "actors/inspect",
                "acyclic.actors.v1.InspectActorRequest",
                "acyclic.actors.v1.InspectActorResponse",
                &request
            ),
            server,
            change_source
        );
        server_result?;
        match change {
            0 => {
                result?;
            }
            5 => assert!(matches!(
                result,
                Err(Error::Stopped(acyclic_auth::operation::Stopped::Cancelled))
            )),
            _ => assert!(matches!(result, Err(Error::Credential(_)))),
        }
        // A local freshness refusal or cancellation never rejects the snapshot.
        revision.store(1, Ordering::SeqCst);
        clock.store(1, Ordering::SeqCst);
        provider.resolve().await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if change == 4 { 2 } else { 1 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn deployment_kind_mismatch_is_rejected_before_credentials_or_io()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let client = Client::with_credential_provider(
        "https://fixture.invalid",
        CredentialProvider::new(move || {
            seen.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Err(acyclic_auth::credential::CredentialError::Invalid))
        }),
        1024,
    )?;
    let worker = wire::DeploymentRequest {
        entry_source: None,
        name: "worker".into(),
        code_sha256: vec![1; 32],
        idempotency_key: "deployment".into(),
        worker_limits: Some(wire::WorkerLimits {
            cpu_millis: 1,
            memory_bytes: 1,
            timeout_millis: 1,
        }),
        ..Default::default()
    };
    // Negative control: generic envelope admission accepts this valid Worker.
    crate::domain::DeploymentRequest::try_from(worker.clone())?;
    assert!(matches!(
        client.deploy_namespace(&worker).await,
        Err(Error::InvalidArgument)
    ));
    let namespace = wire::DeploymentRequest {
        entry_source: None,
        secrets: Vec::new(),
        fetch: None,
        worker_limits: None,
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        }),
        ..worker.clone()
    };
    crate::domain::DeploymentRequest::try_from(namespace.clone())?;
    assert!(matches!(
        client.deploy_worker(&namespace).await,
        Err(Error::InvalidArgument)
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    // Correct operation alternatives proceed as far as credential admission.
    assert!(matches!(
        client.deploy_worker(&worker).await,
        Err(Error::Credential(_))
    ));
    assert!(matches!(
        client.deploy_namespace(&namespace).await,
        Err(Error::Credential(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn credential_callbacks_preserve_current_headers_and_fail_before_transport() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,64}", 2..8),
            |values| {
                runtime.block_on(async {
                    let expected = values.clone();
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let client = Client::with_credential_provider(
                        "https://fixture.invalid",
                        CredentialProvider::new(move || {
                            std::future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                            )
                        }),
                        1024,
                    )
                    .unwrap();
                    for value in &expected {
                        let (header, _) = client.authorization().await.unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    let result = client
                        .inspect_actor(&wire::InspectActorRequest {
                            namespace: None,
                            account_id: Vec::new(),
                            actor_id: "actor-a".into(),
                        })
                        .await;
                    prop_assert!(matches!(result, Err(Error::Credential(_))));
                    Ok(())
                })
            },
        )
        .unwrap();
}

#[tokio::test]
async fn expired_context_refuses_before_the_actual_credential_callback()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let client = Client::with_credential_provider(
        "https://fixture.invalid",
        CredentialProvider::new(move || {
            seen.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Err(acyclic_auth::credential::CredentialError::Invalid))
        }),
        1024,
    )?
    .with_context(acyclic_auth::operation::Context::new(
        1,
        std::future::ready(()),
        std::future::pending(),
    ));
    let request = wire::InspectActorRequest {
        actor_id: "actor-a".into(),
        ..Default::default()
    };
    for client in [client.clone(), client] {
        assert!(matches!(
            client.inspect_actor(&request).await,
            Err(Error::Stopped(acyclic_auth::operation::Stopped::Deadline))
        ));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn credential_cancellation_with_a_ready_token_stops_before_sending()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let cancel = cancellation.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let client = Client::with_credential_provider(
        &format!("http://{}", listener.local_addr()?),
        CredentialProvider::new(move || {
            seen.fetch_add(1, Ordering::Relaxed);
            cancel.cancel();
            std::future::ready(Ok("current-token".to_owned()))
        }),
        1024,
    )?
    .with_context(acyclic_auth::operation::Context::new(
        u64::MAX,
        std::future::pending(),
        cancellation.cancelled_owned(),
    ));
    let request = wire::InspectActorRequest {
        actor_id: "actor-a".into(),
        ..Default::default()
    };
    for client in [client.clone(), client] {
        let operation = client.inspect_actor(&request);
        futures::pin_mut!(operation);
        assert!(matches!(
            futures::poll!(operation),
            std::task::Poll::Ready(Err(Error::Stopped(
                acyclic_auth::operation::Stopped::Cancelled
            )))
        ));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let accepted =
        std::future::poll_fn(|cx| std::task::Poll::Ready(listener.poll_accept(cx))).await;
    assert!(accepted.is_pending());
    Ok(())
}
