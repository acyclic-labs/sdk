use super::*;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::testing::{Counted, RequestClass};

#[tokio::test]
async fn native_tail_preserves_the_complete_real_observation()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = Arc::new(Counted::new(acyclic_stream::MemoryStream::default()));
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: StreamPath::new("observed")?,
            records: vec![vec![1].into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?
    else {
        panic!("initial append conflicted");
    };
    let revision = wire_codec::revision_to_wire(&receipt.revision);
    let service = wire::stream_session_service_server::StreamSessionServiceServer::new(
        acyclic_stream::session::Service::new(
            Arc::clone(&provider),
            acyclic_stream::session::testing::authority(),
            "native".into(),
            acyclic_stream::session::Limits::default(),
        ),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = futures::stream::unfold(listener, |listener| async move {
        listener
            .accept()
            .await
            .ok()
            .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
    });
    let result = async {
        let client = NativeStreamClient::from_client(
            grpc::Client::connect_loopback(endpoint, "exact-token").await?,
        );
        client.register_credential(
            0,
            Buffer::from(acyclic_stream::session::testing::credential().to_vec()),
        )?;
        let before = provider.requests();
        for (accepted, through, durable) in [
            (false, None, None),
            (true, None, Some(0)),
            (true, Some(1), Some(1)),
        ] {
            let response = client
                .tail_result(
                    Buffer::from(
                        wire::TailRequest {
                            path: "observed".into(),
                            accepted,
                            durable_through: through,
                            at_least: vec![revision.clone()],
                        }
                        .encode_to_vec(),
                    ),
                    None,
                )
                .await?;
            assert!(response.error.is_none(), "valid tail observation refused");
            let bytes = response.value.expect("tail omitted response bytes");
            let response = wire::TailResponse::decode(bytes.as_ref())?;
            assert_eq!(response.tail, 1);
            assert_eq!(response.durable, durable);
            assert_eq!(response.applied, Some(revision.clone()));
        }
        let observed = provider.requests().since(before);
        assert_eq!(observed.class(RequestClass::Head), 3);
        assert_eq!(observed.control(), 3);
        assert_eq!(observed.data(), 0);

        let response = client
            .tail_result(
                Buffer::from(
                    wire::TailRequest {
                        path: "observed".into(),
                        accepted: true,
                        at_least: vec![wire::Revision::default()],
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
                None,
            )
            .await?;
        assert!(response.value.is_none());
        assert_eq!(
            response.error.expect("malformed cut accepted").code,
            "invalid_argument"
        );

        let cancellation = NativeStreamCancellation::new();
        cancellation.cancel();
        let response = client
            .tail_result(
                Buffer::from(
                    wire::TailRequest {
                        path: "observed".into(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
                Some(&cancellation),
            )
            .await?;
        assert!(response.value.is_none());
        assert_eq!(
            response.error.expect("cancelled tail accepted").code,
            "cancelled"
        );
        assert_eq!(provider.requests().since(before), observed);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result
}
