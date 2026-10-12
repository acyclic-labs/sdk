use super::*;

#[tokio::test]
async fn grpc_transport_passes_the_public_suite()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = provider_channel(Service::new(Arc::new(MemoryStream::default())));
    let transport = Client::from_channels(
        Arc::from([channel.clone()]),
        Client::authorization("fixture")?,
    );
    crate::conformance::verify(&transport).await?;
    let mut obsolete = tonic::client::Grpc::new(channel);
    obsolete.ready().await?;
    let result: Result<Response<wire::TailResponse>, Status> = obsolete
        .unary(
            Request::new(wire::TailRequest {
                path: "events".into(),
            }),
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/acyclic.stream.v2.StreamService/Tail",
            ),
            tonic_prost::ProstCodec::default(),
        )
        .await;
    assert_eq!(
        result.err().map(|error| error.code()),
        Some(tonic::Code::Unimplemented)
    );
    Ok(())
}

#[tokio::test]
async fn client_reassembles_mixed_compressed_and_plain_read_frames()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = MixedFrames(Service::new(Arc::new(MemoryStream::default())), None, None);
    let channel = provider_channel(service);
    let transport = Client::from_channels(Arc::from([channel]), Client::authorization("fixture")?);
    let path = StreamPath::new("accounts/mixed")?;
    let values = (0..20_u8)
        .map(|value| OwnedBytes::from(vec![value; 64]))
        .collect::<Vec<_>>();
    transport
        .append(AppendRequest {
            path: path.clone(),
            records: values.clone(),
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let records = transport
        .read(ReadRequest {
            path: path.clone(),
            from: 2,
            limit: 17,
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (2..19).collect::<Vec<_>>()
    );
    assert_eq!(
        records
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        values.get(2..19).unwrap_or_default().to_vec()
    );
    crate::conformance::verify(&transport).await?;
    Ok(())
}

#[tokio::test]
async fn service_rejects_oversized_wire_commands_before_provider_dispatch() {
    let service = Service::new(Arc::new(MemoryStream::default()));
    let request = wire::AppendRequest {
        path: "x".repeat(crate::MAX_PATH_BYTES),
        records: vec![Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]); 16],
        if_tail: None,
        idempotency_key: None,
    };
    assert!(request.encoded_len() > crate::MAX_COMMAND_BYTES);
    let error = service.append(Request::new(request)).await.err();
    assert_eq!(
        error.as_ref().map(Status::code),
        Some(Code::InvalidArgument)
    );
    assert_eq!(error.as_ref().map(Status::message), Some("limit_exceeded"));
}

#[test]
fn private_ca_bundle_is_exactly_bounded() {
    assert!(matches!(
        bounded_ca_certificate(&[]),
        Err(ConnectError::InvalidCaCertificate)
    ));
    assert!(matches!(
        bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES]),
        Ok(certificate) if certificate.len() == MAX_CA_CERTIFICATE_BYTES
    ));
    assert!(matches!(
        bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]),
        Err(ConnectError::InvalidCaCertificate)
    ));
}

#[tokio::test]
async fn endpoint_pool_is_explicit_and_bounded()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shared = Client::connect(
        ["https://data-a.invalid", "https://data-b.invalid"],
        "fixture",
        None,
    )
    .await?;
    assert_eq!(shared.channels.len(), 2);

    assert!(matches!(
        Client::connect(std::iter::empty::<&str>(), "fixture", None).await,
        Err(ConnectError::NoEndpoints)
    ));
    assert!(matches!(
        Client::connect(std::iter::repeat("https://data.invalid"), "fixture", None).await,
        Err(ConnectError::EndpointLimit)
    ));
    let oversized = format!("https://{}.invalid", "x".repeat(MAX_ENDPOINT_URI_BYTES));
    assert!(matches!(
        Client::connect([oversized], "fixture", None).await,
        Err(ConnectError::EndpointLimit)
    ));
    let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
    for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
        assert!(matches!(
            Client::connect(["https://data.invalid"], token, None).await,
            Err(ConnectError::InvalidCredential)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn dropping_eager_connect_releases_a_stalled_tls_socket()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let certificate = generate_simple_self_signed(["localhost".to_owned()])?;
    let certificate_pem = certificate.cert.pem();
    let (accepted_sender, accepted_receiver) = tokio::sync::oneshot::channel();
    let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let _ = accepted_sender.send(());
        let mut buffer = [0_u8; 1024];
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = closed_sender.send(());
        Ok::<(), std::io::Error>(())
    });
    let pending = tokio::spawn(Client::connect_eager_with_ca_certificate(
        endpoint,
        "fixture",
        certificate_pem,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), accepted_receiver).await??;
    pending.abort();
    let _ = pending.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), closed_receiver).await??;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn malformed_bearer_is_rejected_before_eager_network_io()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let Err(error) = Client::connect_eager(endpoint, "\r\n").await else {
        return Err("malformed bearer unexpectedly connected".into());
    };
    assert!(matches!(error, ConnectError::InvalidCredential));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept(),)
            .await
            .is_err()
    );
    Ok(())
}

#[test]
fn inspection_rejects_an_observation_for_another_retry_identity() -> Result<(), StreamError> {
    let requested = IdempotencyKey::new(Bytes::from_static(b"requested"))?;
    let forged = wire::IdempotencyObservation {
        idempotency_key: Bytes::from_static(b"other"),
        request_digest: Bytes::from_static(&[7; 32]),
        outcome: Some(wire::idempotency_observation::Outcome::Append(
            append_outcome_to_wire(AppendOutcome::TailConflict { actual_tail: 3 }),
        )),
    };
    assert_eq!(
        bind_observation(&requested, Some(forged)),
        Err(StreamError::Unavailable)
    );
    Ok(())
}
