use super::*;

#[tokio::test]
async fn admission_binds_the_first_interest_and_refuses_an_unbound_golden_frame() -> TestResult {
    let golden = acyclic_proto_build::golden::frame("session.admission")?;
    let decoded = wire::SessionFrame::decode(golden.as_slice())?;
    let frame = frame(
        0,
        0,
        Body::Hello(wire::SessionHello {
            version: crate::SESSION_PROTOCOL_VERSION,
            client_id: Bytes::from_static(b"c"),
            interest: Some(wire::SessionInterest {
                revision: 1,
                ..Default::default()
            }),
            ..Default::default()
        }),
    );
    assert_eq!(frame.encode_to_vec(), golden);
    assert_eq!(decoded, frame);
    // The retained framing vector predates identity binding. Its bytes remain
    // decodable, but it cannot admit any work on the current service.
    let mut refused = serve(
        Arc::new(MemoryStream::default()),
        testing::authority(),
        "node".into(),
        Limits::default(),
        stream::iter([Ok(decoded)]).boxed(),
    )?;
    assert!(matches!(
        refused.next().await,
        Some(Err(StreamError::Unsupported))
    ));
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", Bytes::from_static(b"first")).await?;
    let mut admission = hello(1_000_000, 10);
    let Some(Body::Hello(hello)) = &mut admission.body else {
        return Err("missing admission".into());
    };
    hello.interest = Some(wire::SessionInterest {
        revision: 1,
        upsert: vec![subscribe(1, "events", 0, false)],
        remove: vec![],
    });
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(admission))?;
    let mut output = serve(
        provider,
        testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    assert!(matches!(
        &output
            .next()
            .await
            .transpose()?
            .ok_or("missing welcome")?
            .wire()
            .body,
        Some(Body::Welcome(_))
    ));
    let mut opened = false;
    let mut applied = false;
    for _ in 0..2 {
        match &output
            .next()
            .await
            .transpose()?
            .ok_or("admission ended")?
            .wire()
            .body
        {
            Some(Body::Opened(_)) => opened = true,
            Some(Body::InterestApplied(_)) => applied = true,
            other => return Err(format!("unexpected admission response: {other:?}").into()),
        }
    }
    assert!(opened && applied);
    // No second request exists on the input path; the first request produced
    // both acknowledgements. Dropping its owner ends the same live session.
    drop(sender);
    while output.next().await.transpose()?.is_some() {}
    Ok(())
}

#[tokio::test]
async fn admission_refuses_versions_before_credentials_and_interests() -> TestResult {
    for version in [0, 1, 2, VERSION + 1, u32::MAX] {
        let mut input = hello(0, 0);
        let Some(Body::Hello(hello)) = &mut input.body else {
            return Err("missing admission".into());
        };
        hello.version = version;
        hello.credentials = vec![Bytes::from_static(b"malformed credential")];
        hello.interest = Some(wire::SessionInterest::default());
        let mut output = serve(
            Arc::new(MemoryStream::default()),
            testing::authority(),
            "node".into(),
            Limits::default(),
            stream::iter([Ok(input)]).boxed(),
        )?;
        assert!(matches!(
            output.next().await,
            Some(Err(StreamError::Unsupported))
        ));
        assert!(output.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn admission_binds_identity_and_every_requirement_before_credentials() -> TestResult {
    for mutation in 0..10 {
        let mut input = hello(0, 0);
        let Some(Body::Hello(hello)) = &mut input.body else {
            return Err("missing admission".into());
        };
        let mut protocol = protocol_handshake();
        match mutation {
            0 => hello.protocol = None,
            1 => protocol.protocol = None,
            2 => protocol
                .protocol
                .as_mut()
                .ok_or("missing identity")?
                .version
                .clear(),
            3 => {
                protocol
                    .protocol
                    .as_mut()
                    .ok_or("missing identity")?
                    .version = "other".into()
            }
            4 => protocol
                .protocol
                .as_mut()
                .ok_or("missing identity")?
                .descriptor_digest
                .clear(),
            5 => {
                protocol
                    .protocol
                    .as_mut()
                    .ok_or("missing identity")?
                    .descriptor_digest = "other".into()
            }
            _ => {
                let mut capability = wire::protocol::v1::Capability {
                    name: "stream.session".into(),
                    version: VERSION.to_string(),
                };
                match mutation {
                    6 => capability.name.clear(),
                    7 => capability.version.clear(),
                    8 => capability.version = "other".into(),
                    _ => capability.name = "unsupported.feature".into(),
                }
                protocol
                    .required
                    .as_mut()
                    .ok_or("missing requirements")?
                    .capabilities
                    .push(capability);
            }
        }
        if mutation != 0 {
            hello.protocol = Some(protocol);
        }
        hello.credentials = vec![Bytes::from_static(b"malformed credential")];
        hello.interest = Some(wire::SessionInterest {
            revision: 1,
            upsert: vec![subscribe(1, "events", 0, false)],
            remove: vec![],
        });
        let mut output = serve(
            Arc::new(MemoryStream::default()),
            testing::authority(),
            "node".into(),
            Limits::default(),
            stream::iter([Ok(input)]).boxed(),
        )?;
        assert!(
            matches!(output.next().await, Some(Err(StreamError::Unsupported))),
            "mutation {mutation}"
        );
        assert!(output.next().await.is_none());
    }
    Ok(())
}
