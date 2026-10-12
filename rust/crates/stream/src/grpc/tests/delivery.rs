use super::*;

#[tokio::test]
async fn receipts_do_not_gate_delivery_and_drop_cancels_at_issuing_endpoint()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (fail, follow) in [false, true]
        .into_iter()
        .flat_map(|fail| [false, true].map(|follow| (fail, follow)))
    {
        let other = AckFixture::new(Some(32), fail);
        let issuer = AckFixture::new(Some(32), fail);
        let (first, first_server) =
            ack_loopback(Arc::clone(&other), Some("unavailable_read")).await?;
        let (second, second_server) = ack_loopback(Arc::clone(&issuer), None).await?;
        let client = Client::from_channels(
            Arc::from([first, second]),
            Client::authorization("fixture")?,
        );
        let mut records = if follow {
            client.follow(StreamPath::new("events")?, 0).await?
        } else {
            client
                .read(ReadRequest {
                    path: StreamPath::new("events")?,
                    from: 0,
                    limit: 10,
                })
                .await?
        };
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(200), records.next()).await?;
        assert_eq!(received.transpose()?.map(|record| record.sequence), Some(0));
        wait_for_ack_count(&issuer, &issuer.entered).await?;
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), records.next()).await?;
        assert_eq!(received.transpose()?.map(|record| record.sequence), Some(1));
        assert_eq!(other.entered.load(Ordering::SeqCst), 0);
        drop(records);
        if !fail {
            wait_for_ack_count(&issuer, &issuer.cancelled).await?;
        }
        first_server.abort();
        second_server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn receipts_reject_malformed_frames_and_respect_opt_out_and_absent_tokens()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (length, disabled, fault) in [
        (Some(32), true, None),
        (None, false, None),
        (Some(31), false, None),
        (Some(32), false, Some("bad_cursor")),
    ] {
        let fixture = AckFixture::new(length, true);
        let (channel, server) = ack_loopback(Arc::clone(&fixture), fault).await?;
        let mut client =
            Client::from_channels(Arc::from([channel]), Client::authorization("fixture")?);
        if disabled {
            client = client.without_delivery_acknowledgements();
        }
        let mut records = client
            .read(ReadRequest {
                path: StreamPath::new("events")?,
                from: 0,
                limit: 10,
            })
            .await?;
        let first = records.next().await.transpose();
        if length == Some(31) || fault.is_some() {
            assert_eq!(first.unwrap_err(), StreamError::Unavailable);
        } else {
            assert_eq!(first?.map(|record| record.sequence), Some(0));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert_eq!(fixture.entered.load(Ordering::SeqCst), 0);
        assert_eq!(
            client.acknowledge_delivery(0, &[7; 31]).await,
            Err(StreamError::InvalidArgument)
        );
        if disabled {
            assert_eq!(
                client.acknowledge_delivery(0, &[7; 32]).await,
                Err(StreamError::Unavailable)
            );
            assert_eq!(fixture.entered.load(Ordering::SeqCst), 1);
        }
        drop(records);
        server.abort();
    }
    let client = Client::from_channels(
        Arc::from([provider_channel(Service::new(Arc::new(
            MemoryStream::default(),
        )))]),
        Client::authorization("fixture")?,
    );
    assert_eq!(
        client.acknowledge_delivery(0, &[7; 32]).await,
        Err(StreamError::Unsupported)
    );
    #[cfg(feature = "local")]
    {
        let directory = tempfile::tempdir()?;
        let provider =
            crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default()).await?;
        let service = Service::new(Arc::new(provider));
        let error = service
            .acknowledge_delivery(Request::new(wire::AcknowledgeDeliveryRequest {
                delivery_token: vec![7; 32].into(),
            }))
            .await
            .err()
            .ok_or("local adapter acknowledged a hosted token")?;
        assert_eq!(error.code(), Code::Unimplemented);
    }
    Ok(())
}
