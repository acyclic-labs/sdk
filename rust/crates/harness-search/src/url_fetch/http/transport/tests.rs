use super::*;

fn fixture_error(error: std::io::Error) -> crate::Error {
    crate::Error::Storage(error.to_string())
}

#[tokio::test]
async fn validated_addresses_keep_connection_fallback() -> Result<()> {
    // Reserve a port without listening so the first candidate refuses the
    // connection and cannot be reused by the real source listener.
    let refused = tokio::net::TcpSocket::new_v4().map_err(fixture_error)?;
    refused
        .bind("127.0.0.1:0".parse().unwrap())
        .map_err(fixture_error)?;
    let source = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(fixture_error)?;
    let addresses = [
        refused.local_addr().map_err(fixture_error)?,
        source.local_addr().map_err(fixture_error)?,
    ];
    let stream = validated_connect(addresses.into_iter(), false, None).await?;
    assert_eq!(stream.peer_addr().map_err(fixture_error)?, addresses[1]);
    source.accept().await.map_err(fixture_error)?;
    Ok(())
}

#[test]
fn every_redirect_address_is_checked_before_connecting() -> Result<()> {
    let addresses = [
        "8.8.8.8:80".parse().unwrap(),
        "127.0.0.1:9".parse().unwrap(),
    ];
    assert!(matches!(
        validated_addresses(addresses.into_iter(), true, None),
        Err(crate::Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            reason: "fetch redirect targets a non-public address"
        })
    ));
    Ok(())
}
