use super::*;

#[tokio::test]
async fn validated_addresses_keep_connection_fallback()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    // Reserve a port without listening so the first candidate refuses the
    // connection and cannot be reused by the real source listener.
    let refused = tokio::net::TcpSocket::new_v4()?;
    refused.bind("127.0.0.1:0".parse()?)?;
    let source = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let refused_address = refused.local_addr()?;
    let source_address = source.local_addr()?;
    let addresses = [refused_address, source_address];
    let stream = validated_connect(addresses.into_iter(), false, None).await?;
    assert_eq!(stream.peer_addr()?, source_address);
    source.accept().await?;
    Ok(())
}

#[test]
fn every_redirect_address_is_checked_before_connecting()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let addresses = ["8.8.8.8:80".parse()?, "127.0.0.1:9".parse()?];
    assert!(matches!(
        validated_addresses(addresses.into_iter(), true, None),
        Err(crate::Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            reason: "fetch redirect targets a non-public address"
        })
    ));
    Ok(())
}
