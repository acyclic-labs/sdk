use super::*;

#[test]
fn cursor_entropy_failure_does_not_install_a_key_and_clones_share_initialization()
-> Result<(), Error> {
    let (provider, _) = MemoryObjects::with_default_bucket();
    let failed = provider.cursor_key_with(|key| {
        key.fill(1);
        Err(getrandom::Error::UNSUPPORTED)
    });
    assert_eq!(failed.map_err(|error| error.code), Err(Unavailable));
    let clone = provider.clone();
    let key = clone.cursor_key_with(|key| {
        key.fill(2);
        Ok(())
    })?;
    drop(clone);
    assert_eq!(key, [2; 32]);
    assert_eq!(
        provider.cursor_key_with(|_| Err(getrandom::Error::UNSUPPORTED))?,
        key
    );
    Ok(())
}

#[tokio::test]
async fn bootstrap_pagination_is_bound_to_provider_and_query() -> Result<(), Error> {
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    for key in ["a", "b"] {
        provider
            .put(
                wire::PutObjectHeader {
                    bucket: Some(bucket.clone()),
                    object_key: key.to_owned(),
                    ..Default::default()
                },
                OwnedBytes::from_static(b"body"),
            )
            .await?;
    }
    let mut query = wire::ListObjectsRequest {
        bucket: Some(bucket),
        page_size: 1,
        ..Default::default()
    };
    let first = provider.list(query.clone()).await?;
    assert!(first.is_truncated);
    assert_eq!(
        first.entries.first().map(|entry| entry.object_key.as_str()),
        Some("a")
    );
    query.continuation_token = first.continuation_token;
    let second = provider.clone().list(query.clone()).await?;
    assert_eq!(
        second
            .entries
            .first()
            .map(|entry| entry.object_key.as_str()),
        Some("b")
    );
    assert!(!second.is_truncated);
    let (other, _) = MemoryObjects::with_default_bucket();
    assert_eq!(
        other.list(query.clone()).await.map_err(|error| error.code),
        Err(InvalidArgument)
    );
    query.prefix = "a".to_owned();
    assert_eq!(
        provider.list(query).await.map_err(|error| error.code),
        Err(InvalidArgument)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
mod native;
