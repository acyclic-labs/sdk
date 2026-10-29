use super::*;
use crate::{Medium, Store};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

#[tokio::test]
#[ignore = "requires a disposable S3 bucket configured with STORAGE_S3_* variables"]
async fn s3_ranges_and_shared_conformance() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let required = |name| std::env::var(name);
    let bucket = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(required("STORAGE_S3_BUCKET")?)
            .with_endpoint(required("STORAGE_S3_ENDPOINT")?)
            .with_region(required("STORAGE_S3_REGION")?)
            .with_access_key_id(required("STORAGE_S3_ACCESS_KEY")?)
            .with_secret_access_key(required("STORAGE_S3_SECRET_KEY")?)
            .with_allow_http(std::env::var("STORAGE_S3_ALLOW_HTTP").as_deref() == Ok("1"))
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?,
    );
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|error| format!("test namespace entropy: {error}"))?;
    let prefix = format!("range-conformance/{}", hex::encode(nonce));
    let medium = S3Medium::new(bucket.clone(), &prefix, Capabilities::REQUIRED)?;
    assert!(
        medium
            .create("range", OwnedBytes::from_static(b"abcdefgh"))
            .await?
    );
    let clipped = bucket
        .get_opts(
            &Path::from(format!("{prefix}/range")),
            GetOptions {
                range: Some(GetRange::Bounded(7..9)),
                ..GetOptions::default()
            },
        )
        .await?;
    assert_eq!(clipped.range, 7..8);
    assert_eq!(clipped.bytes().await?.as_ref(), b"h");
    for (range, wanted) in [(0..8, b"abcdefgh".as_slice()), (3..5, b"de"), (8..8, b"")] {
        assert_eq!(
            medium.read("range", Some(range)).await?.unwrap().as_ref(),
            wanted
        );
    }
    // S3 commonly clips a requested suffix at EOF. The physical seam requires
    // the complete selection, including the otherwise bodyless empty case.
    for range in [0..9, 7..9, 9..9, Range { start: 4, end: 3 }] {
        assert!(medium.read("range", Some(range)).await.is_err());
    }
    assert!(medium.read("absent", Some(0..1)).await?.is_none());
    assert!(medium.read("absent", Some(0..0)).await?.is_none());
    assert!(medium.remove("range").await?);
    crate::conformance::verify_medium(&medium, "ranges").await?;
    crate::tests::availability::verify(medium.clone()).await?;
    let store = Store::new(medium);
    crate::conformance::verify(
        &store.account(b"first")?,
        &store.account(b"second")?,
        "ranges",
    )
    .await?;
    Ok(())
}
