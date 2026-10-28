//! Runs the canonical Objects, Stream and filesystem composition suites.
#[path = "storage_conformance/batch.rs"]
mod batch;
#[path = "storage_conformance/filesystem.rs"]
mod filesystem;
#[path = "storage_conformance/host.rs"]
mod host;
#[path = "storage_conformance/s3.rs"]
mod s3;
use acyclic_objects::{
    ObjectsProvider,
    s3::{Capabilities, S3Objects},
    wire,
};
use acyclic_stream::postgres::PostgresStream;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use std::{sync::Arc, time::Instant};

fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("step=config: {name} missing; recovery: run bash scripts/backends/storage-conformance.sh"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider().install_default().map_err(|_| {
        "step=tls-provider: a provider was already installed; recovery: run storage-conformance in a fresh process"
    })?;
    let start = Instant::now();
    let database = required("STORAGE_POSTGRES_URL")?;
    let namespace = uuid::Uuid::new_v4().to_string();
    let streams = Arc::new(PostgresStream::connect(&database, &namespace).await?);
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(required("STORAGE_S3_BUCKET")?)
            .with_endpoint(required("STORAGE_S3_ENDPOINT")?)
            .with_region(required("STORAGE_S3_REGION")?)
            .with_access_key_id(required("STORAGE_S3_ACCESS_KEY")?)
            .with_secret_access_key(required("STORAGE_S3_SECRET_KEY")?)
            .with_allow_http(std::env::var("STORAGE_S3_ALLOW_HTTP").as_deref() == Ok("1"))
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?,
    );
    let prefix = Path::from(format!("conformance/{}", uuid::Uuid::new_v4()));
    let capabilities = Capabilities {
        conditional_put: true,
        read_after_write: true,
        multipart: true,
        range_reads: true,
    };
    let objects = Arc::new(S3Objects::open(store.clone(), prefix.clone(), capabilities).await?);
    let step = Instant::now();
    acyclic_conformance::stream(&acyclic_stream::conformance::Fixture::new(
        streams.as_ref().clone(),
    ))
    .await?;
    println!("Stream unchanged suite: {:?}", step.elapsed());
    let step = Instant::now();
    acyclic_conformance::objects(&acyclic_objects::v1::conformance::Fixture::new(
        objects.as_ref().clone(),
        "conformance",
    ))
    .await?;
    println!("Objects unchanged suite: {:?}", step.elapsed());
    s3::verify(store.clone(), prefix.clone(), capabilities).await?;
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "filesystem".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("created bucket missing identity")?;
    let backend = acyclic_fs::conformance::from_providers(
        streams.as_ref().clone(),
        objects.as_ref().clone(),
        bucket.clone(),
        acyclic_fs::EmbeddedCapabilities { durable: true },
    );
    let step = Instant::now();
    acyclic_fs::conformance::verify(&backend).await?;
    println!(
        "Filesystem shared public suite over Postgres + MinIO: {:?}",
        step.elapsed()
    );
    filesystem::verify(
        acyclic_fs::conformance::Backend::filesystem(&backend),
        async || {
            Ok(acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(Arc::new(
                    PostgresStream::connect(&database, &namespace).await?,
                )),
                acyclic_fs::LogicalObjectStore::new(
                    Arc::new(S3Objects::open(store.clone(), prefix, capabilities).await?),
                    bucket,
                ),
                acyclic_fs::EmbeddedCapabilities { durable: true },
            ))
        },
    )
    .await?;
    host::verify(&database, store, capabilities).await?;
    println!("All storage suites: {:?}", start.elapsed());
    Ok(())
}
