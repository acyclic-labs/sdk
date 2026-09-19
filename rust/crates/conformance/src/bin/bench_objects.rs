//! Measure the public Objects provider paths that grow with namespace size.

use std::time::Instant;

use acyclic_objects::{MemoryObjects, ObjectsProvider, PutRequest, ReadTarget, wire};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let (count, page_size, versioned) = parse_arguments(&arguments)?;
    run(count, page_size, versioned).await
}

fn parse_arguments(arguments: &[String]) -> Result<(usize, u32, bool), Box<dyn std::error::Error>> {
    if arguments.len() > 3 {
        return Err("usage: bench-objects [object-count] [page-size] [--versions]".into());
    }
    let count = arguments
        .first()
        .map_or(Ok(2000), |value| value.parse::<usize>())?;
    if !(1..=20_000).contains(&count) {
        return Err("object count must be between 1 and 20000".into());
    }
    let page_size = arguments
        .get(1)
        .map_or(Ok(1), |value| value.parse::<u32>())?;
    if !(1..=1000).contains(&page_size) {
        return Err("page size must be between 1 and 1000".into());
    }
    let versioned = match arguments.get(2).map(String::as_str) {
        None => false,
        Some("--versions") => true,
        Some(_) => return Err("third argument must be --versions".into()),
    };
    Ok((count, page_size, versioned))
}

async fn run(
    count: usize,
    page_size: u32,
    versioned: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    let started = Instant::now();
    let mut expected = Vec::with_capacity(count);
    for index in 0..count {
        let key = if versioned {
            "group/one".to_owned()
        } else {
            format!("group/{index:08}")
        };
        let version = provider
            .put(PutRequest {
                bucket: bucket.clone(),
                object_key: key.clone(),
                body: Bytes::from_static(b"x"),
                metadata: wire::ObjectMetadata::default(),
                condition: None,
                idempotency_key: None,
            })
            .await?;
        expected.push((key, version.version_id));
    }
    if versioned {
        expected.reverse();
    }
    let put_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    provider.snapshot(bucket.clone(), None).await?;
    let snapshot_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    let first = provider
        .list(
            ReadTarget::Bucket(bucket.clone()),
            "group/".to_owned(),
            None,
            versioned,
            page_size,
            None,
        )
        .await?;
    let first_page_ms = started.elapsed().as_secs_f64() * 1000.0;
    if first.entries.len() != count.min(page_size as usize)
        || first.continuation.is_none() && count > page_size as usize
    {
        return Err("first page mismatch".into());
    }
    let remaining_pages_ms = verify_pages(
        &provider, &bucket, &expected, first, count, page_size, versioned,
    )
    .await?;
    println!(
        "{}",
        serde_json::json!({
            "schema": 1,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "objects": count,
            "mode": if versioned { "versions" } else { "keys" },
            "page_size": page_size,
            "put_ms": put_ms,
            "snapshot_ms": snapshot_ms,
            "first_page_ms": first_page_ms,
            "remaining_pages_ms": remaining_pages_ms,
        })
    );
    Ok(())
}

async fn verify_pages(
    provider: &MemoryObjects,
    bucket: &acyclic_objects::wire::BucketRef,
    expected: &[(String, String)],
    first: acyclic_objects::ProviderListPage,
    count: usize,
    page_size: u32,
    versioned: bool,
) -> Result<f64, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut pages = 1;
    let mut seen = 0;
    for entry in &first.entries {
        if expected.get(seen).is_none_or(|(key, id)| {
            entry.object_key != *key
                || entry
                    .version
                    .as_ref()
                    .is_none_or(|version| version.version_id != *id)
        }) {
            return Err("first page order mismatch".into());
        }
        seen += 1;
    }
    let mut continuation = first.continuation;
    while let Some(token) = continuation {
        let page = provider
            .list(
                ReadTarget::Bucket(bucket.clone()),
                "group/".to_owned(),
                None,
                versioned,
                page_size,
                Some(token),
            )
            .await?;
        pages += 1;
        for entry in &page.entries {
            if expected.get(seen).is_none_or(|(key, id)| {
                entry.object_key != *key
                    || entry
                        .version
                        .as_ref()
                        .is_none_or(|version| version.version_id != *id)
            }) {
                return Err("page order mismatch".into());
            }
            seen += 1;
        }
        continuation = page.continuation;
    }
    if seen != count || pages != count.div_ceil(page_size as usize) {
        return Err("pagination mismatch".into());
    }
    Ok(started.elapsed().as_secs_f64() * 1000.0)
}
