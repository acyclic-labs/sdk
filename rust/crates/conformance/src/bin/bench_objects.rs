//! Measure current-key publication and bounded logical Objects v2 pagination.

use acyclic_objects::v2::{MemoryObjects, ObjectsProvider, wire};
use bytes::Bytes;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() > 2 {
        return Err("usage: bench-objects [object-count] [page-size]".into());
    }
    let count = arguments
        .first()
        .map_or(Ok(2000), |value| value.parse::<usize>())?;
    let page_size = arguments
        .get(1)
        .map_or(Ok(1), |value| value.parse::<u32>())?;
    if !(1..=20_000).contains(&count) || !(1..=1000).contains(&page_size) {
        return Err("object count must be 1..20000 and page size 1..1000".into());
    }
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    let started = Instant::now();
    let mut expected = Vec::with_capacity(count);
    for index in 0..count {
        let key = format!("group/{index:08}");
        let info = provider
            .put(
                wire::PutObjectHeader {
                    bucket: Some(bucket.clone()),
                    object_key: key.clone(),
                    ..Default::default()
                },
                Bytes::from_static(b"x"),
            )
            .await?;
        expected.push((key, info));
    }
    let put_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    let query = wire::ListObjectsRequest {
        bucket: Some(bucket),
        prefix: "group/".into(),
        page_size,
        ..Default::default()
    };
    let first = provider.list(query.clone()).await?;
    let first_page_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    let mut page = first;
    let mut seen = 0;
    let mut pages = 0;
    loop {
        pages += 1;
        if page.entries.len() != (count - seen).min(page_size as usize) {
            return Err("page length mismatch".into());
        }
        for entry in page.entries {
            if expected.get(seen).is_none_or(|(key, info)| {
                entry.object_key != *key || entry.object.as_ref() != Some(info)
            }) {
                return Err("current-key page order or metadata mismatch".into());
            }
            seen += 1;
        }
        if page.continuation_token.is_empty() {
            break;
        }
        page = provider
            .list(wire::ListObjectsRequest {
                continuation_token: page.continuation_token,
                ..query.clone()
            })
            .await?;
    }
    if seen != count || pages != count.div_ceil(page_size as usize) {
        return Err("pagination mismatch".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "schema": 2, "contract": "acyclic.objects.v2", "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH, "objects": count, "mode": "keys",
            "page_size": page_size, "put_ms": put_ms, "first_page_ms": first_page_ms,
            "remaining_pages_ms": started.elapsed().as_secs_f64() * 1000.0,
        })
    );
    Ok(())
}
