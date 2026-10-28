//! Measure blob publication and bounded `list(after)` pagination.

use acyclic_objects::{Blobs, MemoryObjects};
use acyclic_stream::OwnedBytes;
use std::collections::BTreeSet;
use std::time::Instant;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() > 2 {
        return Err("usage: bench-objects [blob-count] [page-size]".into());
    }
    let count = arguments
        .first()
        .map_or(Ok(2000), |value| value.parse::<u64>())?;
    let page_size = arguments
        .get(1)
        .map_or(Ok(1), |value| value.parse::<u32>())?;
    if count == 0 || page_size == 0 {
        return Err("blob count and page size must be positive".into());
    }
    let blobs = MemoryObjects::memory();
    let started = Instant::now();
    let mut expected = BTreeSet::new();
    for index in 0..count {
        let put = blobs
            .put(OwnedBytes::from(index.to_le_bytes().to_vec()), Vec::new())
            .await?;
        expected.insert(put.verified().ok_or("publication outcome unknown")?.digest);
    }
    let put_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    let (mut after, mut listed, mut pages) = (None, Vec::new(), 0u64);
    loop {
        let page = blobs.list(after, page_size).await?;
        pages += 1;
        listed.extend(page.digests);
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let list_ms = started.elapsed().as_secs_f64() * 1000.0;
    if listed.iter().copied().collect::<BTreeSet<_>>() != expected || listed.len() != expected.len()
    {
        return Err("pagination duplicated or skipped a blob".into());
    }
    println!(
        "blobs={count} page_size={page_size} pages={pages} put_ms={put_ms:.1} list_ms={list_ms:.1}"
    );
    Ok(())
}
