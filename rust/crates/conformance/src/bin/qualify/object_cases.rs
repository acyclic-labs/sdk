use super::*;

fn open_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// A seeded put/delete history whose every step matches a model, then survives reopen.
pub(super) async fn objects_history(root: &Path, seed: u64) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    let mut expected = std::collections::BTreeMap::<Digest, Bytes>::new();
    let mut rng = seed | 1;
    for step in 0..48_u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let body = Bytes::copy_from_slice(&(rng % 8).to_le_bytes());
        let digest = Digest::of(&body);
        if step % 5 == 2 {
            let result = provider.delete(digest).await.map_err(open_error)?;
            let replay = provider.delete(digest).await.map_err(open_error)?;
            if replay != acyclic_objects::Delete::Absent
                || (result == acyclic_objects::Delete::Deleted) != expected.contains_key(&digest)
            {
                return Err(format!("step {step}: delete retry changed its outcome"));
            }
            expected.remove(&digest);
        } else {
            let object = provider
                .put(body.clone().into(), Vec::new())
                .await
                .map_err(open_error)?;
            let replay = provider
                .put(body.clone().into(), Vec::new())
                .await
                .map_err(open_error)?;
            if object.verified() != replay.verified()
                || !matches!(replay, acyclic_objects::Put::AlreadyExists { .. })
                || object.verified().map(|verified| verified.length) != Some(body.len() as u64)
            {
                return Err(format!("step {step}: put retry or size differs"));
            }
            expected.insert(digest, body);
        }
        verify_objects_current(&provider, &expected, digest, step).await?;
    }
    drop(provider);
    let reopened = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    for (digest, body) in expected {
        let observed = reopened
            .get(digest, ByteRange::FULL, 8)
            .await
            .map_err(open_error)?;
        if observed.body().as_ref() != body.as_ref() {
            return Err(format!("reopened blob {digest} differs from model"));
        }
    }
    Ok(())
}

const CHUNK_BYTES: usize = 1_024 * 1_024;
const CHUNKS: usize = 48;

fn many_chunks() -> Result<Vec<u8>, String> {
    let mut body = vec![0_u8; CHUNK_BYTES * CHUNKS];
    for (index, chunk) in body.chunks_mut(CHUNK_BYTES).enumerate() {
        chunk.fill(u8::try_from(index).map_err(|_| "chunk index exceeds byte range")?);
    }
    Ok(body)
}

pub(super) async fn objects_many_chunks_setup(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    provider
        .put(acyclic_stream::OwnedBytes::from(many_chunks()?), Vec::new())
        .await
        .map_err(open_error)?;
    Ok(())
}

pub(super) async fn objects_sparse_first_put(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    let put = provider
        .put(
            acyclic_stream::OwnedBytes::from_static(b"sparse body"),
            Vec::new(),
        )
        .await
        .map_err(open_error)?;
    let digest = put
        .verified()
        .ok_or("sparse put outcome is unknown")?
        .digest;
    let root_id = acyclic_objects::RootId::new(*b"sparse").map_err(open_error)?;
    provider
        .retain(&root_id, vec![digest])
        .await
        .map_err(open_error)?;
    Ok(())
}

pub(super) async fn objects_sparse_reopen_gc(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    // Two passes: the first also retires the reopened store's admission epoch.
    provider.collect().await.map_err(open_error)?;
    provider.collect().await.map_err(open_error)?;
    let object = provider
        .get(Digest::of(b"sparse body"), ByteRange::FULL, 11)
        .await
        .map_err(open_error)?;
    if object.body().as_ref() != b"sparse body" {
        return Err("retained body changed after reopen and collection".into());
    }
    Ok(())
}

pub(super) async fn objects_many_chunks_startup(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default(), None)
        .await
        .map_err(open_error)?;
    let digest = Digest::of(&many_chunks()?);
    let head = provider.head(digest).await.map_err(open_error)?;
    if head.length != (CHUNK_BYTES * CHUNKS) as u64 {
        return Err("many-chunk blob length changed across reopen".into());
    }
    Ok(())
}

pub(super) async fn verify_objects_current(
    provider: &impl Blobs,
    expected: &std::collections::BTreeMap<Digest, Bytes>,
    selected: Digest,
    step: u64,
) -> Result<(), String> {
    let mut observed = Vec::new();
    let mut after = None;
    for page_number in 0..4 {
        let page = provider.list(after, 3).await.map_err(open_error)?;
        observed.extend(page.digests);
        after = page.next;
        if after.is_none() {
            break;
        }
        if page_number == 3 {
            return Err(format!("step {step}: listing cursor did not finish"));
        }
    }
    if !observed.iter().eq(expected.keys()) {
        return Err(format!(
            "step {step}: listing differs from model or repeats"
        ));
    }
    match (
        expected.get(&selected),
        provider.get(selected, ByteRange::new(1, 4), 3).await,
    ) {
        (Some(body), Ok(result)) if body.get(1..4) == Some(result.body().as_ref()) => Ok(()),
        (None, Err(error)) if error.code == acyclic_objects::wire::ErrorCode::NotFound => Ok(()),
        _ => Err(format!("step {step}: range read differs from model")),
    }
}
