//! Manifest page publication through the real engine and its fault medium.
use super::*;
use crate::{Manifest, ManifestBuilder, MemoryObjects, Part};

#[tokio::test]
async fn empty_body_preserves_root_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = MemoryObjects::memory();
    let edge = blobs.put(b"edge".to_vec().into(), Vec::new()).await?;
    let edge = edge.verified().ok_or("unknown put")?.digest;
    let metadata = Manifest {
        edges: vec![edge],
        ..Manifest::default()
    };
    let root = ManifestBuilder::default().finish(&blobs, metadata).await?;
    assert_eq!(root.length, 0);
    assert!(blobs.head(root.digest).await?.references.contains(&edge));
    assert!(
        blobs
            .get(root.digest, ByteRange::FULL, 0)
            .await?
            .body()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn nested_pages_preserve_order_across_page_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let blobs = MemoryObjects::memory();
    let mut parts = Vec::new();
    let mut expected = Vec::new();
    for value in 0_u16..257 {
        let bytes = value.to_le_bytes();
        let put = blobs.put(bytes.to_vec().into(), Vec::new()).await?;
        let digest = put.verified().ok_or("unknown put")?.digest;
        expected.extend_from_slice(&bytes);
        parts.push(Part { digest, length: 2 });
    }
    let root = RootId::new(b"manifest-builder-inputs".to_vec())?;
    blobs
        .retain(&root, parts.iter().map(|part| part.digest).collect())
        .await?;
    let mut builder = ManifestBuilder::default();
    for part in parts {
        builder.push(&blobs, part).await?;
    }
    let result = builder.finish(&blobs, Manifest::default()).await?;
    assert_eq!(result.length, expected.len() as u64);
    blobs.retain(&root, vec![result.digest]).await?;
    let selected = blobs
        .get(result.digest, ByteRange::new(250, 270), 20)
        .await?;
    assert_eq!(
        selected.body().as_ref(),
        expected.get(250..270).ok_or("range")?
    );
    let selected = blobs
        .get(result.digest, ByteRange::FULL, result.length)
        .await?;
    assert_eq!(selected.body().as_ref(), expected);
    Ok(())
}

#[tokio::test]
async fn upper_page_carries_keep_ranges_in_order() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = MemoryObjects::memory();
    let mut values = Vec::new();
    for value in 0_u8..=250 {
        let put = blobs.put(vec![value].into(), Vec::new()).await?;
        values.push(Part {
            digest: put.verified().ok_or("unknown put")?.digest,
            length: 1,
        });
    }
    let hold = RootId::new(b"carry-inputs".to_vec())?;
    blobs
        .retain(&hold, values.iter().map(|part| part.digest).collect())
        .await?;
    for count in [16_257_usize, 16_384, 16_385] {
        let mut builder = ManifestBuilder::default();
        let mut expected = Vec::new();
        for index in 0..count {
            let value = index % values.len();
            expected.push(u8::try_from(value)?);
            builder
                .push(&blobs, *values.get(value).ok_or("missing part")?)
                .await?;
        }
        let root = builder.finish(&blobs, Manifest::default()).await?;
        for start in [16_252_usize, count - 4] {
            let end = (start + 8).min(count);
            let object = blobs
                .get(
                    root.digest,
                    ByteRange::new(start as u64, end as u64),
                    (end - start) as u64,
                )
                .await?;
            assert_eq!(
                object.body().as_ref(),
                expected.get(start..end).ok_or("range")?
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn interrupted_page_publication_poisoned_the_builder()
-> Result<(), Box<dyn std::error::Error>> {
    for outcome in ["failure", "unknown", "cancelled"] {
        let medium = Shared::default();
        let blobs = Store::new(medium.clone()).account(b"builder-interruption")?;
        let put = blobs.put(b"part".to_vec().into(), Vec::new()).await?;
        let valid = Part {
            digest: put.verified().ok_or("unknown input")?.digest,
            length: 4,
        };
        let mut builder = ManifestBuilder::default();
        for _ in 0..ManifestBuilder::PAGE - 1 {
            builder.push(&blobs, valid).await?;
        }
        match outcome {
            "failure" => {
                let absent = Part {
                    digest: Digest::of(b"missing"),
                    length: 7,
                };
                assert_eq!(
                    builder.push(&blobs, absent).await.unwrap_err().code,
                    ErrorCode::MissingReference
                );
            }
            "unknown" => {
                *medium.ambiguous.lock().map_err(|_| "poisoned fault lock")? = Some("/e/");
                assert_eq!(
                    builder.push(&blobs, valid).await.unwrap_err().code,
                    ErrorCode::OutcomeUnknown
                );
            }
            "cancelled" => {
                let (reached, release) = medium.park("create", "/e/");
                let mut push = Box::pin(builder.push(&blobs, valid));
                tokio::select! {
                    reached = reached => reached?,
                    _ = &mut push => return Err("page did not reach its publication cut".into()),
                }
                drop(push);
                drop(release);
            }
            _ => return Err("unexpected outcome".into()),
        }
        assert_eq!(
            builder.push(&blobs, valid).await.unwrap_err().code,
            ErrorCode::OutcomeUnknown
        );
        assert_eq!(
            builder
                .finish(&blobs, Manifest::default())
                .await
                .unwrap_err()
                .code,
            ErrorCode::OutcomeUnknown
        );
    }
    Ok(())
}