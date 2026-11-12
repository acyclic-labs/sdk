use super::*;
use crate::content::tree::tests::{Disk, reachable};

async fn index(storage: &impl Storage, label: &'static [u8]) -> Result<Root, StreamError> {
    Index::default()
        .set(
            storage,
            Field::Inline(OwnedBytes::from_static(label)),
            Field::Inline(OwnedBytes::from_static(b"value")),
        )
        .await?
        .checkpoint(storage)
        .await
}

#[tokio::test]
async fn control_roots_reopen_complete_graphs_and_keep_pinned_predecessors()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let namespace = index(&storage, b"namespace").await?;
    let decisions = index(&storage, b"decisions").await?;
    let (registry, binding) = Registry::default()
        .begin(
            &storage,
            CommitId::from_bytes([4; 32]),
            IdempotencyKey::new("root")?,
            [8; 32],
        )
        .await?;
    let stages = registry.checkpoint(&storage).await?;
    let before = ControlRoot {
        namespace,
        stages,
        decisions,
        pending: None,
    };
    let before_root = before.write(&storage).await?;
    let mut writer = tree::Writer::new(&storage)?;
    writer.write(b"opaque pending decision fixture").await?;
    let pending = writer.finish().await?;
    let after = ControlRoot {
        namespace: index(&storage, b"successor").await?,
        pending: Some(pending),
        ..before.clone()
    };
    let after_root = after.write(&storage).await?;
    drop(storage);
    let storage = Disk::open(directory.path());
    assert_eq!(ControlRoot::open(&storage, before_root).await?, before);
    assert_eq!(ControlRoot::open(&storage, after_root).await?, after);
    for (descriptor, root) in [(&before, before_root), (&after, after_root)] {
        let graph = reachable(&storage, root).await?;
        for component in [
            Some(descriptor.namespace),
            Some(descriptor.stages),
            Some(descriptor.decisions),
            descriptor.pending,
        ]
        .into_iter()
        .flatten()
        {
            assert!(
                graph.contains(&component.part.digest),
                "control root omitted a component dependency"
            );
        }
    }
    let reopened = Registry::open(&storage, after.stages).await?;
    assert_eq!(reopened.get(&storage, binding.id()).await?, Some(binding));
    Ok(())
}

#[tokio::test]
async fn control_root_codec_refuses_every_truncation_trailing_bytes_and_invalid_tag()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let empty = Index::default().checkpoint(&storage).await?;
    for pending in [None, Some(empty)] {
        let descriptor = ControlRoot {
            namespace: empty,
            stages: empty,
            decisions: empty,
            pending,
        };
        let root = descriptor.write(&storage).await?;
        let mut input = tree::Reader::new(&storage, root)?;
        let mut bytes = Vec::new();
        while let Some(page) = input.next().await? {
            bytes.extend_from_slice(page.as_slice());
        }
        let mut invalid: Vec<Vec<u8>> = (0..bytes.len()).map(|end| bytes[..end].to_vec()).collect();
        let mut trailing = bytes.clone();
        trailing.push(0);
        invalid.push(trailing);
        let mut tag = bytes.clone();
        tag[BASE_BYTES - 1] = 2;
        invalid.push(tag);
        for bytes in invalid {
            let mut writer = tree::Writer::new(&storage)?;
            writer.write(&bytes).await?;
            let root = writer.finish().await?;
            assert!(
                ControlRoot::open(&storage, root).await.is_err(),
                "control root accepted an incomplete or malformed frame"
            );
        }
    }
    Ok(())
}
