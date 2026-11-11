use super::*;
use crate::content::tree::tests::{Delayed, Disk, reachable};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering as AtomicOrdering;

fn bytes(value: impl AsRef<[u8]>) -> Field {
    Field::Inline(OwnedBytes::copy_from_slice(value.as_ref()))
}

async fn verify(
    storage: &impl Storage,
    index: &Index,
    model: &BTreeMap<Vec<u8>, Field>,
) -> Result<(), StreamError> {
    assert_eq!(index.len(), model.len() as u64);
    let mut cursor = index.scan(storage, None).await?;
    for (key, value) in model {
        assert_eq!(
            cursor.next().await?,
            Some((bytes(key), value.clone())),
            "batched index rewrite lost keys, values or original order"
        );
    }
    assert!(cursor.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn batch_rewrites_one_leaf_with_100k_updates_and_preserves_untouched_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut builder = Builder::new(&storage);
    let mut original = BTreeMap::new();
    let value = bytes(vec![0x71; 1024]);
    for ordinal in 0..1024_u64 {
        let key = format!("k/{ordinal:06}").into_bytes();
        builder.push(bytes(&key), value.clone()).await?;
        original.insert(key, value.clone());
    }
    let before = builder.finish().await?;
    let root = before.root.ok_or("missing original root")?;
    let Page::Internal(children) = codec::read(&storage, root, before.height).await? else {
        return Err("fixture did not have original child bounds".into());
    };
    assert!(children.len() > 4);
    let untouched = children[children.len() / 4].page;
    let Field::Inline(boundary) = &children[children.len() / 2].first.field else {
        return Err("fixture boundary was not inline".into());
    };
    let mut updates = BTreeMap::new();
    updates.insert(b"a-before".to_vec(), bytes(b"before"));
    updates.insert(b"k/000010".to_vec(), bytes(b"replacement"));
    updates.insert(boundary.as_slice().to_vec(), bytes(b"boundary replacement"));
    let padding = "x".repeat(46);
    for ordinal in 0..100_000_u64 {
        let key = format!("k/000500/{ordinal:06}/{padding}").into_bytes();
        updates.insert(key, bytes(ordinal.to_le_bytes()));
    }
    updates.insert(b"z-after".to_vec(), bytes(b"after"));
    let mut model = original.clone();
    model.extend(
        updates
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    let mut builder = Builder::new(&storage);
    for (key, value) in updates {
        builder.push(bytes(key), value).await?;
    }
    let updates = builder.finish().await?;
    let merged = before.merge(&storage, &updates).await?;
    assert!(
        merged.height >= 2,
        "batch did not promote beyond one original level"
    );
    verify(&storage, &merged, &model).await?;
    verify(&storage, &before, &original).await?;
    assert_eq!(merged.merge(&storage, &Index::default()).await?, merged);
    assert_eq!(Index::default().merge(&storage, &updates).await?, updates);
    let checkpoint = merged.checkpoint(&storage).await?;
    let graph = reachable(&storage, checkpoint).await?;
    assert!(
        graph.contains(&untouched.part.digest),
        "batch replaced an untouched subtree"
    );
    assert!(storage.largest.load(AtomicOrdering::Relaxed) <= tree::PAGE_BYTES);
    drop(storage);
    let storage = Disk::open(directory.path());
    let reopened = Index::open(&storage, checkpoint).await?;
    verify(&storage, &reopened, &model).await?;
    Ok(())
}

#[tokio::test]
async fn cancelling_batch_page_io_leaves_the_original_root_whole()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let mut builder = Builder::new(&disk);
    builder.push(bytes(b"a"), bytes(b"old")).await?;
    builder.push(bytes(b"c"), bytes(b"old")).await?;
    let before = builder.finish().await?;
    let mut builder = Builder::new(&disk);
    builder.push(bytes(b"a"), bytes(b"new")).await?;
    builder.push(bytes(b"b"), bytes(b"inserted")).await?;
    let updates = builder.finish().await?;
    let storage = Delayed {
        disk,
        put: Default::default(),
        get: Default::default(),
    };
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (_resume, resumed) = tokio::sync::oneshot::channel();
    *storage.put.lock().map_err(|_| "gate poisoned")? = Some((entered, resumed));
    {
        let apply = before.merge(&storage, &updates);
        tokio::pin!(apply);
        tokio::select! {
            _ = waiting => {},
            result = &mut apply => { result?; return Err("batch page write did not suspend".into()); },
        }
    }
    let original = BTreeMap::from([
        (b"a".to_vec(), bytes(b"old")),
        (b"c".to_vec(), bytes(b"old")),
    ]);
    verify(&storage, &before, &original).await?;
    let mut model = original.clone();
    model.insert(b"a".to_vec(), bytes(b"new"));
    model.insert(b"b".to_vec(), bytes(b"inserted"));
    let merged = before.merge(&storage, &updates).await?;
    verify(&storage, &merged, &model).await?;
    verify(&storage, &before, &original).await?;
    assert_eq!(merged.merge(&storage, &updates).await?, merged);
    Ok(())
}
