use super::*;
use crate::content::tree::tests::Disk;
use std::sync::atomic::Ordering;

fn bytes(value: impl AsRef<[u8]>) -> Field {
    Field::Inline(OwnedBytes::copy_from_slice(value.as_ref()))
}

async fn reachable(
    storage: &Disk,
    root: Root,
) -> Result<std::collections::BTreeSet<[u8; 32]>, StreamError> {
    let mut pending = vec![root.part.digest];
    let mut reachable = std::collections::BTreeSet::new();
    while let Some(digest) = pending.pop() {
        if !reachable.insert(digest) {
            continue;
        }
        let page = storage.get(digest).await?;
        if page.kind == tree::Kind::Manifest {
            let manifest = crate::content::Manifest::decode(page.bytes.as_slice())?;
            pending.extend(manifest.parts().map(|part| part.digest));
            pending.extend(manifest.edges());
        }
    }
    Ok(reachable)
}

#[tokio::test]
async fn ordered_index_builds_multiple_levels_and_reopens_pinned_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let empty = Builder::new(&storage).finish().await?;
    assert!(empty.get(&storage, bytes("missing")).await?.is_none());
    let mut before = Builder::new(&storage);
    before.push(bytes("before"), bytes("old")).await?;
    let before = before.finish().await?;
    let mut builder = Builder::new(&storage);
    let value = OwnedBytes::from(vec![0x71; 3500]);
    for index in 0..8192_u64 {
        builder
            .push(
                bytes(format!("keys/{index:08}")),
                Field::Inline(value.clone()),
            )
            .await?;
    }
    let index = builder.finish().await?;
    assert!(
        index.height >= 2,
        "control did not cross internal-page boundaries"
    );
    assert_eq!(index.len(), 8192);
    let root = index.root().ok_or("missing index root")?;
    let graph = reachable(&storage, root).await?;
    let Page::Internal(children) = codec::read(&storage, root, index.height).await? else {
        return Err("expected internal root".into());
    };
    for child in children {
        assert!(
            graph.contains(&child.page.part.digest),
            "ordered index lost a child-page dependency"
        );
    }
    assert_eq!(
        before.get(&storage, bytes("before")).await?,
        Some(bytes("old"))
    );
    assert!(
        before
            .get(&storage, bytes("keys/00000000"))
            .await?
            .is_none()
    );
    let reopened = Disk::open(directory.path());
    for position in (0..8192).step_by(31).chain([0, 1, 7, 8, 511, 4095, 8191]) {
        assert_eq!(
            index
                .get(&reopened, bytes(format!("keys/{position:08}")))
                .await?,
            Some(Field::Inline(value.clone()))
        );
    }
    for key in ["", "keys/", "keys/00000000/", "keys/00008192", "z"] {
        assert!(index.get(&reopened, bytes(key)).await?.is_none());
    }
    assert!(storage.largest.load(Ordering::Relaxed) <= tree::PAGE_BYTES);
    Ok(())
}

#[tokio::test]
async fn ordered_index_compares_large_keys_by_bytes_and_retains_references()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut left = vec![b'a'; 2 * tree::PAGE_BYTES];
    left.push(b'0');
    let mut writer = tree::Writer::new(&storage)?;
    writer.write(&left).await?;
    let left_root = writer.finish().await?;
    let mut right = left.clone();
    *right.last_mut().ok_or("empty key")? = b'1';
    let value = vec![0xc3; 3 * tree::PAGE_BYTES + 1];
    let mut writer = tree::Writer::new(&storage)?;
    writer.write(&value).await?;
    let value_root = writer.finish().await?;
    let mut builder = Builder::new(&storage);
    builder.push(bytes("0"), bytes("first")).await?;
    builder
        .push(Field::Content(left_root), Field::Content(value_root))
        .await?;
    builder.push(bytes(&right), bytes("second")).await?;
    builder.push(bytes("z"), bytes("last")).await?;
    let index = builder.finish().await?;
    assert_eq!(
        index.get(&storage, bytes(&left)).await?,
        Some(Field::Content(value_root))
    );
    assert_eq!(
        index.get(&storage, bytes(&right)).await?,
        Some(bytes("second"))
    );
    left.push(0);
    assert!(index.get(&storage, bytes(&left)).await?.is_none());
    let reachable = reachable(&storage, index.root().ok_or("missing index root")?).await?;
    assert!(
        reachable.contains(&left_root.part.digest),
        "ordered index lost a large-key dependency"
    );
    assert!(
        reachable.contains(&value_root.part.digest),
        "ordered index lost a large-value dependency"
    );
    let mut duplicate = Builder::new(&storage);
    duplicate
        .push(Field::Content(left_root), bytes("one"))
        .await?;
    left.pop();
    assert_eq!(
        duplicate.push(bytes(&left), bytes("two")).await,
        Err(StreamError::InvalidArgument)
    );
    assert!(matches!(
        duplicate.finish().await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_ordered_index_input_cannot_resume_or_seal_a_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    type Gate = (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );
    struct Delayed {
        disk: Disk,
        gate: std::sync::Mutex<Option<Gate>>,
    }
    #[async_trait::async_trait]
    impl Storage for Delayed {
        async fn put(&self, digest: [u8; 32], page: tree::Page) -> Result<(), StreamError> {
            self.disk.put(digest, page).await?;
            let gate = self
                .gate
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take();
            if let Some((written, resume)) = gate {
                written.send(()).map_err(|_| StreamError::Unavailable)?;
                resume.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(())
        }
        async fn get(&self, digest: [u8; 32]) -> Result<tree::Page, StreamError> {
            self.disk.get(digest).await
        }
    }
    let directory = tempfile::tempdir()?;
    let (written, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    let storage = Delayed {
        disk: Disk::open(directory.path()),
        gate: std::sync::Mutex::new(Some((written, wait))),
    };
    let mut builder = Builder::new(&storage);
    builder.push(bytes("a"), bytes("accepted input")).await?;
    {
        let writing = builder.push(bytes("b"), bytes(vec![1; tree::PAGE_BYTES]));
        tokio::pin!(writing);
        tokio::select! {
            result = &mut writing => return Err(format!("input completed before its stored-page event: {result:?}").into()),
            result = event => result?,
        }
    }
    assert_eq!(
        builder.push(bytes("c"), bytes("replacement")).await,
        Err(StreamError::Unavailable),
        "cancelled ordered-index builder accepted replacement input"
    );
    assert!(matches!(
        builder.finish().await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}
