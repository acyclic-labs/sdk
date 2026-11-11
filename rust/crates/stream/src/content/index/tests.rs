use super::*;
use crate::content::tree::tests::Disk;
use std::sync::atomic::Ordering;

fn bytes(value: impl AsRef<[u8]>) -> Field {
    Field::Inline(OwnedBytes::copy_from_slice(value.as_ref()))
}

type Gate = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);
struct Delayed {
    disk: Disk,
    put: std::sync::Mutex<Option<Gate>>,
    get: std::sync::Mutex<Option<Gate>>,
}
async fn pause(gate: &std::sync::Mutex<Option<Gate>>) -> Result<(), StreamError> {
    let gate = gate.lock().map_err(|_| StreamError::Unavailable)?.take();
    if let Some((entered, resume)) = gate {
        entered.send(()).map_err(|_| StreamError::Unavailable)?;
        resume.await.map_err(|_| StreamError::Unavailable)?;
    }
    Ok(())
}
#[async_trait::async_trait]
impl Storage for Delayed {
    async fn put(&self, digest: [u8; 32], page: tree::Page) -> Result<(), StreamError> {
        self.disk.put(digest, page).await?;
        pause(&self.put).await
    }
    async fn get(&self, digest: [u8; 32]) -> Result<tree::Page, StreamError> {
        let page = self.disk.get(digest).await?;
        pause(&self.get).await?;
        Ok(page)
    }
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
    let checkpoint = index.checkpoint(&storage).await?;
    assert!(
        reachable(&storage, checkpoint)
            .await?
            .contains(&root.part.digest),
        "index checkpoint lost its node ownership"
    );
    drop(index);
    let reopened = Disk::open(directory.path());
    let index = Index::open(&reopened, checkpoint).await?;
    assert_eq!(index.len(), 8192);
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
    let mut cursor = index.scan(&reopened, None).await?;
    for position in 0..8192 {
        assert_eq!(
            cursor.next().await?,
            Some((
                bytes(format!("keys/{position:08}")),
                Field::Inline(value.clone())
            ))
        );
    }
    assert!(cursor.next().await?.is_none());
    for position in [0, 1, 7, 8, 9, 511, 1024, 8191, 8192] {
        let key = format!("keys/{position:08}");
        let mut cursor = index.scan(&reopened, Some(bytes(&key))).await?;
        let expected = (position < 8192).then(|| (bytes(&key), Field::Inline(value.clone())));
        assert_eq!(cursor.next().await?, expected);
    }
    assert!(storage.largest.load(Ordering::Relaxed) <= tree::PAGE_BYTES);
    Ok(())
}

#[tokio::test]
async fn ordered_index_checkpoint_refuses_torn_or_malformed_descriptors()
-> Result<(), Box<dyn std::error::Error>> {
    async fn raw(storage: &impl Storage, bytes: &[u8]) -> Result<Root, StreamError> {
        let mut writer = tree::Writer::new(storage)?;
        writer.write(bytes).await?;
        writer.finish().await
    }
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let empty = Index::default().checkpoint(&storage).await?;
    assert_eq!(Index::open(&storage, empty).await?, Index::default());
    let mut trailing = codec::body(&storage, empty, 0, tree::LEAF_BYTES).await?;
    trailing.push(1);
    assert!(
        Index::open(&storage, raw(&storage, &trailing).await?)
            .await
            .is_err(),
        "index checkpoint accepted trailing bytes"
    );
    let mut builder = Builder::new(&storage);
    builder.push(bytes("a"), bytes("first")).await?;
    builder.push(bytes("b"), bytes("second")).await?;
    let index = builder.finish().await?;
    let checkpoint = index.checkpoint(&storage).await?;
    let body = codec::body(&storage, checkpoint, 0, tree::LEAF_BYTES).await?;
    assert_eq!(Index::open(&storage, checkpoint).await?, index);
    for cut in 0..body.len() {
        assert!(
            Index::open(&storage, raw(&storage, &body[..cut]).await?)
                .await
                .is_err(),
            "index checkpoint accepted truncation at {cut}"
        );
    }
    let mut version = body.clone();
    version[checkpoint::MAGIC.len() - 2] = b'2';
    assert_eq!(
        Index::open(&storage, raw(&storage, &version).await?).await,
        Err(StreamError::Unsupported)
    );
    let mut count = body.clone();
    let count_at = count.len() - 8;
    count[count_at..].copy_from_slice(&3_u64.to_le_bytes());
    assert!(
        Index::open(&storage, raw(&storage, &count).await?)
            .await
            .is_err(),
        "index checkpoint accepted invalid leaf count"
    );
    let mut height = body;
    height[checkpoint::MAGIC.len() + 1 + 41] = 1;
    assert!(
        Index::open(&storage, raw(&storage, &height).await?)
            .await
            .is_err(),
        "index checkpoint accepted wrong node height"
    );
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
    struct Backing {
        bytes: Vec<u8>,
        retired: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
        }
    }
    let retired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slice = OwnedBytes::from(bytes::Bytes::from_owner(Backing {
        bytes: vec![9; 1024 * 1024],
        retired: std::sync::Arc::clone(&retired),
    }))
    .slice(..8);
    let mut bounded = Builder::new(&storage);
    bounded.push(bytes("small"), Field::Inline(slice)).await?;
    assert!(
        retired.load(Ordering::Acquire),
        "inline index field retained an oversized backing"
    );
    let bounded = bounded.finish().await?;
    assert_eq!(
        bounded.get(&storage, bytes("small")).await?,
        Some(bytes([9; 8]))
    );
    Ok(())
}

#[tokio::test]
async fn unordered_index_runs_match_the_ordered_model_and_refuse_global_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut sorter = Sorter::new(&storage);
    let mut expected = std::collections::BTreeMap::new();
    for source in 0..4097_u64 {
        let position = source * 2053 % 4097;
        let key = format!("descriptors/{position:08}");
        let value = position.to_le_bytes();
        expected.insert(key.clone(), value);
        sorter.push(bytes(&key), bytes(value)).await?;
    }
    let index = sorter.finish().await?;
    assert_eq!(index.len(), expected.len() as u64);
    let mut cursor = index.scan(&storage, None).await?;
    for (key, value) in expected {
        assert_eq!(cursor.next().await?, Some((bytes(key), bytes(value))));
    }
    assert!(cursor.next().await?.is_none());
    let mut duplicate = Sorter::new(&storage);
    for position in 0..4097_u64 {
        duplicate
            .push(
                bytes(format!("descriptors/{position:08}")),
                bytes(position.to_le_bytes()),
            )
            .await?;
    }
    let result = match duplicate
        .push(bytes("descriptors/00000000"), bytes("changed"))
        .await
    {
        Err(error) => Err(error),
        Ok(()) => duplicate.finish().await.map(|_| ()),
    };
    assert_eq!(
        result,
        Err(StreamError::InvalidArgument),
        "ordered index sorter accepted a duplicate across runs"
    );
    let mut within = Sorter::new(&storage);
    within.push(bytes("same"), bytes("first")).await?;
    assert_eq!(
        within.push(bytes("same"), bytes("second")).await,
        Err(StreamError::InvalidArgument)
    );
    assert!(matches!(
        within.finish().await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_unordered_index_input_cannot_resume_or_seal_a_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (written, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    let storage = Delayed {
        disk: Disk::open(directory.path()),
        put: std::sync::Mutex::new(Some((written, wait))),
        get: std::sync::Mutex::new(None),
    };
    let mut sorter = Sorter::new(&storage);
    sorter.push(bytes("a"), bytes("accepted input")).await?;
    {
        let writing = sorter.push(bytes("b"), bytes(vec![1; tree::PAGE_BYTES]));
        tokio::pin!(writing);
        tokio::select! {
            result = &mut writing => return Err(format!("input completed before its stored-page event: {result:?}").into()),
            result = event => result?,
        }
    }
    assert_eq!(
        sorter.push(bytes("c"), bytes("replacement")).await,
        Err(StreamError::Unavailable),
        "cancelled unordered-index sorter accepted replacement input"
    );
    assert!(matches!(
        sorter.finish().await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_ordered_index_input_cannot_resume_or_seal_a_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (written, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    let storage = Delayed {
        disk: Disk::open(directory.path()),
        put: std::sync::Mutex::new(Some((written, wait))),
        get: std::sync::Mutex::new(None),
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

#[tokio::test]
async fn cancelled_ordered_index_cursor_cannot_skip_a_consumed_child()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Delayed {
        disk: Disk::open(directory.path()),
        put: std::sync::Mutex::new(None),
        get: std::sync::Mutex::new(None),
    };
    let mut builder = Builder::new(&storage);
    let value = OwnedBytes::from(vec![2; 3500]);
    for position in 0..256 {
        builder
            .push(
                bytes(format!("{position:04}")),
                Field::Inline(value.clone()),
            )
            .await?;
    }
    let index = builder.finish().await?;
    assert!(index.height > 0, "control needs a child-read boundary");
    let mut root = index.root().ok_or("missing index root")?;
    let mut height = index.height;
    let first_page = loop {
        match codec::read(&storage, root, height).await? {
            Page::Leaf(entries) => break entries.len(),
            Page::Internal(children) => {
                root = children.first().ok_or("missing first child")?.page;
                height -= 1;
            }
        }
    };
    let mut cursor = index.scan(&storage, None).await?;
    for position in 0..first_page {
        assert_eq!(
            cursor.next().await?,
            Some((
                bytes(format!("{position:04}")),
                Field::Inline(value.clone())
            ))
        );
    }
    let (entered, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    *storage.get.lock().map_err(|_| "gate poisoned")? = Some((entered, wait));
    {
        let reading = cursor.next();
        tokio::pin!(reading);
        tokio::select! {
            result = &mut reading => return Err(format!("cursor completed before child-read event: {result:?}").into()),
            result = event => result?,
        }
    }
    assert!(
        matches!(cursor.next().await, Err(StreamError::Unavailable)),
        "cancelled ordered-index cursor skipped a consumed child"
    );
    let mut fresh = index.scan(&storage, None).await?;
    for position in 0..256 {
        assert_eq!(
            fresh.next().await?,
            Some((
                bytes(format!("{position:04}")),
                Field::Inline(value.clone())
            ))
        );
    }
    assert!(fresh.next().await?.is_none());
    Ok(())
}
