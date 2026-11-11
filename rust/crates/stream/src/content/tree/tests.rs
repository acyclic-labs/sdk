use super::*;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

// Real immutable files exercise the shared algorithm, including close/reopen.
pub(crate) struct Disk {
    directory: PathBuf,
    reads: AtomicUsize,
    pub(crate) largest: AtomicUsize,
}

impl Disk {
    pub(crate) fn open(path: &Path) -> Self {
        Self {
            directory: path.to_owned(),
            reads: AtomicUsize::new(0),
            largest: AtomicUsize::new(0),
        }
    }
    fn key(&self, digest: [u8; 32]) -> PathBuf {
        let mut name = String::with_capacity(64);
        for byte in digest {
            write!(name, "{byte:02x}").expect("formatting into a String");
        }
        self.directory.join(name)
    }
}

#[async_trait]
impl Storage for Disk {
    async fn put(&self, digest: [u8; 32], page: Page) -> Result<(), StreamError> {
        self.largest.fetch_max(page.bytes.len(), Ordering::Relaxed);
        verify(&digest, page.bytes.as_slice())?;
        let mut encoded = vec![u8::from(page.kind == Kind::Manifest)];
        encoded.extend_from_slice(page.bytes.as_slice());
        let path = self.key(digest);
        match std::fs::read(&path) {
            Ok(existing) if existing == encoded => return Ok(()),
            Ok(_) => return Err(StreamError::InvalidResponse),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(StreamError::Unavailable),
        }
        std::fs::write(path, encoded).map_err(|_| StreamError::Unavailable)
    }
    async fn get(&self, digest: [u8; 32]) -> Result<Page, StreamError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let encoded = std::fs::read(self.key(digest)).map_err(|_| StreamError::Unavailable)?;
        let kind = match encoded.first() {
            Some(0) => Kind::Blob,
            Some(1) => Kind::Manifest,
            _ => return Err(StreamError::InvalidResponse),
        };
        Ok(Page {
            kind,
            bytes: OwnedBytes::from(encoded).slice(1..),
        })
    }
}

fn pattern(offset: usize) -> u8 {
    (offset as u8).wrapping_mul(31) ^ (offset >> 16) as u8
}
fn input(length: usize, fragment: usize) -> impl Stream<Item = Result<OwnedBytes, StreamError>> {
    futures::stream::iter((0..length).step_by(fragment).map(move |offset| {
        Ok((offset..(offset + fragment).min(length))
            .map(pattern)
            .collect::<Vec<_>>()
            .into())
    }))
}

#[tokio::test]
async fn content_64_mib_survives_reopen_with_bounded_lazy_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let length = 64 * 1024 * 1024;
    let root = upload(&disk, input(length, 123_457)).await?;
    assert_eq!(root.part.length, length as u64);
    assert!(disk.largest.load(Ordering::Relaxed) <= PAGE_BYTES);
    drop(disk);
    let disk = Disk::open(directory.path());
    let mut reader = Reader::new(&disk, root)?;
    assert_eq!(disk.reads.load(Ordering::Relaxed), 0);
    let mut position = 0;
    while let Some(bytes) = reader.next().await? {
        assert!(bytes.len() <= LEAF_BYTES);
        assert!(
            bytes
                .as_slice()
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == pattern(position + index))
        );
        position += bytes.len();
    }
    assert_eq!(position, length);
    assert_eq!(
        disk.reads.load(Ordering::Relaxed),
        length.div_ceil(LEAF_BYTES) + 1
    );
    Ok(())
}

#[tokio::test]
async fn content_identity_ignores_fragments_and_blob_bytes_cannot_alias_manifests()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let length = 3 * LEAF_BYTES + 17;
    let left = upload(&disk, input(length, 97)).await?;
    let right = upload(&disk, input(length, length)).await?;
    assert_eq!(left, right);
    let manifest = disk.get(left.part.digest).await?;
    let embedded = upload(&disk, futures::stream::iter([Ok(manifest.bytes.clone())])).await?;
    assert_ne!(embedded.part.digest, left.part.digest);
    let mut reader = Reader::new(&disk, embedded)?;
    assert_eq!(
        reader
            .next()
            .await?
            .ok_or("missing embedded bytes")?
            .as_slice(),
        manifest.bytes.as_slice()
    );
    assert!(reader.next().await?.is_none());
    let empty = upload(&disk, futures::stream::empty()).await?;
    let mut reader = Reader::new(&disk, empty)?;
    assert!(
        reader
            .next()
            .await?
            .ok_or("missing empty content")?
            .is_empty()
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn content_dependencies_spill_to_edges_without_changing_byte_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let value = upload(&disk, input(2 * LEAF_BYTES + 1, 997)).await?;
    let mut writer = Writer::new(&disk)?;
    writer.write(b"header").await?;
    writer.reference(value).await?;
    writer.write(b"tail").await?;
    let root = writer.finish().await?;
    let page = disk.get(root.part.digest).await?;
    let manifest = Manifest::decode(page.bytes.as_slice())?;
    assert_eq!(manifest.parts().len(), 1);
    assert_eq!(
        manifest.edges().collect::<Vec<_>>(),
        vec![value.part.digest]
    );
    let mut reader = Reader::new(&disk, root)?;
    assert_eq!(
        reader.next().await?.ok_or("missing body")?.as_slice(),
        b"headertail"
    );
    assert!(reader.next().await?.is_none());

    let mut writer = Writer::new(&disk)?;
    writer.write(b"x").await?;
    for _ in 0..EDGE_FANOUT + 1 {
        writer.reference(value).await?;
    }
    let root = writer.finish().await?;
    let page = disk.get(root.part.digest).await?;
    let manifest = Manifest::decode(page.bytes.as_slice())?;
    let edges = manifest.edges().collect::<Vec<_>>();
    assert_eq!(edges.len(), 1);
    let dependencies = disk.get(edges[0]).await?;
    let dependencies = Manifest::decode(dependencies.bytes.as_slice())?;
    assert_eq!(dependencies.parts().len(), 0);
    let children = dependencies.edges().collect::<Vec<_>>();
    assert_eq!(children.len(), 2);
    for (digest, count) in children.into_iter().zip([EDGE_FANOUT, 1]) {
        let page = disk.get(digest).await?;
        assert!(page.bytes.len() <= PAGE_BYTES);
        let manifest = Manifest::decode(page.bytes.as_slice())?;
        assert_eq!(manifest.parts().len(), 0);
        assert_eq!(manifest.edges().len(), count);
        assert!(manifest.edges().all(|digest| digest == value.part.digest));
    }
    let mut selected = Reader::range(&disk, root, 0, 1)?;
    assert_eq!(
        selected
            .next()
            .await?
            .ok_or("missing byte range")?
            .as_slice(),
        b"x"
    );
    assert!(selected.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn content_ranges_seek_without_fetching_unselected_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let length = 3 * LEAF_BYTES + 17;
    let root = upload(&disk, input(length, 997)).await?;
    let before = disk.reads.load(Ordering::Relaxed);
    let offset = LEAF_BYTES - 7;
    let selected = LEAF_BYTES + 23;
    let mut reader = Reader::range(&disk, root, offset as u64, selected as u64)?;
    let mut position = offset;
    while let Some(bytes) = reader.next().await? {
        assert!(!bytes.is_empty());
        assert!(
            bytes
                .as_slice()
                .iter()
                .enumerate()
                .all(|(index, byte)| { *byte == pattern(position + index) })
        );
        position += bytes.len();
    }
    assert_eq!(position, offset + selected);
    assert_eq!(disk.reads.load(Ordering::Relaxed) - before, 4);

    let before = disk.reads.load(Ordering::Relaxed);
    let mut suffix = Reader::range(&disk, root, (length - 17) as u64, 17)?;
    let bytes = suffix.next().await?.ok_or("missing selected suffix")?;
    assert_eq!(bytes.len(), 17);
    assert!(
        bytes
            .as_slice()
            .iter()
            .enumerate()
            .all(|(index, byte)| { *byte == pattern(length - 17 + index) })
    );
    assert!(suffix.next().await?.is_none());
    assert_eq!(disk.reads.load(Ordering::Relaxed) - before, 2);

    let before = disk.reads.load(Ordering::Relaxed);
    let mut empty = Reader::range(&disk, root, length as u64, 0)?;
    assert!(empty.next().await?.is_none());
    assert_eq!(disk.reads.load(Ordering::Relaxed), before);
    assert!(matches!(
        Reader::range(&disk, root, length as u64, 1),
        Err(StreamError::InvalidArgument)
    ));
    assert!(matches!(
        Reader::range(&disk, root, u64::MAX, 1),
        Err(StreamError::InvalidArgument)
    ));
    Ok(())
}

#[tokio::test]
async fn content_tree_carries_across_manifest_pages_and_refuses_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let mut builder = Builder {
        storage: &disk,
        levels: Vec::new(),
    };
    for index in 0..FANOUT + 1 {
        let mut bytes = BLOB_MAGIC.to_vec();
        bytes.push(index as u8);
        let part = put(&disk, Kind::Blob, bytes, 1).await?;
        builder.push(0, part).await?;
    }
    let root = builder.finish().await?;
    assert_eq!(root.height, 2);
    let mut reader = Reader::new(&disk, root)?;
    for index in 0..FANOUT + 1 {
        assert_eq!(
            reader.next().await?.ok_or("missing tree leaf")?.as_slice(),
            &[index as u8]
        );
    }
    assert!(reader.next().await?.is_none());
    // Seek across two manifest subtrees without fetching preceding leaves.
    let before = disk.reads.load(Ordering::Relaxed);
    let mut selected = Reader::range(&disk, root, FANOUT as u64 - 1, 2)?;
    for index in FANOUT - 1..=FANOUT {
        assert_eq!(
            selected
                .next()
                .await?
                .ok_or("missing selected leaf")?
                .as_slice(),
            &[index as u8]
        );
    }
    assert!(selected.next().await?.is_none());
    assert_eq!(disk.reads.load(Ordering::Relaxed) - before, 5);
    let root_path = disk.key(root.part.digest);
    let mut stored = std::fs::read(&root_path)?;
    *stored.last_mut().ok_or("empty manifest")? ^= 1;
    std::fs::write(root_path, stored)?;
    assert!(matches!(
        Reader::new(&disk, root)?.next().await,
        Err(StreamError::InvalidResponse)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_content_read_cannot_skip_the_consumed_page()
-> Result<(), Box<dyn std::error::Error>> {
    struct DelayedRead {
        disk: Disk,
        gate: std::sync::Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
    }
    #[async_trait::async_trait]
    impl Storage for DelayedRead {
        async fn put(&self, digest: [u8; 32], page: Page) -> Result<(), StreamError> {
            self.disk.put(digest, page).await
        }
        async fn get(&self, digest: [u8; 32]) -> Result<Page, StreamError> {
            let page = self.disk.get(digest).await?;
            let gate = if page.kind == Kind::Blob {
                self.gate
                    .lock()
                    .map_err(|_| StreamError::Unavailable)?
                    .take()
            } else {
                None
            };
            if let Some((fetched, resume)) = gate {
                fetched.send(()).map_err(|_| StreamError::Unavailable)?;
                resume.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(page)
        }
    }
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let root = upload(&disk, input(2 * LEAF_BYTES + 1, LEAF_BYTES)).await?;
    let (fetched, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    let delayed = DelayedRead {
        disk,
        gate: std::sync::Mutex::new(Some((fetched, wait))),
    };
    let mut reader = Reader::new(&delayed, root)?;
    {
        let reading = reader.next();
        tokio::pin!(reading);
        tokio::select! {
            result = &mut reading => return Err(format!("read completed before its fetched-page event: {result:?}").into()),
            result = event => result?,
        }
    }
    assert!(
        matches!(reader.next().await, Err(StreamError::Unavailable)),
        "cancelled content reader skipped the consumed page"
    );
    Ok(())
}
