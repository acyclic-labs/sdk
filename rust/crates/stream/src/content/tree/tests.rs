use super::*;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

// Real immutable files exercise the shared algorithm, including close/reopen.
struct Disk {
    directory: PathBuf,
    reads: AtomicUsize,
    largest: AtomicUsize,
}

impl Disk {
    fn open(path: &Path) -> Self {
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
