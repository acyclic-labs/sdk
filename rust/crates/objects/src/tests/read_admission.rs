//! Real retained local bytes through an applied read-only medium.
use super::*;
use std::sync::{Arc, atomic::AtomicUsize};

struct Applied {
    inner: Arc<crate::LocalMedium>,
    writes: AtomicUsize,
}

impl Applied {
    fn refuse<T>(&self) -> Result<T, MediumError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(MediumError::Failed(
            std::io::ErrorKind::PermissionDenied.into(),
        ))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Medium for Applied {
    async fn create(&self, _: &str, _: OwnedBytes) -> Result<bool, MediumError> {
        self.refuse()
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        self.inner.read(key, range).await
    }

    async fn remove(&self, _: &str) -> Result<bool, MediumError> {
        self.refuse()
    }

    async fn replace(&self, _: &str, _: &[u8], _: OwnedBytes) -> Result<bool, MediumError> {
        self.refuse()
    }

    async fn remove_if(&self, _: &str, _: &[u8]) -> Result<bool, MediumError> {
        self.refuse()
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        self.inner.list(prefix, after, limit).await
    }

    async fn flush(&self, _: &[String]) -> Result<(), MediumError> {
        self.refuse()
    }

    fn volatile(&self) -> bool {
        self.inner.volatile()
    }

    fn recovered(&self) -> bool {
        self.inner.recovered()
    }

    fn settle(&self) {
        self.inner.settle();
    }
}

#[tokio::test]
async fn retained_local_reads_do_not_claim_a_writer_run() -> Result<(), Box<dyn std::error::Error>>
{
    use acyclic_stream::RevisionCut;

    let directory = tempfile::tempdir()?;
    let store =
        Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
    let writer = store.account(b"original-account")?;
    let bytes = OwnedBytes::from(b"retained package bytes".to_vec());
    let put = writer.put(bytes.clone(), Vec::new()).await?;
    let verified = put.verified().ok_or("publication is known")?;
    let digest = verified.digest;
    let root = RootId::new(b"original-publication".to_vec())?;
    let retained = writer.retain(&root, vec![digest]).await?;
    let minimum = RevisionCut::of(retained.revision.ok_or("retain commits a revision")?);
    let selected = Store::new(Applied {
        inner: Arc::clone(store.medium()),
        writes: AtomicUsize::new(0),
    });
    let reader = selected.account(b"original-account")?;
    assert!(reader.revisions().await?.is_none());
    // A writer's run record cannot prove an index at a selected applied cut.
    assert_eq!(reader.admitted(minimum.clone()).await?, minimum);
    let object = reader
        .get(digest, ByteRange::FULL, bytes.len() as u64)
        .await?;
    assert_eq!(object.body(), &bytes);
    assert!(object.head().revision.is_none());
    let head = reader.head(digest).await?;
    assert_eq!(head.digest, digest);
    assert_eq!(head.length, bytes.len() as u64);
    assert!(head.revision.is_none());
    let page = reader.list(None, 1).await?;
    assert_eq!(page.digests, vec![digest]);
    assert!(page.revision.is_none());
    assert_eq!(reader.usage().await?.blobs, 1);

    let budget = Arc::new(Budget::new(u64::MAX));
    *budget.writes.lock().unwrap() = Some(0);
    let meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = budget;
    let request = crate::ObjectsNativeRequest::new(
        (digest, ByteRange::FULL, bytes.len() as u64),
        (),
        &meter,
    )?;
    let native = reader.get_native(request, Arc::clone(&meter))?.await?;
    assert_eq!(native.body(), &bytes);
    assert!(native.head().revision.is_none());

    // Original account selection still fences equal digests in another account.
    assert_eq!(
        selected
            .account(b"other-account")?
            .head(digest)
            .await
            .map_err(|e| e.code),
        Err(ErrorCode::NotFound),
    );
    assert_eq!(selected.medium().writes.load(Ordering::SeqCst), 0);
    // The same projection refuses a real writer; reads did not borrow authority.
    assert_eq!(
        reader.put(bytes, Vec::new()).await.map_err(|e| e.code),
        Err(ErrorCode::Unavailable),
    );
    assert_eq!(selected.medium().writes.load(Ordering::SeqCst), 1);
    Ok(())
}
