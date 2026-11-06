//! Real retained local bytes through an applied read-only medium.
use super::*;
use std::sync::{Arc, atomic::AtomicUsize};

struct Applied<'a> {
    inner: &'a crate::LocalMedium,
    writes: AtomicUsize,
}

impl Applied<'_> {
    fn refuse<T>(&self) -> Result<T, MediumError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(MediumError::Failed(
            std::io::ErrorKind::PermissionDenied.into(),
        ))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Medium for Applied<'_> {
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

    async fn observe(&self, key: String) -> Result<Option<Observed<'_>>, MediumError> {
        self.inner.observe(key).await
    }

    async fn replace<'a>(
        &'a self,
        _: Observation<'a>,
        _: OwnedBytes,
    ) -> Result<Option<Observation<'a>>, MediumError> {
        self.refuse()
    }

    async fn remove_if(&self, _: Observation<'_>) -> Result<bool, MediumError> {
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
async fn native_local_batch_refusals_belong_to_their_original_members()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store =
        Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
    let blobs = store.account(b"original-members")?;
    let bytes = OwnedBytes::from(b"real local batch".to_vec());
    let digest = blobs
        .put(bytes.clone(), Vec::new())
        .await?
        .verified()
        .ok_or("original publication is known")?
        .digest;
    for read in [false, true] {
        for refused in [true, false] {
            let budget = Arc::new(Budget::new(u64::MAX));
            if refused {
                // The first member submits real I/O. Later members refuse
                // while its completion still fences ordered batch delivery.
                *(if read { &budget.reads } else { &budget.writes })
                    .lock()
                    .unwrap() = Some(1);
            }
            let meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = budget.clone();
            let causes = if read {
                blobs
                    .get_batch_native(
                        crate::ObjectsNativeRequest::new(
                            vec![(digest, ByteRange::FULL, bytes.len() as u64); 4],
                            (),
                            &meter,
                        )?,
                        Arc::clone(&meter),
                    )?
                    .await?
                    .map(|result| result.map(|object| assert_eq!(object.body(), &bytes)))
                    .collect::<Vec<_>>()
            } else {
                blobs
                    .put_batch_native(
                        crate::ObjectsNativeRequest::new(
                            super::sealed(
                                (0..4)
                                    .map(|member| {
                                        (vec![u8::from(refused), member].into(), Vec::new())
                                    })
                                    .collect(),
                            ),
                            (),
                            &meter,
                        )?,
                        Arc::clone(&meter),
                    )?
                    .await?
                    .map(|result| result.map(|receipt| assert!(receipt.put().verified().is_some())))
                    .collect::<Vec<_>>()
            };
            for (member, result) in causes.into_iter().enumerate() {
                if refused {
                    let cause = result.err().map(|error| error.into_cause());
                    assert!(
                        matches!(&cause, Some(crate::NativeObjectsCause::Admission(error)) if error.kind() == std::io::ErrorKind::QuotaExceeded),
                        "original member refusal: read={read} member={member} {cause:?}"
                    );
                } else {
                    result?;
                }
            }
            assert_eq!(budget.held.lock().unwrap().0, 0, "member custody retained");
        }
    }
    Ok(())
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
        inner: store.medium(),
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
