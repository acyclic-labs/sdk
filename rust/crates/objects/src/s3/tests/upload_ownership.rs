use super::recording::Recording;
use super::*;
use crate::tests::budget::Budget;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use object_store::PutPayload;
use std::sync::atomic::Ordering;

fn body(
    length: usize,
    native: bool,
    budget: &Arc<Budget>,
) -> Result<OwnedBytes, Box<dyn std::error::Error>> {
    let data = vec![37; length + 4];
    let bytes = if native {
        let meter: Arc<dyn NativeReadMeter> = budget.clone();
        let allocation = ReadAllocation::new(&meter, data.capacity())?;
        OwnedBytes::from_vec_with_guard(data, allocation, meter)
            .map_err(|failure| format!("{:?}", failure.into_error()))?
    } else {
        data.into()
    };
    // Both the backing and its selection must survive transport conversion.
    Ok(bytes.slice(1..length + 3).slice(1..length + 1))
}

fn transferred(payloads: &[PutPayload], pointer: *const u8, length: usize) -> bool {
    let mut offset = 0;
    for payload in payloads {
        let [part] = payload.as_ref() else {
            return false;
        };
        if part.as_ptr() != pointer.wrapping_add(offset) || part.iter().any(|byte| *byte != 37) {
            return false;
        }
        offset += part.len();
    }
    offset == length
}

#[tokio::test]
async fn uploads_retain_original_selection_credit_and_admission()
-> Result<(), Box<dyn std::error::Error>> {
    const PART: usize = 5 * 1024 * 1024;
    for native in [false, true] {
        for length in [1024, 1024 * 1024, PART + 17] {
            let bucket = Arc::new(Recording::default());
            let medium = S3Medium::new(bucket.clone(), "ownership", Capabilities::REQUIRED)?;
            let budget = Arc::new(Budget::new(u64::MAX));
            let original = body(length, native, &budget)?;
            let pointer = original.as_ptr();
            let held = budget.live();
            if length > PART {
                // No HTTP completion is fabricated. The real provider accepts
                // both parts; unavailable signing then ends the original upload.
                assert!(matches!(
                    medium.create("value", original).await,
                    Err(MediumError::Failed(_))
                ));
                assert_eq!(bucket.starts.load(Ordering::SeqCst), 1);
                assert_eq!(bucket.aborts.load(Ordering::SeqCst), 1);
                assert!(bucket.inner.head(&medium.path("value")).await.is_err());
            } else {
                assert!(medium.create("value", original).await?);
                let observed = medium.observe("value".into()).await?.ok_or("absent")?;
                assert_eq!(observed.bytes.as_ptr(), pointer);
                drop(observed);
            }
            let uploads = bucket.take();
            assert_eq!(uploads.len(), if length > PART { 2 } else { 1 });
            assert!(transferred(&uploads, pointer, length));
            if length > PART {
                assert_eq!(uploads[0].content_length(), PART);
                assert_eq!(uploads[1].content_length(), 17);
            }
            if native {
                assert!(held > length as u64);
                assert!(budget.live() > held);
            }
            budget.cancel();
            assert!(transferred(&uploads, pointer, length));
            drop(uploads);
            if length <= PART {
                bucket.inner.delete(&medium.path("value")).await?;
            }
            assert_eq!(budget.live(), 0);
        }

        let bucket = Arc::new(Recording::default());
        let medium = S3Medium::new(bucket.clone(), "replace", Capabilities::REQUIRED)?;
        assert!(
            medium
                .create("value", OwnedBytes::from_static(b"old"))
                .await?
        );
        drop(bucket.take());
        let observed = medium.observe("value".into()).await?.ok_or("absent")?;
        let budget = Arc::new(Budget::new(u64::MAX));
        let original = body(1024, native, &budget)?;
        let pointer = original.as_ptr();
        assert!(
            medium
                .replace(observed.observation, original)
                .await?
                .is_some()
        );
        let uploads = bucket.take();
        assert_eq!(uploads.len(), 1);
        assert!(transferred(&uploads, pointer, 1024));
        let read = medium.read("value", None).await?.ok_or("absent")?;
        assert_eq!(read.as_ptr(), pointer);
        budget.cancel();
        drop(uploads);
        bucket.inner.delete(&medium.path("value")).await?;
        if native {
            assert!(budget.live() > 0, "the escaped read still owns its backing");
        }
        drop(read);
        assert_eq!(budget.live(), 0);

        // Restore the former full copy through the actual upload path. The
        // same pointer oracle must reject it while contents still agree.
        let budget = Arc::new(Budget::new(u64::MAX));
        let original = body(1024, native, &budget)?;
        let pointer = original.as_ptr();
        assert!(
            medium
                .create("copied", OwnedBytes::copy_from_slice(&original))
                .await?
        );
        let uploads = bucket.take();
        assert!(!transferred(&uploads, pointer, 1024));
        assert_eq!(uploads[0].as_ref()[0].as_ref(), original.as_slice());
    }

    // Refusal before transport-owner admission must not enter either
    // provider upload API, including multipart creation.
    for operation in ["create", "replace", "multipart"] {
        for cancelled in [false, true] {
            let bucket = Arc::new(Recording::default());
            let medium = S3Medium::new(bucket.clone(), "refusal", Capabilities::REQUIRED)?;
            assert!(
                medium
                    .create("value", OwnedBytes::from_static(b"old"))
                    .await?
            );
            drop(bucket.take());
            let observed = medium.observe("value".into()).await?.ok_or("absent")?;
            let length = if operation == "multipart" {
                PART + 1
            } else {
                1024
            };
            let limit = if cancelled {
                u64::MAX
            } else {
                (length + 4) as u64 + OwnedBytes::vec_guard_header_bytes::<ReadAllocation>()
            };
            let budget = Arc::new(Budget::new(limit));
            let original = body(length, true, &budget)?;
            if cancelled {
                budget.cancel();
            }
            let result = if operation == "replace" {
                medium
                    .replace(observed.observation, original)
                    .await
                    .map(|_| ())
            } else {
                medium.create("refused", original).await.map(|_| ())
            };
            let Err(MediumError::Failed(error)) = result else {
                panic!("refused admission must preserve its no-effect failure");
            };
            assert_eq!(
                error.kind(),
                if cancelled {
                    std::io::ErrorKind::Interrupted
                } else {
                    std::io::ErrorKind::OutOfMemory
                }
            );
            assert!(bucket.take().is_empty());
            assert_eq!(bucket.starts.load(Ordering::SeqCst), 0);
            assert_eq!(bucket.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(
                medium.read("value", None).await?.unwrap().as_slice(),
                b"old"
            );
            assert!(medium.read("refused", None).await?.is_none());
            assert_eq!(budget.live(), 0);
        }
    }
    Ok(())
}
