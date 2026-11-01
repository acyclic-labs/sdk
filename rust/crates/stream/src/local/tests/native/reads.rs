use super::*;
use crate::{ReadKeySeek, RevisionCut};
use futures::FutureExt as _;

#[tokio::test]
async fn finite_reads_admit_before_revision_waits_and_keep_the_original_producer()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let local_provider = if local {
            Some(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            None
        };
        let (provider, revisions): (Arc<dyn crate::NativeStreamProvider>, _) =
            if let Some(local) = &local_provider {
                (
                    Arc::new(local.clone()),
                    Arc::clone(&local.inner.provider.revisions),
                )
            } else {
                let memory = MemoryStream::default();
                let revisions = Arc::clone(&memory.revisions);
                (Arc::new(memory), revisions)
            };
        provider.append(keyed_append("read/admission", 0)?).await?;
        let mut next = revisions.current();
        next.index += 1;
        let valid = ReadRequest {
            path: StreamPath::new("read/admission")?,
            key: None,
            from: 1,
            limit: 1,
            at_least: RevisionCut::of(next.clone()),
        };
        for mut invalid in [
            ReadRequest {
                limit: 0,
                ..valid.clone()
            },
            ReadRequest {
                limit: u32::MAX,
                ..valid.clone()
            },
            ReadRequest {
                key: Some(ReadKeySeek {
                    key: vec![0; crate::MAX_RECORD_KEY_BYTES + 1].into(),
                    before: 2,
                }),
                ..valid.clone()
            },
        ] {
            // Admission cannot depend on a future, foreign, or closed revision
            // producer. No watch registration or publication guard is needed.
            for foreign in [false, true] {
                let mut cut = next.clone();
                if foreign {
                    cut.incarnation ^= 1;
                }
                invalid.at_least = RevisionCut::of(cut);
                assert!(matches!(
                    provider.read(invalid.clone()).now_or_never(),
                    Some(Err(StreamError::LimitExceeded))
                ));
                let meter = Arc::new(Meter::default());
                assert!(matches!(
                    provider
                        .native_read(invalid.clone(), meter.original())
                        .now_or_never(),
                    Some(Ok(Err(StreamError::LimitExceeded)))
                ));
                assert_eq!(meter.examined.load(Ordering::SeqCst), 0);
                assert_eq!(meter.read.load(Ordering::SeqCst), 0);
                meter.assert_refunded();
            }
        }

        // Healthy requests still park on the original applied-revision event,
        // leaving its satisfying append runnable on the same real provider.
        let meter = Arc::new(Meter::default());
        let mut portable = provider.read(valid.clone());
        let mut native = provider.native_read(valid.clone(), meter.original());
        assert!((&mut portable).now_or_never().is_none());
        assert!((&mut native).now_or_never().is_none());
        assert_eq!(meter.examined.load(Ordering::SeqCst), 0);
        if let Some(local) = &local_provider {
            assert!(local.inner.publication.try_write().is_ok());
        }
        provider.append(keyed_append("read/admission", 1)?).await?;
        let mut portable = portable.await?;
        let expected = portable.next().await.ok_or("missing portable record")??;
        assert_eq!(expected.sequence, 1);
        assert!(portable.next().await.is_none());
        drop(portable);
        let (mut native, allocation) = native.await??.into_parts();
        let actual = native.next().await.ok_or("missing native record")??;
        assert_eq!(actual.value(), &expected);
        drop(actual);
        assert!(native.next().await.is_none());
        drop(native);
        drop(allocation);
        meter.assert_refunded();

        next.index += 1;
        let lost = ReadRequest {
            at_least: RevisionCut::of(next),
            ..valid
        };
        let mut portable = provider.read(lost.clone());
        let mut native = provider.native_read(lost, meter.original());
        assert!((&mut portable).now_or_never().is_none());
        assert!((&mut native).now_or_never().is_none());
        revisions.close();
        assert!(matches!(portable.await, Err(StreamError::Unavailable)));
        assert!(matches!(native.await?, Err(StreamError::Unavailable)));
        meter.assert_refunded();
    }
    Ok(())
}
