//! Focused public whole-file read behavior tests.
use acyclic_fs::model::{Lifecycle, VolumeConfig};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, FsError, IdempotencyKey,
    Transaction, TransactionCommit, WorkBudget, WorkspaceError,
};
use bytes::Bytes;
use std::error::Error;

macro_rules! value {
    ($result:expr) => {
        $result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    };
}

macro_rules! unbounded {
    ($receiver:expr, $method:ident($($arg:expr),* $(,)?)) => {
        value!($receiver.$method($($arg,)* WorkBudget::UNBOUNDED, &CancellationToken::new()).await)
    };
}

fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, E>> {
    std::future::ready(Ok(acyclic_fs::OperationReceipt {
        value: (),
        work: acyclic_fs::WorkCounters::UNCHARGED,
    }))
}

async fn commit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
) -> Result<TransactionCommit<A, O>, WorkspaceError> {
    value!(
        transaction
            .commit::<WorkspaceError, _, _>(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                allow
            )
            .await
    )
}

#[tokio::test]
async fn whole_file_read_resolves_once_and_preserves_admission() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("whole-read"))?;
    let cancellation = CancellationToken::new();
    for length in [0, 5, 300] {
        let bytes = Bytes::from(vec![7; length]);
        workspace.write("/entry", bytes.clone()).await?;
        // Both receipt comparisons start with warm immutable pages. A cold
        // read can fetch pages that the next read finds in the decoded cache.
        let warm = workspace
            .read(
                "/entry",
                length as u64,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(warm.value.as_ref(), bytes.as_ref());
        drop(warm);
        let read = workspace
            .read(
                "/entry",
                length as u64,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let range = workspace
            .read_range(
                "/entry",
                0,
                length as u64,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(read.value.as_ref(), bytes.as_ref());
        assert_eq!(range.value.bytes.as_ref(), bytes.as_ref());
        assert_eq!(read.work.page_reads, range.work.page_reads);
        assert_eq!(
            read.work.backend_read_operations,
            range.work.backend_read_operations
        );
        assert_eq!(read.work.bytes_hashed, range.work.bytes_hashed);
        assert_eq!(read.work.output_bytes, range.work.output_bytes);
        assert_eq!(read.work.bytes_copied, range.work.bytes_copied);
        if length != 0 {
            let denied = workspace
                .read(
                    "/entry",
                    length as u64 - 1,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await;
            assert!(
                matches!(denied, Err(ref failure) if matches!(failure.error, WorkspaceError::ReadLimitExceeded))
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let native = workspace
                .read("/entry", length as u64, &work, &cancellation)
                .await?;
            assert_eq!(native.as_ref(), bytes.as_ref());
            assert_eq!(work.receipt().output_bytes, length as u64);
            drop(native);
            assert_eq!(
                work.live_allocation_bytes(),
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    for (path, expected_missing) in [("/missing", true), ("/", false)] {
        let result = workspace
            .read(path, 64, WorkBudget::UNBOUNDED, &cancellation)
            .await;
        assert!(matches!(result, Err(ref failure) if matches!(
            (&failure.error, expected_missing),
            (WorkspaceError::NotFound, true) | (WorkspaceError::NotRegularFile, false)
        )));
    }
    assert!(
        workspace
            .read("/entry", 300, WorkBudget::default(), &cancellation)
            .await
            .is_err()
    );
    cancellation.cancel();
    assert!(
        workspace
            .read("/entry", 300, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn whole_file_read_preserves_volume_read_limit() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.limits.maximum_read_bytes = 4;
    let workspace = unbounded!(fs, create_workspace_with_config("read-limit", config))?;
    workspace.write_text("/entry", "abc").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, resize("/entry", 5))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let cancellation = CancellationToken::new();
    let error = workspace
        .read("/entry", 5, WorkBudget::UNBOUNDED, &cancellation)
        .await;
    assert!(matches!(error, Err(ref failure) if matches!(
        failure.error,
        WorkspaceError::Filesystem(FsError::FileRead(acyclic_fs::kernel::FileRangeReadError::InvalidRange))
    )));
    assert_eq!(
        unbounded!(workspace, read_range("/entry", 0, 4))?
            .bytes
            .as_ref(),
        b"abc\0"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeMutation {
    Unrelated,
    Metadata,
    Bytes,
}

async fn unchanged_range_case(
    body: Vec<u8>,
    left: u16,
    right: u16,
    mutation: RangeMutation,
) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("unchanged-range"))?;
    workspace
        .write("/entry", Bytes::copy_from_slice(&body))
        .await?;
    let original = unbounded!(workspace, head())?;
    match mutation {
        RangeMutation::Unrelated => {
            workspace
                .write_text("/unrelated", "later generation")
                .await?;
        }
        RangeMutation::Metadata => {
            use acyclic_fs::kernel::{FileMetadata, MetadataField};
            let before = unbounded!(original, stat("/entry"))?;
            let mode = before.metadata.posix_mode.unwrap_or(0o100_644) ^ 0o111;
            let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
            unbounded!(
                transaction,
                set_metadata(
                    "/entry",
                    FileMetadata {
                        posix_mode: MetadataField::Value(mode),
                        ..FileMetadata::default()
                    }
                )
            )?;
            assert!(matches!(
                commit(&mut transaction).await?,
                TransactionCommit::Committed(_)
            ));
            let after = unbounded!(workspace, stat("/entry"))?;
            assert_eq!(after.metadata.posix_mode, Some(mode));
            assert_ne!(before.metadata, after.metadata);
            assert_eq!(before.file_id, after.file_id);
            assert_eq!(before.kind, after.kind);
            assert_eq!(before.logical_bytes, after.logical_bytes);
            assert_eq!(
                unbounded!(workspace, read("/entry", body.len() as u64))?.as_ref(),
                body.as_slice()
            );
        }
        RangeMutation::Bytes => {
            let mut changed = body.clone();
            if let Some(first) = changed.first_mut() {
                *first ^= 0xff;
            } else {
                changed.push(1);
            }
            workspace.write("/entry", Bytes::from(changed)).await?;
        }
    }
    let current = unbounded!(workspace, head())?;
    assert_ne!(original.id(), current.id());
    let left = usize::from(left) % (body.len() + 1);
    let right = usize::from(right) % (body.len() + 1);
    let (start, end) = (left.min(right), left.max(right));
    let range = acyclic_fs::ByteRange {
        offset: start as u64,
        length: (end - start) as u64,
    };
    let selected = current
        .read_range_if_unchanged(
            &original,
            "/entry",
            range,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    if mutation == RangeMutation::Bytes {
        assert!(
            matches!(selected, Err(failure) if matches!(failure.error, WorkspaceError::StaleIdentity))
        );
    } else {
        assert_eq!(selected?.value.bytes.as_ref(), &body[start..end]);
    }
    let pinned = unbounded!(
        original,
        read_range_if_unchanged(&original, "/entry", range)
    )?;
    assert_eq!(pinned.bytes.as_ref(), &body[start..end]);
    Ok(())
}

proptest::proptest! {
    // Metadata-only and unrelated changes preserve every selected range. Any
    // payload change fails, including empty ranges and changes outside the range.
    #[test]
    fn unchanged_payload_ranges_remain_generation_exact(
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
        left in proptest::prelude::any::<u16>(),
        right in proptest::prelude::any::<u16>(),
        mutation in proptest::prop_oneof![
            proptest::strategy::Just(RangeMutation::Unrelated),
            proptest::strategy::Just(RangeMutation::Metadata),
            proptest::strategy::Just(RangeMutation::Bytes),
        ],
    ) {
        let result = futures::executor::block_on(unchanged_range_case(body, left, right, mutation));
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn unchanged_range_allows_metadata_updates_and_rejects_changed_bytes()
-> Result<(), Box<dyn Error>> {
    for body in [Vec::new(), b"payload".to_vec()] {
        for mutation in [RangeMutation::Metadata, RangeMutation::Bytes] {
            unchanged_range_case(body.clone(), 0, u16::MAX, mutation).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn unchanged_range_reads_do_not_copy_the_body() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("large-range"))?;
    workspace
        .write("/entry", Bytes::from(vec![7; 2 * 1024 * 1024]))
        .await?;
    let original = unbounded!(workspace, head())?;
    workspace.write_text("/unrelated", "changed").await?;
    let current = unbounded!(workspace, head())?;
    let selected = current
        .read_range_if_unchanged(
            &original,
            "/entry",
            acyclic_fs::ByteRange {
                offset: 1024,
                length: 16,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(selected.value.bytes.as_ref(), &[7; 16]);
    assert!(
        selected.work.bytes_copied < 64 * 1024,
        "{:?}",
        selected.work
    );
    Ok(())
}

#[tokio::test]
async fn unchanged_range_rejects_a_foreign_deployment() -> Result<(), Box<dyn Error>> {
    let first = Fs::memory();
    let second = Fs::memory();
    let first = unbounded!(first, create_workspace("same-name"))?;
    let second = unbounded!(second, create_workspace("same-name"))?;
    first.write_text("/entry", "same bytes").await?;
    second.write_text("/entry", "same bytes").await?;
    let original = unbounded!(first, head())?;
    let other = unbounded!(second, head())?;
    let result = other
        .read_range_if_unchanged(
            &original,
            "/entry",
            acyclic_fs::ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(result, Err(failure) if matches!(failure.error, WorkspaceError::ForeignGeneration))
    );
    Ok(())
}
