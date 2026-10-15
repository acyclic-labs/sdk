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
