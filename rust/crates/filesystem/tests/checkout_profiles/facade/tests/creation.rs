use acyclic_fs::{CancellationToken, Fs, FsError};
use acyclic_fs_format::identity::VolumeId;
use acyclic_fs_format::model::Lifecycle;
use acyclic_fs_work::performance::WorkCounters;

use super::B;
use super::checkout_fixture::config;
use super::fixtures::TestResult;

#[tokio::test]
async fn unsupported_semantics_reject_before_backend_work() -> TestResult {
    let cancel = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let failure = Fs::memory()
        .create_volume_with_id(VolumeId::from_bytes([43; 16]), durable, B, &cancel)
        .await
        .err()
        .ok_or("durable memory volume succeeded")?;
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}
