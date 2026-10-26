use super::super::tests::{memory_volume, portable_path, private_checkout};
use super::restore_recovery_tests::FINITE_MATERIALIZATION_BUDGET;
use super::*;

#[tokio::test]
async fn durable_traversal_groups_full_flushes_without_overlapping_staging_actions()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let cancellation = CancellationToken::new();
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let volume = memory_volume(crate::VolumeId::new(), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let width = acyclic_native_runtime::NativeSyncBatch::capacity()?;
    let count = 2 * width + 1;
    let mut expected = Vec::new();
    for index in 0..count {
        let name = format!("file-{index}");
        let bytes = vec![u8::try_from(index)?; if index % 2 == 0 { 16 } else { 8192 }];
        checkout
            .create(
                crate::AuthoredMutation::CreateFile {
                    path: portable_path(config.limits, &[name.as_bytes()])?,
                    bytes: Bytes::from(bytes.clone()).into(),
                    metadata: FileMetadata {
                        modified_ns: crate::kernel::MetadataField::Value(1_700_000_000_123_456_700),
                        ..FileMetadata::default()
                    },
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        expected.push((name, bytes));
    }
    for original in [false, true] {
        for mode in [
            MaterializeMode::DurableOutput,
            MaterializeMode::ReconstructibleView,
        ] {
            verify_materialization(&mut checkout, original, mode, &expected).await?;
        }
    }
    Ok(())
}

async fn verify_materialization<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    original: bool,
    mode: MaterializeMode,
    expected: &[(String, Vec<u8>)],
) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let width = acyclic_native_runtime::NativeSyncBatch::capacity()?;
    let work = crate::NativeMaterializationWork::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
        .map_err(|failure| MaterializeError::from(failure.0))?;
    let output = tempfile::tempdir()?;
    let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)?;
    let mut active = false;
    let mut prior = 0;
    let mut groups = Vec::new();
    let mut action = |phase| {
        match phase {
            NativeMaterializationActionPhase::Before => {
                assert!(!active, "two actions share one staging completion");
                active = true;
            }
            NativeMaterializationActionPhase::Completed => {
                assert!(active, "physical work was never accepted");
                active = false;
                let total = work.receipt().durability_operations;
                if total != prior {
                    groups.push(total - prior);
                }
                prior = total;
            }
        }
        std::future::ready(Ok::<(), MaterializeError>(()))
    };
    let reader = checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&work, &cancellation),
            &cancellation,
        )?
        .into_pinned_reader_with_native_work(&work, &cancellation)?;
    let source = if original {
        GenerationSource::Original(&reader)
    } else {
        GenerationSource::Standalone(checkout)
    };
    materialize_source(
        source,
        &[root],
        &MaterializeOptions::native(output.path()),
        mode,
        GenerationReceipt::from_work(&work)?,
        Some(&mut action),
    )
    .await?;
    assert!(!active);
    if mode == MaterializeMode::DurableOutput {
        assert_eq!(
            groups,
            [width as u64, width as u64, 1],
            "independent full flushes were serialized or omitted"
        );
        assert_eq!(work.receipt().durability_operations, expected.len() as u64);
    } else {
        assert!(groups.is_empty());
        assert_eq!(work.receipt().durability_operations, 0);
    }
    for (name, bytes) in expected {
        let path = output.path().join(name);
        assert_eq!(&std::fs::read(&path)?, bytes);
        assert_eq!(
            std::fs::metadata(path)?
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
            1_700_000_000_123_456_700
        );
    }
    work.receipt().verify(FINITE_MATERIALIZATION_BUDGET)?;
    Ok(())
}
