use super::native_staging_custody_tests::budget;
use super::*;

use acyclic_fs::{
    CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
};
use acyclic_stream::MemoryStream;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn original_corrupt_receipt_drops_source_and_keeps_first_native_quota_cause() -> Result<()> {
    for (fault, deny_received_bytes) in [(4, false), (5, false), (5, true)] {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let cancellation = CancellationToken::new();
        let mut allowance = budget();
        if deny_received_bytes {
            allowance.authority_bytes_read = 0;
        }
        let native = NativeMaterializationWork::new(allowance, &cancellation)
            .expect("original finite malformed receipt ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original immutable source custody"),
        );
        let authority = Authority {
            kind: crate::core::AggregateKind::Agent,
            id: "malformed-immutable-receipt".into(),
        };
        let path = operations::publication_path(
            &authority,
            "harness/v2/aggregate-operations/",
            None,
            &mut Some(&mut io),
        )?;
        let mut source = Vec::new();
        crate::store::history::reserve(&mut source, 4096, &mut io)?;
        io.copy_bytes(9)?;
        source.extend_from_slice(b"immutable");
        provider
            .aggregate_commit_fault
            .store(fault, Ordering::SeqCst);
        let error = stage_immutable_vec_with_work(&client, path, Ok(source), &mut Some(&mut io))
            .await
            .expect_err("actual provider receipt corruption must be rejected");
        if deny_received_bytes {
            assert!(matches!(
                error,
                Error::NativeWork {
                    cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                        counter: "authority_bytes_read",
                        ..
                    }),
                    custody: None,
                }
            ));
        } else {
            assert!(matches!(
                error,
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "immutable record receipt is invalid",
                }
            ));
        }
        assert_eq!(io.into_custody()?.bytes(), 0);
        drop(client);
        drop(provider);
        if !deny_received_bytes {
            assert_eq!(
                native
                    .remaining()
                    .expect("corrupt receipt aliases retire with the provider")
                    .peak_allocation_bytes,
                budget().peak_allocation_bytes
            );
        }
    }
    Ok(())
}
