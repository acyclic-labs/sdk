use super::*;

#[test]
fn v2_project_merge_receipt_fixture_round_trips_canonically() -> Result<()> {
    let fixture = include_str!("../../fixtures/v2/project-merge-receipt.json").trim();
    let receipt: ProjectMergeReceipt =
        serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
    let seed: ForkSeed = serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    receipt.validate(&seed)?;
    assert_eq!(
        serde_json::to_string(&receipt).map_err(|error| Error::Invalid(error.to_string()))?,
        fixture
    );
    let mut forged = receipt.clone();
    forged.child.id = "other-child".into();
    assert!(forged.validate(&seed).is_err());
    let mut unsafe_proof = receipt.provider_proof;
    unsafe_proof.statement = serde_json::json!({"unsafe": 9_007_199_254_740_992_u64});
    assert!(unsafe_proof.validate(&mut None).is_err());
    Ok(())
}

fn field_copy_receipt() -> Result<ProjectMergeReceipt> {
    let mut receipt: ProjectMergeReceipt =
        serde_json::from_str(include_str!("../../fixtures/v2/project-merge-receipt.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    receipt.provider_proof.statement = serde_json::json!({
        "state": [null, true, "original-state-ref", {"revision": 9_007_199_254_740_991_u64}],
        "notice": {"original": false, "count": -1},
    });
    receipt.notice.attachments = crate::conversation::ReferencedAttachments::Inline {
        items: vec![crate::conversation::Attachment {
            file: receipt.notice.content.clone(),
            label: Some("original notice attachment".into()),
        }],
    };
    receipt
        .notice
        .extensions
        .insert("merge-proof".into(), receipt.notice.content.clone());
    receipt.validate_shape(&mut None)?;
    Ok(receipt)
}

#[cfg(feature = "filesystem")]
fn field_copy_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        allocation_operations: 100_000,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        items_examined: 100_000,
        ..acyclic_fs::WorkBudget::default()
    }
}

#[cfg(feature = "filesystem")]
#[test]
fn owned_merge_copy_uses_physical_fields_and_finite_inspection() -> Result<()> {
    use crate::store::history::{HistoryIo as _, HistoryWork};
    let receipt = field_copy_receipt()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut io = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        field_copy_budget(),
        &cancellation,
    )?);
    let (copied, retained) =
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut io), |quota| receipt.copy(quota))?;
    assert_eq!(copied, receipt);
    assert!(retained > 0);
    drop(copied);
    io.release_owned(retained)?;
    let observed = io.finish(Ok(())).expect("physical owned receipt");
    assert!(observed.work.bytes_copied > 0);
    let mut denied = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        acyclic_fs::WorkBudget {
            items_examined: 0,
            ..field_copy_budget()
        },
        &cancellation,
    )?);
    assert!(receipt.validate_shape(&mut Some(&mut denied)).is_err());
    let mut partial =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::WorkBudget {
                bytes_copied: receipt.child.id.len() as u64,
                ..field_copy_budget()
            },
            &cancellation,
        )?);
    assert!(
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut partial), |quota| receipt
            .copy(quota))
        .is_err()
    );
    partial.admit_owned(field_copy_budget().peak_allocation_bytes)?;
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_merge_copy_transfers_original_custody_and_static_malformed_failures() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let receipt = field_copy_receipt()?;
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(field_copy_budget(), &cancellation)
        .expect("original merge ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original merge observer"),
    );
    let (copied, retained) =
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut io), |quota| receipt.copy(quota))?;
    assert_eq!(copied, receipt);
    let guard = io.into_custody()?;
    assert_eq!(guard.bytes(), retained);
    assert!(guard.work().shares_ledger(&native));
    drop(copied);
    drop(guard);
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native).expect("same merge observer"),
    );
    let mut malformed = receipt.clone();
    malformed.source_project = malformed.target_project.clone();
    assert!(matches!(
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut io), |quota| malformed.copy(quota)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    cancellation.cancel();
    assert!(
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut io), |quota| receipt.copy(quota))
            .is_err()
    );
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_partial_merge_copy_retires_new_fields_without_releasing_existing_custody() -> Result<()> {
    use crate::store::history::HistoryIo as _;
    use acyclic_fs::{
        CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
    };
    let receipt = field_copy_receipt()?;
    let baseline_token = CancellationToken::new();
    let baseline = NativeMaterializationWork::new(field_copy_budget(), &baseline_token)
        .expect("validation-only original ledger");
    let mut baseline_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&baseline).expect("validation observer"),
    );
    receipt.validate_shape(&mut Some(&mut baseline_io))?;
    let validation_copies = baseline.receipt().bytes_copied;
    assert_eq!(baseline_io.into_custody()?.bytes(), 0);
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::WorkBudget {
        bytes_copied: validation_copies + 4 + receipt.child.id.len() as u64,
        ..field_copy_budget()
    };
    let native = NativeMaterializationWork::new(budget, &cancellation)
        .expect("partial merge original ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("partial merge observer"),
    );
    let held = io.clone_text("held")?;
    assert!(matches!(
        crate::wire_codec::CodecQuota::produce(&mut Some(&mut io), |quota| receipt.copy(quota)),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            ..
        })
    ));
    let guard = io.into_custody()?;
    assert_eq!(guard.bytes(), held.capacity() as u64);
    drop(held);
    drop(guard);
    native
        .admit_retained(
            budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    Ok(())
}
