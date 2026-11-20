use super::*;
use crate::facade::test_snapshot::checkout_state;
use crate::{FsError, OperationFailure};
use acyclic_stream::OwnedBytes;

const B: WorkBudget = WorkBudget::UNBOUNDED;

fn path(name: &str, checkout: &MemoryCheckout) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        vec![LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec())?],
        checkout.volume_config().limits,
    )?)
}

#[tokio::test]
async fn full_case_fold_legacy_alias_collisions_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    for prefix in [String::new(), "Q".repeat(120)] {
        let first_name = format!("{prefix}Straße");
        let second_name = format!("{prefix}STRASSE");
        let alias = format!("{prefix}strasse");
        let (mut checkout, [first, second]) =
            legacy_folded_checkout([&first_name, &second_name]).await?;
        let before = checkout_state(&checkout);
        for (name, expected) in [(&first_name, first), (&second_name, second)] {
            assert_eq!(
                checkout
                    .lookup_no_follow(&path(name, &checkout)?, B, &cancel)
                    .await?
                    .value
                    .record
                    .ok_or("missing exact file")?
                    .file_id,
                expected
            );
        }
        let alias_path = path(&alias, &checkout)?;
        assert!(matches!(
            checkout.lookup_no_follow(&alias_path, B, &cancel).await,
            Err(OperationFailure {
                error: FsError::NameCollision,
                ..
            })
        ));
        assert!(matches!(
            checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::Write {
                        path: alias_path,
                        offset: 0,
                        bytes: OwnedBytes::from_static(b"bad")
                    }],
                    B,
                    &cancel
                )
                .await,
            Err(OperationFailure {
                error: FsError::NameCollision,
                ..
            })
        ));
        assert_eq!(checkout_state(&checkout), before);
    }
    Ok(())
}
