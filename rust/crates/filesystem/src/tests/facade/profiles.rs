use super::*;

#[tokio::test]
async fn full_case_fold_legacy_alias_collisions_fail_closed() -> TestResult {
    let cancel = CancellationToken::new();
    for prefix in [String::new(), "Q".repeat(120)] {
        let first_name = format!("{prefix}Straße");
        let second_name = format!("{prefix}STRASSE");
        let alias = format!("{prefix}strasse");
        let (mut checkout, [first, second]) =
            crate::facade::test_fixtures::legacy_folded_checkout([&first_name, &second_name])
                .await?;
        let before = checkout_state(&checkout);
        for (name, expected) in [(&first_name, first), (&second_name, second)] {
            assert_eq!(
                checkout
                    .lookup_no_follow(&path(name)?, B, &cancel)
                    .await?
                    .value
                    .record
                    .ok_or("missing exact file")?
                    .file_id,
                expected
            );
        }
        assert!(matches!(
            checkout.lookup_no_follow(&path(&alias)?, B, &cancel).await,
            Err(OperationFailure {
                error: FsError::NameCollision,
                ..
            })
        ));
        assert!(matches!(
            checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::Write {
                        path: path(&alias)?,
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
