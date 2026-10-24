use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn megabyte_interior_restoration_uses_byte_work_instead_of_item_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("large-interior-restoration"))?;
    let bytes = vec![b'x'; 1_048_576];
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, write("/large", bytes.clone()))?;
    let TransactionCommit::Committed(base) = commit(&mut seed).await? else {
        return Err("seed did not commit".into());
    };
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        transaction.checkout,
        change_content(
            NamespacePath::from_portable(
                &PortablePath::parse("/large", workspace.limits())?,
                workspace.limits()
            )?,
            crate::ContentChange::Write {
                offset: 1,
                bytes: OwnedBytes::from(bytes[1..bytes.len() - 1].to_vec())
            },
            crate::ContentTimes::Preserve
        )
    )?;
    let TransactionCommit::Committed(current) = commit(&mut transaction).await? else {
        return Err("range write did not commit".into());
    };
    // The former item-per-byte charge fails this production operation under
    // the unchanged boundary budget, despite ample byte capacity.
    let receipt = current
        .matches_content(
            &base,
            "/large",
            crate::performance::BOUNDARY_BUDGET,
            &CancellationToken::new(),
        )
        .await?;
    assert!(receipt.value);
    assert!(receipt.work.items_examined < crate::performance::BOUNDARY_BUDGET.items_examined);
    assert!(receipt.work.source_bytes_read >= 2 * (bytes.len() as u64 - 2));
    let denied = current
        .matches_content(
            &base,
            "/large",
            WorkBudget {
                source_bytes_read: 0,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &CancellationToken::new(),
        )
        .await;
    assert!(denied.is_err(), "comparison must retain its byte admission");
    Ok(())
}

#[test]
fn native_range_restoration_compares_bytes_across_payload_layouts() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner =
        proptest::test_runner::TestRunner::new(proptest::test_runner::Config::with_cases(8));
    runner.run(
        &(1_usize..512, any::<u8>(), 1_u8..=255),
        |(past_boundary, byte, change)| {
            let result: Result<(), Box<dyn Error>> = runtime.block_on(async {
                let fs = Fs::memory();
                let workspace = unbounded!(fs, create_workspace("native-content-equality"))?;
                let bytes = vec![byte; 131_072 + past_boundary];
                let changed = byte ^ change;
                let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(seed, write("/large", bytes.clone()))?;
                let TransactionCommit::Committed(base) = commit(&mut seed).await? else {
                    return Err("seed did not commit".into());
                };
                let offset = 65_536 + past_boundary as u64;
                for replacement in [changed, byte] {
                    let mut transaction =
                        unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    // This is the shared shipping entry point used by native mount writes.
                    unbounded!(
                        transaction.checkout,
                        change_content(
                            NamespacePath::from_portable(
                                &PortablePath::parse("/large", workspace.limits())?,
                                workspace.limits()
                            )?,
                            crate::ContentChange::Write {
                                offset,
                                bytes: OwnedBytes::from(vec![replacement]),
                            },
                            crate::ContentTimes::Preserve
                        )
                    )?;
                    let TransactionCommit::Committed(current) = commit(&mut transaction).await?
                    else {
                        return Err("range change did not commit".into());
                    };
                    let equal = unbounded!(current, matches_content(&base, "/large"))?;
                    assert_eq!(equal, replacement == byte);
                    let actual = unbounded!(current, read("/large", bytes.len() as u64))?;
                    let mut expected = bytes.clone();
                    *expected
                        .get_mut(usize::try_from(offset)?)
                        .ok_or("range offset outside fixture")? = replacement;
                    assert_eq!(actual.as_ref(), expected);
                    if replacement == byte {
                        // Negative control: the former payload-identity comparison
                        // rejects these equal bytes after the native range rewrite.
                        let mut selected = unbounded!(
                            workspace,
                            checkout(
                                GenerationSelector::Exact(current.id()),
                                CheckoutMode::read_only_pinned()
                            )
                        )?;
                        let mut original = unbounded!(
                            workspace,
                            checkout(
                                GenerationSelector::Exact(base.id()),
                                CheckoutMode::read_only_pinned()
                            )
                        )?;
                        let path = NamespacePath::from_portable(
                            &PortablePath::parse("/large", workspace.limits())?,
                            workspace.limits(),
                        )?;
                        let selected = unbounded!(selected, lookup_no_follow(&path))?
                            .record
                            .unwrap();
                        let original = unbounded!(original, lookup_no_follow(&path))?
                            .record
                            .unwrap();
                        assert_ne!(selected.payload, original.payload);
                    }
                }
                Ok(())
            });
            result.map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn sparse_zero_representations_compare_by_bytes() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("sparse-content-equality"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, write("/zeros", vec![0; 131_072]))?;
    let TransactionCommit::Committed(base) = commit(&mut seed).await? else {
        return Err("seed did not commit".into());
    };
    let path = NamespacePath::from_portable(
        &PortablePath::parse("/zeros", workspace.limits())?,
        workspace.limits(),
    )?;
    for allocated in [false, true] {
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(
            transaction.checkout,
            change_content(
                &path,
                crate::ContentChange::ZeroRange {
                    range: crate::ByteRange {
                        offset: 65_537,
                        length: 127
                    },
                    allocated,
                    extend: false,
                },
                crate::ContentTimes::Preserve
            )
        )?;
        let TransactionCommit::Committed(current) = commit(&mut transaction).await? else {
            return Err("sparse change did not commit".into());
        };
        assert!(unbounded!(current, matches_content(&base, "/zeros"))?);
    }
    Ok(())
}
