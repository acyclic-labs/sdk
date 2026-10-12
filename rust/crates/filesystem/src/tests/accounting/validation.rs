use super::*;

#[tokio::test]
async fn accounting_requires_original_objects_and_authoritative_time_not_offline_defaults()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "fail-closed-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "original").await?;
    workspace
        .write_text("/file", "different current head")
        .await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    // A different actual Objects provider has none of the original closure.
    let (absent, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let absent = crate::LogicalObjectStore::new(Arc::new(absent), bucket);
    assert!(matches!(
        accounting_page(
            &authority,
            &absent,
            authority_id,
            AccountingCursor::genesis(),
            2,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    // Simulate collection of old closures while preserving the complete current
    // closure in an actual independent Objects provider.
    let latest = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    let current = crate::kernel::prove_generation_closure(
        &objects,
        root(latest),
        crate::kernel::ClosureLimits {
            decode: crate::kernel::DecodeLimits::default(),
            maximum_objects: 4096,
            maximum_files: 4096,
            maximum_object_bytes: 16 * 1024 * 1024,
            profile: crate::model::FilesystemProfile::Portable,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
        },
        BUDGET,
        &cancellation,
    )
    .await?;
    for object in current.objects {
        let bytes = {
            let authority_input_1 = 16 * 1024 * 1024;

            let mut __object_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __object_result = crate::AsyncObjectStore::read(
                &objects,
                object,
                authority_input_1,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }?
        .value
        .bytes;
        {
            let mut __object_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __object_result =
                crate::AsyncObjectStore::put(&absent, object, bytes, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?;
    }
    let durable = records(&authority, authority_id).await?;
    let cursor = |record: &DurableCommit| -> Result<AccountingCursor, Box<dyn Error>> {
        Ok(AccountingCursor {
            sequence: record.sequence,
            digest: record.digest,
            settled_at_micros: record.settled_at_micros.ok_or("Stream timestamp missing")?,
        })
    };
    let present = accounting_page(
        &authority,
        &absent,
        authority_id,
        cursor(&durable[1])?,
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert!(present.complete);
    assert_eq!(
        present.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(latest),
            logical_regular_file_bytes: 22,
        }
    );
    assert!(matches!(
        accounting_page(
            &authority,
            &absent,
            authority_id,
            cursor(&durable[0])?,
            1,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    let initial = records(&authority, authority_id).await?.remove(0);
    let offline = MemoryAuthorityStore::default();
    let CreateAuthorityOutcome::Created(head) = ({
        let authority_input_1 = initial.epoch;
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work = crate::AuthorityWork::new(BUDGET, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::create_authority(
            &offline,
            authority_id,
            authority_input_1,
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("offline fixture authority already existed".into());
    };
    let AppendOutcome::Committed(committed) = ({
        let authority_input_1 = initial.epoch;
        let authority_input_3 = ProposedCommit {
            operation_id: initial.operation_id,
            fingerprint: initial.fingerprint,
            payload: initial.payload,
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work = crate::AuthorityWork::new(BUDGET, authority_cancellation);
        let __authority_result = AsyncAuthorityStore::compare_and_append(
            &offline,
            crate::GuardedAppend {
                authority_id: authority_id,
                epoch: authority_input_1,
                expected: head,
                commit: authority_input_3,
                permit: crate::PublicationPermit::Unrestricted,
            },
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))
        .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("offline actual Memory authority did not commit".into());
    };
    assert_eq!(committed.settled_at_micros, None);
    assert!(matches!(
        accounting_page(
            &offline,
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            1,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::MissingTime)
    ));
    Ok(())
}

#[tokio::test]
async fn accounting_rejects_malformed_cursor_and_unbounded_admission() -> Result<(), Box<dyn Error>>
{
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "cursor-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "a").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority,
        &objects,
        authority_id,
        AccountingCursor::genesis(),
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    for cursor in [
        AccountingCursor {
            digest: Digest::ZERO,
            ..first.next
        },
        AccountingCursor {
            settled_at_micros: first.next.settled_at_micros + 1,
            ..first.next
        },
        AccountingCursor {
            sequence: Sequence::new(99),
            ..first.next
        },
    ] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                cursor,
                1,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidHistory)
        ));
    }
    for cursor in [
        AccountingCursor {
            digest: first.next.digest,
            ..AccountingCursor::genesis()
        },
        AccountingCursor {
            settled_at_micros: 1,
            ..AccountingCursor::genesis()
        },
    ] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                cursor,
                1,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    for maximum in [0, 257] {
        assert!(matches!(
            accounting_page(
                &authority,
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                maximum,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    assert!(matches!(
        accounting_page(
            &authority,
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await,
        Err(AccountingError::InvalidBounds)
    ));
    Ok(())
}

#[tokio::test]
async fn accounting_checks_actual_replayed_digest_operation_and_time_before_producing_facts()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "damaged-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "actual").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let durable = records(&authority, authority_id).await?;
    let later = durable
        .last()
        .ok_or("actual records missing")?
        .settled_at_micros
        .ok_or("Stream timestamp missing")?
        + 1;
    let cancellation = CancellationToken::new();
    for damage in [
        ReplayDamage::Digest,
        ReplayDamage::Operation,
        ReplayDamage::RegressingTime(later),
    ] {
        assert!(matches!(
            accounting_page(
                &DamagedReplay {
                    inner: &authority,
                    damage
                },
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                2,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::InvalidHistory)
        ));
    }
    for damage in [ReplayDamage::MissingTime, ReplayDamage::ZeroTime] {
        assert!(matches!(
            accounting_page(
                &DamagedReplay {
                    inner: &authority,
                    damage
                },
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                2,
                BUDGET,
                &cancellation,
            )
            .await,
            Err(AccountingError::MissingTime)
        ));
    }
    Ok(())
}
