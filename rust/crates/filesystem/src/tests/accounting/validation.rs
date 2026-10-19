use super::*;

#[derive(Clone, Copy)]
enum Damage {
    Digest,
    Operation,
    MissingTime,
    ZeroTime,
    RegressingTime(u64),
}

struct DamagedReplay<'a> {
    inner: &'a StreamAuthorityStore<acyclic_stream::MemoryStream>,
    damage: Damage,
}

impl AsyncAuthorityStore for DamagedReplay<'_> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn head(
        &self,
        id: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        self.inner.head(id, work).await
    }
    async fn replay(
        &self,
        id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        let (mut records, allocation) = self.inner.replay(id, after, limit, work).await?;
        for record in &mut records {
            match self.damage {
                Damage::Digest => record.digest = Digest::ZERO,
                Damage::Operation => record.operation_id = OperationId::from_bytes([121; 16]),
                Damage::MissingTime => record.settled_at_micros = None,
                Damage::ZeroTime => record.settled_at_micros = Some(0),
                Damage::RegressingTime(later) if record.sequence == Sequence::new(1) => {
                    record.settled_at_micros = Some(later)
                }
                Damage::RegressingTime(_) => (),
            }
        }
        Ok((records, allocation))
    }
}

#[tokio::test]
async fn cursor_history_time_and_admission_fail_closed() -> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("validated-accounting", B, &cancel)
        .await?
        .value;
    workspace.write_text("/file", "actual").await?;
    let id = volume_authority_id(workspace.id().volume_id());
    let first = page(&authority, &objects, id, AccountingCursor::genesis(), 1).await?;
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
            page(&authority, &objects, id, cursor, 1).await,
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
            page(&authority, &objects, id, cursor, 1).await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    for maximum in [0, 257] {
        assert!(matches!(
            page(
                &authority,
                &objects,
                id,
                AccountingCursor::genesis(),
                maximum
            )
            .await,
            Err(AccountingError::InvalidBounds)
        ));
    }
    assert!(matches!(
        accounting_page(
            &authority,
            &objects,
            id,
            AccountingCursor::genesis(),
            1,
            B,
            &cancel
        )
        .await,
        Err(AccountingError::InvalidBounds)
    ));
    let later = records(&authority, id)
        .await?
        .last()
        .ok_or("records missing")?
        .settled_at_micros
        .ok_or("settlement time missing")?
        + 1;
    for damage in [
        Damage::Digest,
        Damage::Operation,
        Damage::RegressingTime(later),
        Damage::MissingTime,
        Damage::ZeroTime,
    ] {
        let failed = page(
            &DamagedReplay {
                inner: &authority,
                damage,
            },
            &objects,
            id,
            AccountingCursor::genesis(),
            2,
        )
        .await;
        if matches!(damage, Damage::MissingTime | Damage::ZeroTime) {
            assert!(matches!(failed, Err(AccountingError::MissingTime)));
        } else {
            assert!(matches!(failed, Err(AccountingError::InvalidHistory)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn current_objects_and_offline_authority_cannot_substitute_for_original_proof()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("original-proof", B, &cancel)
        .await?
        .value;
    workspace.write_text("/file", "original").await?;
    workspace
        .write_text("/file", "different current head")
        .await?;
    let id = volume_authority_id(workspace.id().volume_id());
    let (empty, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let current_only = crate::LogicalObjectStore::new(Arc::new(empty), bucket);
    assert!(matches!(
        page(
            &authority,
            &current_only,
            id,
            AccountingCursor::genesis(),
            2
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    let latest = root(workspace.head(B, &cancel).await?.value.id());
    let proof = super::archive::prove(&objects, latest, workspace.volume_config()).await?;
    for object in proof.objects {
        let bytes = objects
            .read(
                object,
                16 * 1024 * 1024,
                &mut AuthorityWork::new(BUDGET, &cancel),
            )
            .await?
            .bytes;
        current_only
            .put(object, bytes, &mut AuthorityWork::new(BUDGET, &cancel))
            .await?;
    }
    let durable = records(&authority, id).await?;
    let present = page(&authority, &current_only, id, cursor(&durable[1])?, 1).await?;
    assert!(present.complete);
    assert_eq!(
        present.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: latest,
            logical_regular_file_bytes: 22
        }
    );
    assert!(matches!(
        page(&authority, &current_only, id, cursor(&durable[0])?, 1).await,
        Err(AccountingError::Proof(_))
    ));
    let initial = &durable[0];
    let offline = crate::memory::MemoryAuthorityStore::default();
    offline
        .create_first_record(
            id,
            ProposedCommit {
                operation_id: initial.operation_id,
                fingerprint: initial.fingerprint,
                payload: initial.payload.clone(),
            },
            &mut AuthorityWork::new(BUDGET, &cancel),
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(
        page(&offline, &objects, id, AccountingCursor::genesis(), 1).await,
        Err(AccountingError::MissingTime)
    ));
    Ok(())
}
