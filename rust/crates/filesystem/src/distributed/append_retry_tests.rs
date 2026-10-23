use super::*;
use acyclic_stream::{MemoryStream, StreamProvider as _};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Race {
    store: Arc<StreamAuthorityStore<MemoryStream>>,
    authority: AuthorityId,
    expected: Head,
    gate_only: bool,
    fired: bool,
}

fn proposed(id: u8) -> ProposedCommit {
    ProposedCommit {
        operation_id: OperationId::from_bytes([id; 16]),
        fingerprint: Digest::from_bytes([id; 32]),
        payload: Bytes::from_static(b"publication"),
    }
}

impl AuthorityGuard for Race {
    type Error = AuthorityStoreError;

    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        if self.fired {
            return Ok(());
        }
        self.fired = true;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        if self.gate_only {
            let ReservationOutcome::Reserved(reservation) = self
                .store
                .reserve_publication(
                    self.authority,
                    self.expected,
                    OperationId::from_bytes([4; 16]),
                    &mut work,
                )
                .await?
            else {
                return Err(AuthorityStoreError::RejectedStatic("race reservation denied").into());
            };
            self.store
                .release_publication(reservation, &mut work)
                .await?;
        } else {
            let outcome = self
                .store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: self.authority,
                        epoch: self.expected.epoch,
                        expected: self.expected,
                        commit: proposed(4),
                        permit: PublicationPermit::Unrestricted,
                    },
                    &mut work,
                    None::<&mut ()>,
                )
                .await?;
            assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
        }
        Ok(())
    }
}

async fn retry_after_conflict(native: bool, gate_only: bool) -> TestResult {
    let store = Arc::new(StreamAuthorityStore::new(Arc::new(MemoryStream::default())));
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(1 << 28, 1 << 28),
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let mut work = if native {
        AuthorityWork::native(&original, &cancellation)
    } else {
        AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
    };
    let authority = AuthorityId::from_bytes([1; 16]);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let expected = store.head(authority, &mut work).await?;
    let mut request = crate::GuardedAppend {
        authority_id: authority,
        epoch: expected.epoch,
        expected,
        commit: proposed(3),
        permit: PublicationPermit::Unrestricted,
    };
    let mut race = Race {
        store: store.clone(),
        authority,
        expected,
        gate_only,
        fired: false,
    };
    let first = store
        .compare_and_append(request.clone(), &mut work, Some(&mut race))
        .await?;
    assert!(race.fired);
    assert!(if gate_only {
        matches!(first, FsAppendOutcome::Fenced { .. })
    } else {
        matches!(first, FsAppendOutcome::Conflict { .. })
    });
    assert!(
        store
            .find_operation(authority, request.commit.operation_id, &mut work)
            .await?
            .is_none()
    );
    request.expected = store.head(authority, &mut work).await?;
    assert_eq!(request.expected == expected, gate_only);
    assert!(matches!(
        store
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::Committed(_)
    ));
    // A fresh store follows the atomic operation locator after the failed attempt.
    let reopened = StreamAuthorityStore::new(store.provider());
    assert!(
        reopened
            .find_operation(authority, request.commit.operation_id, &mut work)
            .await?
            .is_some()
    );
    // Recovery remains keyed by the logical operation, even with a stale head.
    request.expected = expected;
    assert!(matches!(
        reopened
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::AlreadyCommitted(_)
    ));
    request.commit.fingerprint = Digest::from_bytes([5; 32]);
    assert!(matches!(
        reopened
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::IdempotencyConflict { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn ordinary_append_retries_after_a_head_conflict() -> TestResult {
    retry_after_conflict(false, false).await
}

#[tokio::test]
async fn native_append_retries_after_a_head_conflict() -> TestResult {
    retry_after_conflict(true, false).await
}

#[tokio::test]
async fn ordinary_append_retries_after_a_gate_conflict_without_head_change() -> TestResult {
    retry_after_conflict(false, true).await
}

#[tokio::test]
async fn native_append_retries_after_a_gate_conflict_without_head_change() -> TestResult {
    retry_after_conflict(true, true).await
}

struct Overlap(Race);

impl AuthorityGuard for Overlap {
    type Error = AuthorityStoreError;

    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        if self.0.fired {
            return Ok(());
        }
        self.0.fired = true;
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut nested = Race {
            store: self.0.store.clone(),
            authority: self.0.authority,
            expected: self.0.expected,
            gate_only: true,
            fired: false,
        };
        let mut commit = proposed(3);
        commit.fingerprint = Digest::from_bytes([6; 32]);
        let outcome = self
            .0
            .store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: self.0.authority,
                    epoch: self.0.expected.epoch,
                    expected: self.0.expected,
                    commit,
                    permit: PublicationPermit::Unrestricted,
                },
                &mut work,
                Some(&mut nested),
            )
            .await?;
        assert!(matches!(outcome, FsAppendOutcome::Fenced { .. }));
        Ok(())
    }
}

#[tokio::test]
async fn overlapping_failed_fingerprints_do_not_poison_publication() -> TestResult {
    for native in [false, true] {
        let store = Arc::new(StreamAuthorityStore::new(Arc::new(MemoryStream::default())));
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::performance::work_budget(1 << 28, 1 << 28),
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        let authority = AuthorityId::from_bytes([1; 16]);
        store
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let expected = store.head(authority, &mut work).await?;
        let request = crate::GuardedAppend {
            authority_id: authority,
            epoch: expected.epoch,
            expected,
            commit: proposed(3),
            permit: PublicationPermit::Unrestricted,
        };
        let mut overlap = Overlap(Race {
            store: store.clone(),
            authority,
            expected,
            gate_only: true,
            fired: false,
        });
        assert!(matches!(
            store
                .compare_and_append(request.clone(), &mut work, Some(&mut overlap))
                .await?,
            FsAppendOutcome::Fenced { .. }
        ));
        assert!(matches!(
            store
                .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::Committed(_)
        ));
        assert!(matches!(
            StreamAuthorityStore::new(store.provider())
                .compare_and_append(request, &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::AlreadyCommitted(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn a_malformed_operation_locator_is_still_corrupt() -> TestResult {
    for native in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let store = Arc::new(StreamAuthorityStore::new(stream.clone()));
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::performance::work_budget(1 << 28, 1 << 28),
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        let authority = AuthorityId::from_bytes([1; 16]);
        store
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        stream
            .append(acyclic_stream::AppendRequest {
                path: work.operation_path(authority, proposed(3).operation_id)?,
                records: vec![acyclic_stream::OwnedBytes::from_static(b"foreign").into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        assert!(matches!(
            store
                .find_operation(authority, proposed(3).operation_id, &mut work)
                .await,
            Err(AuthorityStoreError::Corrupt(_)) | Err(AuthorityStoreError::CorruptStatic(_))
        ));
    }
    Ok(())
}
