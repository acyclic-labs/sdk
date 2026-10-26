use super::*;

async fn external_composition_policy_fence(seal: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let authority = AuthorityId::from_bytes([101; 16]);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let cut = store.policy_cut(authority, &mut work).await?;
    let effect = StreamPath::new("harness/approval-answer")?;
    let mut commit = crate::FilesystemCommit::new(acyclic_stream::CommitRequest {
        conditions: vec![acyclic_stream::CommitCondition::Absent {
            path: effect.clone(),
        }],
        mutations: vec![acyclic_stream::CommitMutation::Append {
            path: effect.clone(),
            records: vec![b"approved".to_vec().into()],
        }],
        idempotency_key: IdempotencyKey::new(b"approval-composition".to_vec())?,
    });
    if seal {
        cut.scope(async { store.seal(&mut commit, &mut work) })
            .await?;
        assert!(commit.request_mut().is_none());
    }
    // A different policy head wins after the caller's original admission.
    let head = store.head(authority, &mut work).await?;
    let changed = store
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: authority,
                epoch: head.epoch,
                expected: head,
                commit: proposed(102),
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        )
        .await?;
    assert!(matches!(changed, FsAppendOutcome::Committed(_)));
    if seal {
        let request = commit.request().clone();
        let newer = store.policy_cut(authority, &mut work).await?;
        newer
            .scope(async { store.seal(&mut commit, &mut work) })
            .await?;
        assert_eq!(commit.request().conditions, request.conditions);
        assert_eq!(commit.request().idempotency_key, request.idempotency_key);
    }
    let outcome = provider.commit(commit.request().clone()).await?;
    if seal {
        assert!(matches!(outcome, CommitOutcome::Conflict(_)));
        assert_eq!(provider.tail(effect).await, Err(StreamError::NotFound));
        assert!(matches!(
            store.settle(commit, outcome, &mut work).await?,
            crate::FilesystemCommitOutcome::Rejected(_)
        ));
    } else {
        // Negative control: omitting the real seal allows the stale effect.
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
        assert_eq!(provider.tail(effect).await?, 1);
        assert!(matches!(
            store.settle(commit, outcome, &mut work).await,
            Err(AuthorityStoreError::RejectedStatic(
                "filesystem commit was not sealed before submission"
            ))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn external_composition_seal_fences_revocation_and_preserves_retry_identity() -> TestResult {
    external_composition_policy_fence(true).await
}

#[tokio::test]
async fn external_composition_without_seal_is_a_stale_publication_control() -> TestResult {
    external_composition_policy_fence(false).await
}

fn filesystem() -> crate::Fs<StreamAuthorityStore<ReleaseSpy>, crate::MemoryObjectBackend> {
    crate::Fs::new(
        StreamAuthorityStore::new(Arc::new(ReleaseSpy::new(Arc::new(MemoryStream::default())))),
        crate::Fs::memory().objects().clone(),
        crate::EmbeddedCapabilities::MEMORY,
    )
}

async fn fork<A: AsyncAuthorityStore + 'static, O: crate::AsyncObjectStore + 'static>(
    source: &crate::Workspace<A, O>,
    name: &str,
) -> Result<crate::Workspace<A, O>, crate::WorkspaceError> {
    let token = CancellationToken::new();
    let generation = source
        .head(WorkBudget::UNBOUNDED, &token)
        .await
        .map_err(|failure| failure.error)?
        .value;
    source
        .fork::<_, _, crate::WorkspaceError>(
            name,
            crate::ForkOptions::from_generation(generation, crate::IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &token,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}

#[tokio::test]
async fn stream_policy_owner_forks_and_reopens_without_a_process_lease() -> TestResult {
    let fs = filesystem();
    assert_eq!(
        fs.authority().policy_owner(),
        crate::PolicyOwner::AtomicPublication
    );
    assert!(!fs.has_process_policy_owner());
    let token = CancellationToken::new();
    let source = fs
        .create_workspace("source", WorkBudget::UNBOUNDED, &token)
        .await?
        .value;
    source
        .with_stable_policy(WorkBudget::UNBOUNDED, &token, || fork(&source, "child"))
        .await?
        .value?;
    let reopened = crate::Fs::new(
        StreamAuthorityStore::new(fs.authority().provider()),
        fs.objects().clone(),
        fs.capabilities(),
    );
    let child = reopened
        .open_workspace("child", WorkBudget::UNBOUNDED, &token)
        .await?
        .value;
    child.head(WorkBudget::UNBOUNDED, &token).await?;
    Ok(())
}

async fn revocation(omit_fence: bool) -> TestResult {
    let fs = filesystem();
    let token = CancellationToken::new();
    let source = fs
        .create_workspace("source", WorkBudget::UNBOUNDED, &token)
        .await?
        .value;
    let publish = || async {
        let writer = source.clone();
        // A separate task models an independently polling remote policy writer.
        let published = tokio::spawn(async move {
            let outcome = crate::access::fixtures::publish_policy(
                &writer,
                &crate::access::fixtures::denied(crate::access::Action::Read, "**"),
            )
            .await;
            assert!(outcome.is_ok(), "policy writer failed: {outcome:?}");
        })
        .await;
        assert!(
            published.is_ok(),
            "policy writer task failed: {published:?}"
        );
        fork(&source, "revoked-child").await
    };
    let result = if omit_fence {
        publish().await
    } else {
        source
            .with_stable_policy(WorkBudget::UNBOUNDED, &token, publish)
            .await?
            .value
    };
    assert!(
        result.is_err(),
        "fork published after independent source policy revocation"
    );
    assert!(
        fs.open_workspace("revoked-child", WorkBudget::UNBOUNDED, &token)
            .await
            .is_err()
    );
    // A rejected cut leaves no lock. Restore permission and authorize a new
    // operation through the public policy binding before publishing again.
    let allowed = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    crate::access::fixtures::publish_policy(&source, &allowed).await?;
    source
        .with_stable_policy(WorkBudget::UNBOUNDED, &token, || async {
            let bound = source
                .clone()
                .bind_coarse_access(
                    &[
                        crate::access::Action::Read,
                        crate::access::Action::List,
                        crate::access::Action::Fork,
                    ],
                    &token,
                )
                .await?;
            fork(&bound, "new-cut").await
        })
        .await?
        .value?;
    Ok(())
}

#[tokio::test]
async fn independent_source_revocation_fences_stream_fork_atomically() -> TestResult {
    revocation(false).await
}

#[tokio::test]
#[should_panic(expected = "fork published after independent source policy revocation")]
async fn missing_durable_policy_cut_negative_control() {
    let outcome = revocation(true).await;
    assert!(
        outcome.is_ok(),
        "control failed before the publication oracle: {outcome:?}"
    );
}

#[tokio::test]
async fn wire_fork_accepts_stream_backend_policy_owner() -> TestResult {
    use acyclic_fs_wire::wire::filesystem::v2::{
        self as wire, filesystem_service_server::FilesystemService,
    };
    use tonic::Request;
    let fs = filesystem();
    let service = crate::FilesystemWireService::new(fs, crate::FilesystemWireLimits::default())?;
    service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "wire-source".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: Some(wire::OperationOptions {
                idempotency_key: vec![6; 16],
            }),
        }))
        .await?;
    let source = service
        .open_workspace(Request::new(wire::OpenWorkspaceRequest {
            selector: Some(wire::open_workspace_request::Selector::Name(
                "wire-source".into(),
            )),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing source")?
        .head;
    let child = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            source,
            destination_name: "wire-child".into(),
            selection: None,
            operation: Some(wire::OperationOptions {
                idempotency_key: vec![7; 16],
            }),
        }))
        .await?
        .into_inner();
    assert_eq!(child.status, wire::MutationStatus::Committed as i32);
    Ok(())
}

#[tokio::test]
async fn policy_cut_admission_preserves_cancellation_and_budget() -> TestResult {
    let fs = filesystem();
    let token = CancellationToken::new();
    let source = fs
        .create_workspace("admission", WorkBudget::UNBOUNDED, &token)
        .await?
        .value;
    let invoked = std::cell::Cell::new(false);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.authority_records_read = 0;
    let result = source
        .with_stable_policy(budget, &token, || async {
            invoked.set(true);
        })
        .await;
    assert!(result.is_err());
    assert!(!invoked.get());
    token.cancel();
    let result = source
        .with_stable_policy(WorkBudget::UNBOUNDED, &token, || async {
            invoked.set(true);
        })
        .await;
    assert!(result.is_err());
    assert!(!invoked.get());
    Ok(())
}

async fn append_revocation(omit_fence: bool) -> TestResult {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let source = AuthorityId::from_bytes([90; 16]);
    let target = AuthorityId::from_bytes([91; 16]);
    for id in [source, target] {
        store
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
    }
    let cut = store.policy_cut(source, &mut work).await?;
    let source_head = store.head(source, &mut work).await?;
    let target_head = store.head(target, &mut work).await?;
    store
        .compare_and_append(
            crate::GuardedAppend {
                authority_id: source,
                epoch: source_head.epoch,
                expected: source_head,
                commit: proposed(92),
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        )
        .await?;
    let append = store.compare_and_append(
        crate::GuardedAppend {
            authority_id: target,
            epoch: target_head.epoch,
            expected: target_head,
            commit: proposed(93),
            permit: PublicationPermit::Unrestricted,
        },
        &mut work,
        None::<&mut ()>,
    );
    let outcome = if omit_fence {
        append.await?
    } else {
        cut.scope(append).await?
    };
    assert!(
        matches!(outcome, FsAppendOutcome::Conflict { .. }),
        "merge append ignored source policy revocation"
    );
    assert_eq!(store.head(target, &mut work).await?, target_head);
    Ok(())
}

#[tokio::test]
async fn independent_source_revocation_fences_merge_append_atomically() -> TestResult {
    append_revocation(false).await
}

#[tokio::test]
#[should_panic(expected = "merge append ignored source policy revocation")]
async fn missing_merge_policy_cut_negative_control() {
    let outcome = append_revocation(true).await;
    assert!(
        outcome.is_ok(),
        "control failed before the publication oracle: {outcome:?}"
    );
}

#[tokio::test]
async fn a_policy_cut_cannot_authorize_another_streams_owner() -> TestResult {
    let first = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let second = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let authority = AuthorityId::from_bytes([94; 16]);
    for store in [&first, &second] {
        store
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
    }
    let cut = first.policy_cut(authority, &mut work).await?;
    let head = second.head(authority, &mut work).await?;
    let result = cut
        .scope(second.compare_and_append(
            crate::GuardedAppend {
                authority_id: authority,
                epoch: head.epoch,
                expected: head,
                commit: proposed(95),
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        ))
        .await;
    assert!(matches!(
        result,
        Err(AuthorityError::Authority(
            AuthorityStoreError::RejectedStatic(
                "policy cut and publication use different Streams owners"
            )
        ))
    ));
    assert_eq!(second.head(authority, &mut work).await?, head);
    Ok(())
}

async fn same_authority_publication(change_head: bool) -> TestResult {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let authority = AuthorityId::from_bytes([96; 16]);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let cut = store.policy_cut(authority, &mut work).await?;
    let mut head = store.head(authority, &mut work).await?;
    if change_head {
        let outcome = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: authority,
                    epoch: head.epoch,
                    expected: head,
                    commit: proposed(97),
                    permit: PublicationPermit::Unrestricted,
                },
                &mut work,
                None::<&mut ()>,
            )
            .await?;
        assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
        head = store.head(authority, &mut work).await?;
    }
    let outcome = cut
        .scope(store.compare_and_append(
            crate::GuardedAppend {
                authority_id: authority,
                epoch: head.epoch,
                expected: head,
                commit: proposed(98),
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        ))
        .await;
    if change_head {
        assert!(matches!(
            outcome,
            Err(AuthorityError::Authority(
                AuthorityStoreError::RejectedStatic(
                    "policy cut conflicts with the publication condition"
                )
            ))
        ));
        assert_eq!(store.head(authority, &mut work).await?, head);
    } else {
        assert!(matches!(outcome?, FsAppendOutcome::Committed(_)));
        assert_ne!(store.head(authority, &mut work).await?, head);
    }
    Ok(())
}

#[tokio::test]
async fn same_authority_policy_cut_preserves_publication_conditions() -> TestResult {
    same_authority_publication(false).await
}

#[tokio::test]
async fn conflicting_same_authority_policy_cut_rejects_without_publication() -> TestResult {
    same_authority_publication(true).await
}
