use super::*;
use acyclic_stream::MemoryStream;
use std::sync::Arc;

fn state(id: WorkspaceId, revision: u64, branch: &str) -> GitCompatState {
    let mut state = GitCompatState::new(branch, id);
    state.revision = revision;
    state
}

#[tokio::test]
async fn independent_hosts_share_conditional_state_without_crossing_repositories()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let first = StreamGitCompatStore::new(StreamAuthorityStore::new(provider.clone()));
    let second = StreamGitCompatStore::new(StreamAuthorityStore::new(provider.clone()));
    let id = WorkspaceId::from_bytes([1; 16]);
    let foreign = WorkspaceId::from_bytes([2; 16]);
    assert!(GitCompatStore::load(&first, id).await?.is_none());
    let initial = state(id, 1, "main");
    assert!(GitCompatStore::compare_and_swap(&first, id, 0, initial.clone()).await?);
    assert_eq!(GitCompatStore::load(&second, id).await?, Some(initial));
    assert!(GitCompatStore::load(&second, foreign).await?.is_none());

    let (left, right) = futures::join!(
        GitCompatStore::compare_and_swap(&first, id, 1, state(id, 2, "left")),
        GitCompatStore::compare_and_swap(&second, id, 1, state(id, 2, "right")),
    );
    assert_ne!(left?, right?);
    let reopened = StreamGitCompatStore::new(StreamAuthorityStore::new(provider));
    let committed = GitCompatStore::load(&reopened, id)
        .await?
        .ok_or("missing committed state")?;
    assert_eq!(committed.revision, 2);
    assert!(["left", "right"].contains(&committed.current_branch.as_str()));
    assert!(!GitCompatStore::compare_and_delete(&second, id, 1).await?);
    assert_eq!(GitCompatStore::load(&first, id).await?, Some(committed));
    assert!(GitCompatStore::compare_and_delete(&second, id, 2).await?);
    assert!(GitCompatStore::load(&reopened, id).await?.is_none());
    assert!(!GitCompatStore::compare_and_swap(&first, id, 2, state(id, 3, "stale")).await?);
    assert!(
        GitCompatStore::compare_and_swap(&first, foreign, 0, state(foreign, 1, "foreign")).await?
    );
    assert!(GitCompatStore::load(&reopened, id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn malformed_record_refuses_instead_of_becoming_absent()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let store = StreamGitCompatStore::new(authority.clone());
    let id = WorkspaceId::from_bytes([3; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let observed = authority
        .observe_record(
            StreamGitCompatStore::<MemoryStream>::path(id, "refs")?,
            &mut work,
        )
        .await?;
    let mut commit = FilesystemCommit::new(CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(bytes::Bytes::from_static(
            b"malformed-state",
        ))?,
        conditions: Vec::new(),
        mutations: Vec::new(),
        deadline_unix_millis: None,
    });
    authority.contribute_record(
        &mut commit,
        &observed.observation,
        &StateRecord::Companion {
            kind: CompanionKind::GitRefs,
            payload: bytes::Bytes::from_static(b"invalid JSON").into(),
        },
        crate::PublicationHold::none(),
        &mut work,
    )?;
    authority.seal_observed(&mut commit, &[], None, &mut work)?;
    assert!(matches!(
        authority.submit(commit, &mut work).await?,
        FilesystemCommitOutcome::Committed(_)
    ));
    assert!(matches!(
        GitCompatStore::load(&store, id).await,
        Err(StreamGitCompatStoreError::Codec(_))
    ));
    assert!(
        GitCompatStore::compare_and_swap(&store, id, 0, state(id, 1, "overwrite"))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn persisted_lineage_resolution_requires_original_read_admission()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let fs = crate::Fs::memory();
    let root = fs
        .create_workspace("persisted-admission-root", budget, &cancellation)
        .await?
        .value;
    let (provider, _) = fs.authority_records(root.id().volume_id())?;
    let graph = crate::WorkspaceGraph::new(StreamGitCompatStore::new(StreamAuthorityStore::new(
        provider.clone(),
    )));
    graph.register_root(&root, budget, &cancellation).await?;
    let child = graph
        .fork(
            &root,
            "persisted-admission-child",
            crate::IdempotencyKey::new(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let grandchild = graph
        .fork(
            &child,
            "persisted-admission-grandchild",
            crate::IdempotencyKey::new(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    drop(graph);
    // Recreate the shipped adapter over the same provider and production wire
    // records. No test adapter can substitute its own accounting behavior.
    let reopened = crate::WorkspaceGraph::new(StreamGitCompatStore::new(
        StreamAuthorityStore::new(provider),
    ));
    let denied = WorkBudget {
        backend_read_operations: 0,
        ..budget
    };
    fn read_refusal(error: &crate::WorkspaceLineageError<StreamGitCompatStoreError>) -> bool {
        let crate::WorkspaceLineageError::Store(StreamGitCompatStoreError::Authority(error)) =
            error
        else {
            return false;
        };
        let work = match error {
            AuthorityStoreError::Work(error) => error,
            #[cfg(not(target_arch = "wasm32"))]
            AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Work(
                error,
            )) => error,
            _ => return false,
        };
        matches!(
            work,
            crate::WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                ..
            }
        )
    }
    let mut ordinary_refused = [false; 3];
    #[cfg(not(target_arch = "wasm32"))]
    let mut native_refused = [false; 3];
    for (index, id) in [root.id(), child.id(), grandchild.id()]
        .into_iter()
        .enumerate()
    {
        let result = reopened.resolve(id, denied, &cancellation).await;
        ordinary_refused[index] = result
            .as_ref()
            .err()
            .is_some_and(|failure| read_refusal(&failure.error));
        if let Err(failure) = result {
            assert_eq!(failure.work.backend_read_operations, 0);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let original = crate::NativeMaterializationWork::new(denied, &cancellation)
                .map_err(|(error, _)| error)?;
            let result = reopened.resolve(id, &original, &cancellation).await;
            native_refused[index] = result
                .as_ref()
                .err()
                .is_some_and(|error| read_refusal(error));
            assert_eq!(original.receipt().backend_read_operations, 0);
        }
    }
    assert_eq!(
        ordinary_refused, [true; 3],
        "persisted lineage escaped original ordinary read admission"
    );
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(
        native_refused, [true; 3],
        "persisted lineage escaped original native read admission"
    );
    Ok(())
}
