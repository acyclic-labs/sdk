use super::accounting_tests::{ObservedAuthority, Prefunded, ReplayCustody};
use super::*;
use std::sync::Mutex;

#[tokio::test]
async fn fork_preserves_original_accounting_through_the_authority_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let authority = Arc::new(ObservedAuthority::default());
    let fs = crate::Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let root = fs
        .create_workspace("fork-accounting-root", budget, &cancellation)
        .await?
        .value;
    root.write_text("/source", "original").await?;
    let graph = crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
    for native in [false, true] {
        let registration = crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
        let record = if native {
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Native(original.clone()));
            let record = registration
                .register_root(&root, &original, &cancellation)
                .await?;
            original.receipt().verify(budget)?;
            record
        } else {
            let original = Arc::new(Prefunded {
                budget,
                held: Mutex::new(None),
            });
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Standalone(original.clone()));
            let (result, spent, live, baseline) = registration
                .register_root(&root, original.as_ref(), &cancellation)
                .await;
            spent.verify(budget)?;
            assert!(
                live >= baseline,
                "registration retired earlier original backing"
            );
            result?
        };
        assert!(authority.replay_custody.lock().unwrap().is_none());
        assert_eq!(record.workspace_id, root.id());
        assert_eq!(record.parent_workspace_id, None);
        assert_eq!(record.fork_generation, record.initial_generation);
    }
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let constrained = WorkBudget {
            backend_read_operations: 0,
            ..budget
        };
        let registration = crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
        *authority.replay_minimum.lock().unwrap() = Some(0);
        let failure = registration
            .register_root(&root, constrained, &cancellation)
            .await
            .err()
            .ok_or("registration escaped cancellation or its read allowance")?;
        if cancelled {
            assert!(matches!(
                failure.error,
                crate::WorkspaceLineageError::Workspace(WorkspaceError::Cancelled(_))
            ));
            assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
        } else {
            assert!(matches!(
                failure.error,
                crate::WorkspaceLineageError::Workspace(WorkspaceError::Work(
                    crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }
                ))
            ));
        }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert!(authority.replay_minimum.lock().unwrap().take().is_some());
    }
    let mut workspaces = vec![root];
    for (parent_index, name, native, via_graph) in [
        (0, "left", false, false),
        (0, "right", true, false),
        (1, "grandchild", false, false),
        (2, "lineage-child", false, true),
        (4, "lineage-grandchild", true, true),
    ] {
        let parent = &workspaces[parent_index];
        let generation = parent.head(budget, &cancellation).await?.value;
        let fork_generation = generation.id();
        let options = ForkOptions::from_generation(generation, IdempotencyKey::new());
        let options = if name == "grandchild" {
            options.inherit_paths(vec!["/source".to_owned()])
        } else {
            options
        };
        let child = if native {
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Native(original.clone()));
            let child = if via_graph {
                graph
                    .fork(
                        parent,
                        name,
                        IdempotencyKey::new(),
                        &original,
                        &cancellation,
                    )
                    .await?
            } else {
                parent
                    .fork::<_, _, WorkspaceError>(
                        name,
                        options,
                        crate::PublicationPermit::Unrestricted,
                        &original,
                        &cancellation,
                        (),
                    )
                    .await?
            };
            original.receipt().verify(budget)?;
            child
        } else {
            let original = Arc::new(Prefunded {
                budget,
                held: Mutex::new(None),
            });
            *authority.replay_custody.lock().unwrap() =
                Some(ReplayCustody::Standalone(original.clone()));
            let (result, spent, live, baseline) = if via_graph {
                graph
                    .fork(
                        parent,
                        name,
                        IdempotencyKey::new(),
                        original.as_ref(),
                        &cancellation,
                    )
                    .await
            } else {
                let (result, spent, live, baseline) = parent
                    .fork::<_, _, WorkspaceError>(
                        name,
                        options,
                        crate::PublicationPermit::Unrestricted,
                        original.as_ref(),
                        &cancellation,
                        (),
                    )
                    .await;
                (
                    result.map_err(crate::WorkspaceLineageError::Workspace),
                    spent,
                    live,
                    baseline,
                )
            };
            spent.verify(budget)?;
            assert!(live >= baseline, "fork retired earlier original backing");
            result?
        };
        assert!(authority.replay_custody.lock().unwrap().is_none());
        assert_eq!(child.name().as_str(), name);
        if parent_index == 0 {
            // Derive the minimal read count from the actual authenticated
            // primitives, including the selected child's immutable ancestry.
            let parent_proof = parent
                .generation(fork_generation, budget, &cancellation)
                .await?;
            let initial = child.head(budget, &cancellation).await?;
            let ancestry = initial.value.parents(budget, &cancellation).await?;
            let exact = child
                .generation(initial.value.id(), budget, &cancellation)
                .await?;
            for selected_exact in [false, true] {
                if selected_exact {
                    child.write_text("/later", "advanced").await?;
                }
                let selected_reads = if selected_exact {
                    exact.work.backend_read_operations
                } else {
                    initial.work.backend_read_operations
                };
                let expected_reads = parent_proof.work.backend_read_operations
                    + selected_reads
                    + ancestry.work.backend_read_operations;
                for native_registration in [false, true] {
                    let registration =
                        crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
                    let (record, spent) = if native_registration {
                        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                            .map_err(|(error, _)| error)?;
                        if !selected_exact {
                            *authority.replay_custody.lock().unwrap() =
                                Some(ReplayCustody::Native(original.clone()));
                        }
                        let record = if selected_exact {
                            registration
                                .register_existing_child_at_generation(
                                    parent,
                                    &child,
                                    fork_generation,
                                    initial.value.id(),
                                    &original,
                                    &cancellation,
                                )
                                .await?
                        } else {
                            registration
                                .register_existing_child(
                                    parent,
                                    &child,
                                    fork_generation,
                                    &original,
                                    &cancellation,
                                )
                                .await?
                        };
                        (record, original.receipt())
                    } else {
                        let original = Arc::new(Prefunded {
                            budget,
                            held: Mutex::new(None),
                        });
                        if !selected_exact {
                            *authority.replay_custody.lock().unwrap() =
                                Some(ReplayCustody::Standalone(original.clone()));
                        }
                        let (result, spent, live, baseline) = if selected_exact {
                            registration
                                .register_existing_child_at_generation(
                                    parent,
                                    &child,
                                    fork_generation,
                                    initial.value.id(),
                                    original.as_ref(),
                                    &cancellation,
                                )
                                .await
                        } else {
                            registration
                                .register_existing_child(
                                    parent,
                                    &child,
                                    fork_generation,
                                    original.as_ref(),
                                    &cancellation,
                                )
                                .await
                        };
                        assert!(
                            live >= baseline,
                            "child registration retired original backing"
                        );
                        (result?, spent)
                    };
                    spent.verify(budget)?;
                    assert!(authority.replay_custody.lock().unwrap().is_none());
                    assert_eq!(
                        spent.backend_read_operations, expected_reads,
                        "child registration repeated or detached authenticated reads"
                    );
                    assert_eq!(record.parent_workspace_id, Some(parent.id()));
                    assert_eq!(record.fork_generation, fork_generation);
                    assert_eq!(record.initial_generation, initial.value.id());
                }
            }
            for cancelled in [false, true] {
                let cancellation = CancellationToken::new();
                if cancelled {
                    cancellation.cancel();
                }
                let constrained = WorkBudget {
                    backend_read_operations: 0,
                    ..budget
                };
                let failure = graph
                    .register_existing_child_at_generation(
                        parent,
                        &child,
                        fork_generation,
                        initial.value.id(),
                        constrained,
                        &cancellation,
                    )
                    .await
                    .err()
                    .ok_or("child registration escaped its cancellation or read allowance")?;
                if cancelled {
                    assert!(matches!(
                        failure.error,
                        crate::WorkspaceLineageError::Workspace(WorkspaceError::Cancelled(_))
                    ));
                    assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
                } else {
                    assert!(matches!(
                        failure.error,
                        crate::WorkspaceLineageError::Workspace(WorkspaceError::Work(
                            crate::WorkError::BudgetExceeded {
                                counter: "backend_read_operations",
                                ..
                            }
                        ))
                    ));
                }
                assert_eq!(failure.work.backend_read_operations, 0);
            }
        }
        let read = child.read("/source", 8, budget, &cancellation).await?;
        assert_eq!(read.value.as_ref(), b"original");
        workspaces.push(child);
    }
    let root = &workspaces[0];
    let generation = root.head(budget, &cancellation).await?.value;
    for (cancelled, via_graph) in [(false, false), (true, false), (false, true), (true, true)] {
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let constrained = WorkBudget {
            allocation_operations: 0,
            ..budget
        };
        *authority.replay_minimum.lock().unwrap() = Some(0);
        let failure = if via_graph {
            graph
                .fork(
                    root,
                    "refused-child",
                    IdempotencyKey::new(),
                    constrained,
                    &cancellation,
                )
                .await
                .err()
                .ok_or("lineage fork escaped cancellation or its allocation allowance")?
        } else {
            root.fork::<_, _, WorkspaceError>(
                "refused-child",
                ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                constrained,
                &cancellation,
                (),
            )
            .await
            .err()
            .ok_or("fork escaped cancellation or its allocation allowance")?
            .map_error(crate::WorkspaceLineageError::Workspace)
        };
        if cancelled {
            assert!(matches!(
                failure.error,
                crate::WorkspaceLineageError::Workspace(WorkspaceError::Cancelled(_))
            ));
            assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
        } else {
            assert!(matches!(
                failure.error,
                crate::WorkspaceLineageError::Workspace(WorkspaceError::Work(
                    crate::WorkError::BudgetExceeded {
                        counter: "allocation_operations",
                        ..
                    }
                ))
            ));
        }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert!(authority.replay_minimum.lock().unwrap().take().is_some());
    }
    Ok(())
}
