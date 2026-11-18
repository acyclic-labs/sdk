//! Durable workspace-context registry contracts over the public store interface.

mod workspace_context {
    mod tests {
        use acyclic_fs::{WorkspaceId, workspace_context::*};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use std::{
            collections::BTreeMap,
            path::{Path, PathBuf},
            sync::Mutex,
        };

        #[derive(Default)]
        struct CountingContextStore {
            inner: MemoryWorkspaceContextStore,
            loads: Arc<AtomicUsize>,
            lists: Arc<AtomicUsize>,
            swaps: Arc<AtomicUsize>,
            duplicate_list: bool,
            insert_before_swap: Mutex<Option<WorkspaceContext>>,
        }

        impl CountingContextStore {
            fn reset(&self) {
                self.loads.store(0, Ordering::Relaxed);
                self.lists.store(0, Ordering::Relaxed);
                self.swaps.store(0, Ordering::Relaxed);
            }

            fn counts(&self) -> (usize, usize, usize) {
                (
                    self.loads.load(Ordering::Relaxed),
                    self.lists.load(Ordering::Relaxed),
                    self.swaps.load(Ordering::Relaxed),
                )
            }

            async fn publish_interleaved(&self) -> Result<(), MemoryWorkspaceContextStoreError> {
                let inserted = self
                    .insert_before_swap
                    .lock()
                    .map_err(|_| MemoryWorkspaceContextStoreError)?
                    .take();
                if let Some(record) = inserted {
                    assert!(
                        self.inner
                            .compare_and_swap(record.context_id, record.revision - 1, record)
                            .await?
                    );
                }
                Ok(())
            }
        }

        impl WorkspaceContextStore for CountingContextStore {
            type Error = MemoryWorkspaceContextStoreError;

            async fn load(
                &self,
                context_id: WorkspaceContextId,
            ) -> Result<Option<WorkspaceContext>, Self::Error> {
                self.loads.fetch_add(1, Ordering::Relaxed);
                self.inner.load(context_id).await
            }

            async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
                self.lists.fetch_add(1, Ordering::Relaxed);
                let mut records = self.inner.list().await?;
                if self.duplicate_list {
                    records.extend(records.last().cloned());
                }
                Ok(records)
            }

            async fn compare_and_swap(
                &self,
                context_id: WorkspaceContextId,
                expected_revision: u64,
                replacement: WorkspaceContext,
            ) -> Result<bool, Self::Error> {
                self.swaps.fetch_add(1, Ordering::Relaxed);
                self.publish_interleaved().await?;
                self.inner
                    .compare_and_swap(context_id, expected_revision, replacement)
                    .await
            }

            async fn compare_and_swap_many(
                &self,
                expected_revisions: BTreeMap<WorkspaceContextId, u64>,
                replacements: Vec<WorkspaceContext>,
                require_exact_set: bool,
            ) -> Result<bool, Self::Error> {
                self.swaps.fetch_add(1, Ordering::Relaxed);
                self.publish_interleaved().await?;
                self.inner
                    .compare_and_swap_many(expected_revisions, replacements, require_exact_set)
                    .await
            }

            async fn discard_subtree(
                &self,
                parent: WorkspaceContextId,
                child: WorkspaceContextId,
                maximum: u32,
            ) -> Result<WorkspaceContextDiscardOutcome, Self::Error> {
                self.swaps.fetch_add(1, Ordering::Relaxed);
                self.inner.discard_subtree(parent, child, maximum).await
            }
        }

        fn context(byte: u8) -> WorkspaceContextId {
            WorkspaceContextId::from_bytes([byte; 16])
        }

        fn root_id(byte: u8) -> WorkspaceRootId {
            WorkspaceRootId::from_bytes([byte; 16])
        }

        fn workspace(byte: u8) -> WorkspaceId {
            WorkspaceId::from_bytes([byte; 16])
        }

        fn numbered_context(number: u128) -> WorkspaceContextId {
            WorkspaceContextId::from_bytes(number.to_le_bytes())
        }

        fn numbered_root(number: u128) -> WorkspaceRootId {
            WorkspaceRootId::from_bytes(number.to_le_bytes())
        }

        fn numbered_workspace(number: u128) -> WorkspaceId {
            WorkspaceId::from_bytes(number.to_le_bytes())
        }

        fn scaled_roots(generation: u128, count: u128) -> Vec<WorkspaceContextRoot> {
            (0..count)
                .map(|number| WorkspaceContextRoot {
                    root_id: numbered_root(number + 1),
                    source_path: PathBuf::from(if cfg!(windows) {
                        format!("C:\\scale-root-{number}")
                    } else {
                        format!("/scale-root-{number}")
                    }),
                    workspace_id: numbered_workspace(generation * count + number + 1),
                    workspace_name: format!("scale-{generation}-{number}"),
                    parent_workspace_id: (generation > 0)
                        .then(|| numbered_workspace((generation - 1) * count + number + 1)),
                    mount_path: None,
                })
                .collect()
        }

        fn root(root_byte: u8, workspace_byte: u8, parent: Option<u8>) -> WorkspaceContextRoot {
            WorkspaceContextRoot {
                root_id: root_id(root_byte),
                source_path: PathBuf::from(if cfg!(windows) { "C:\\work" } else { "/work" }),
                workspace_id: workspace(workspace_byte),
                workspace_name: format!("workspace-{workspace_byte}"),
                parent_workspace_id: parent.map(workspace),
                mount_path: None,
            }
        }

        #[tokio::test]
        async fn recursive_contexts_require_exact_direct_parent_roots() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(1), [root(9, 1, None)])
                .await
                .expect("root");
            registry
                .register_child(context(2), context(1), [root(9, 2, Some(1))])
                .await
                .expect("child");
            registry
                .register_child(context(3), context(2), [root(9, 3, Some(2))])
                .await
                .expect("grandchild");

            registry
                .authorize_parent(context(3), context(2))
                .await
                .expect("direct parent");
            assert!(matches!(
                registry.authorize_parent(context(3), context(1)).await,
                Err(WorkspaceContextError::UnauthorizedParent)
            ));
        }

        #[tokio::test]
        async fn root_removal_requires_descendants_to_release_the_binding_first() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(41), scaled_roots(0, 2))
                .await
                .expect("two root bindings");
            registry
                .register_child(context(42), context(41), scaled_roots(1, 2))
                .await
                .expect("child bindings");
            assert!(matches!(
                registry.remove_root(context(41), numbered_root(2)).await,
                Err(WorkspaceContextError::RootInUse)
            ));
            let child = registry
                .remove_root(context(42), numbered_root(2))
                .await
                .expect("child releases its root");
            assert!(!child.roots.contains_key(&numbered_root(2)));
            let parent = registry
                .remove_root(context(41), numbered_root(2))
                .await
                .expect("parent releases unused root");
            assert!(!parent.roots.contains_key(&numbered_root(2)));
            assert!(matches!(
                registry.remove_root(context(41), numbered_root(1)).await,
                Err(WorkspaceContextError::InvalidRoots)
            ));
        }

        #[tokio::test]
        async fn deep_fork_chain_root_removal_keeps_unrelated_lineage_and_live_descendants() {
            const DEPTH: u128 = 1024;
            let ordered_context =
                |number: u128| WorkspaceContextId::from_bytes(number.to_be_bytes());
            let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
            // The unrelated chain sorts before the root being removed, exercising
            // cached ancestor walks before inspecting that root's descendants.
            registry
                .register_root(ordered_context(1), scaled_roots(0, 2))
                .await
                .expect("unrelated root");
            for generation in 1..=DEPTH {
                registry
                    .register_child(
                        ordered_context(generation + 1),
                        ordered_context(generation),
                        scaled_roots(generation, 2),
                    )
                    .await
                    .expect("deep unrelated child");
            }
            let target = ordered_context(DEPTH + 2);
            registry
                .register_root(target, scaled_roots(0, 2))
                .await
                .expect("independent target root");
            let before = registry
                .resolve(target)
                .await
                .expect("target before removal");
            let mut expected_records = registry
                .store()
                .list()
                .await
                .expect("all records before removal");
            registry.store().reset();
            let removed = registry
                .remove_root(target, numbered_root(2))
                .await
                .expect("unrelated descendants do not block removal");
            assert_eq!(registry.store().counts(), (0, 1, 1));
            assert_eq!(removed.revision, before.revision + 1);
            assert_eq!(removed.roots.len(), 1);
            assert_eq!(
                removed.roots.get(&numbered_root(1)),
                before.roots.get(&numbered_root(1))
            );
            *expected_records
                .iter_mut()
                .find(|record| record.context_id == target)
                .expect("target record") = removed;
            assert_eq!(
                registry
                    .store()
                    .list()
                    .await
                    .expect("all records after removal"),
                expected_records
            );
            registry
                .adopt_root(
                    target,
                    before
                        .roots
                        .get(&numbered_root(2))
                        .expect("second root")
                        .clone(),
                )
                .await
                .expect("restore target binding");

            for generation in 1..=DEPTH {
                registry
                    .register_child(
                        ordered_context(DEPTH * 2 + 3 - generation),
                        if generation == 1 {
                            target
                        } else {
                            ordered_context(DEPTH * 2 + 4 - generation)
                        },
                        scaled_roots(generation, 2),
                    )
                    .await
                    .expect("deep target child");
                registry
                    .set_active(ordered_context(DEPTH * 2 + 3 - generation), false)
                    .await
                    .expect("freeze retained descendant");
            }
            // Frozen descendants remain retained and must still block removal.
            let records_before = registry
                .store()
                .list()
                .await
                .expect("all records before rejection");
            registry.store().reset();
            assert!(matches!(
                registry.remove_root(target, numbered_root(2)).await,
                Err(WorkspaceContextError::RootInUse)
            ));
            assert_eq!(registry.store().counts(), (0, 1, 0));
            assert_eq!(
                registry
                    .store()
                    .list()
                    .await
                    .expect("all records after rejection"),
                records_before
            );
        }

        #[tokio::test]
        async fn context_mutations_return_the_original_conditional_conflict() {
            for operation in 0..5 {
                let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
                let mut concurrent = registry
                    .register_root(context(41), scaled_roots(0, 2))
                    .await
                    .expect("original root");
                concurrent.revision += 1;
                concurrent.state = WorkspaceContextState::Frozen;
                *registry
                    .store()
                    .insert_before_swap
                    .lock()
                    .expect("injection lock") = Some(concurrent.clone());
                registry.store().reset();
                let result = match operation {
                    0 => {
                        registry
                            .register_child(context(42), context(41), scaled_roots(1, 2))
                            .await
                    }
                    1 => {
                        registry
                            .adopt_root(context(41), scaled_roots(0, 3).pop().expect("new root"))
                            .await
                    }
                    2 => registry.remove_root(context(41), numbered_root(2)).await,
                    3 => {
                        registry
                            .set_workspace(
                                context(41),
                                numbered_root(2),
                                workspace(99),
                                "selected".to_owned(),
                                None,
                            )
                            .await
                    }
                    _ => registry.set_active(context(41), true).await,
                };
                assert!(matches!(result, Err(WorkspaceContextError::Conflict)));
                assert_eq!(
                    registry.store().counts(),
                    match operation {
                        0 => (2, 0, 1),
                        2 => (0, 1, 1),
                        _ => (1, 0, 1),
                    }
                );
                assert_eq!(
                    registry.resolve(context(41)).await.expect("root"),
                    concurrent
                );
                assert!(
                    registry
                        .store()
                        .load(context(42))
                        .await
                        .expect("child")
                        .is_none()
                );
                if operation == 0 {
                    let registered = registry
                        .register_child(context(42), context(41), scaled_roots(1, 2))
                        .await
                        .expect("explicit new publication observes the concurrent parent");
                    assert_eq!(
                        registry
                            .register_child(context(42), context(41), scaled_roots(1, 2))
                            .await
                            .expect("exact registration remains idempotent"),
                        registered
                    );
                }
            }
        }

        #[tokio::test]
        async fn root_removal_preserves_a_concurrently_registered_descendant() {
            let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
            let before = registry
                .register_root(context(41), scaled_roots(0, 2))
                .await
                .expect("target root");
            let child = WorkspaceContext {
                version: before.version,
                revision: 1,
                context_id: context(42),
                parent_context_id: Some(context(41)),
                roots: scaled_roots(1, 2)
                    .into_iter()
                    .map(|root| (root.root_id, root))
                    .collect(),
                state: WorkspaceContextState::Active,
            };
            *registry
                .store()
                .insert_before_swap
                .lock()
                .expect("injection lock") = Some(child.clone());
            registry.store().reset();
            assert!(matches!(
                registry.remove_root(context(41), numbered_root(2)).await,
                Err(WorkspaceContextError::Conflict)
            ));
            assert_eq!(registry.store().counts(), (0, 1, 1));
            assert_eq!(registry.resolve(context(41)).await.expect("target"), before);
            assert_eq!(
                registry.resolve(context(42)).await.expect("new child"),
                child
            );
            registry.store().reset();
            assert!(matches!(
                registry.remove_root(context(41), numbered_root(2)).await,
                Err(WorkspaceContextError::RootInUse)
            ));
            assert_eq!(registry.store().counts(), (0, 1, 0));
        }

        #[tokio::test]
        async fn root_removal_rejects_invalid_stored_records_before_publication() {
            let mut accepted = Vec::new();
            for corruption in 0..9 {
                let registry = WorkspaceContextRegistry::new(CountingContextStore {
                    duplicate_list: corruption == 7,
                    ..CountingContextStore::default()
                });
                registry
                    .register_root(context(41), scaled_roots(0, 2))
                    .await
                    .expect("target root");
                registry
                    .register_root(context(42), scaled_roots(0, 2))
                    .await
                    .expect("unrelated root");
                {
                    let store = &registry.store().inner;
                    let mut record = store
                        .load(context(42))
                        .await
                        .expect("load")
                        .expect("stored context");
                    let revision = record.revision;
                    match corruption {
                        0 => record.version = 0,
                        1 => record.revision = 0,
                        2 => record.roots.clear(),
                        7 => {}
                        8 => record.version += 1,
                        _ => {
                            let root = record.roots.get_mut(&numbered_root(1)).expect("root");
                            match corruption {
                                3 => root.root_id = numbered_root(99),
                                4 => root.source_path = PathBuf::from("relative"),
                                5 => root.mount_path = Some(PathBuf::from("relative")),
                                _ => {
                                    assert_eq!(corruption, 6);
                                    root.workspace_name.clear();
                                }
                            }
                        }
                    }
                    assert!(
                        store
                            .compare_and_swap(context(42), revision, record)
                            .await
                            .expect("corrupt record")
                    );
                }
                let before = registry
                    .store()
                    .inner
                    .list()
                    .await
                    .expect("corrupt snapshot");
                registry.store().reset();
                if !matches!(
                    registry.remove_root(context(41), numbered_root(2)).await,
                    Err(WorkspaceContextError::IncompatibleState)
                ) {
                    accepted.push(corruption);
                    continue;
                }
                assert_eq!(registry.store().counts(), (0, 1, 0));
                assert_eq!(
                    registry
                        .store()
                        .inner
                        .list()
                        .await
                        .expect("unchanged snapshot"),
                    before
                );
            }
            assert!(accepted.is_empty(), "accepted corruptions {accepted:?}");
        }

        #[tokio::test]
        async fn root_removal_rejects_cyclic_and_missing_ancestry_without_mutation() {
            for cyclic in [false, true] {
                let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
                registry
                    .register_root(context(41), scaled_roots(0, 2))
                    .await
                    .expect("target root");
                registry
                    .register_root(context(42), scaled_roots(0, 2))
                    .await
                    .expect("unrelated root");
                // Persisted adapters can return malformed lineage after corruption.
                // Registration intentionally cannot create either of these states.
                let store = registry.store();
                let mut record = store
                    .load(context(42))
                    .await
                    .expect("load")
                    .expect("unrelated context");
                record.parent_context_id = Some(if cyclic { context(42) } else { context(99) });
                assert!(
                    store
                        .compare_and_swap(context(42), record.revision, record)
                        .await
                        .expect("corrupt lineage")
                );
                let before = registry
                    .store()
                    .list()
                    .await
                    .expect("records before removal");
                assert!(matches!(
                    registry.remove_root(context(41), numbered_root(2)).await,
                    Err(WorkspaceContextError::IncompatibleState)
                ));
                assert_eq!(
                    registry
                        .store()
                        .list()
                        .await
                        .expect("records after removal"),
                    before
                );
            }
        }

        #[tokio::test]
        async fn roots_are_adopted_lazily_through_the_exact_parent() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(1), [root(9, 1, None)])
                .await
                .expect("root");
            registry
                .register_child(context(2), context(1), [root(9, 2, Some(1))])
                .await
                .expect("child");

            let source_path = PathBuf::from(if cfg!(windows) {
                "C:\\second-work"
            } else {
                "/second-work"
            });
            let parent_root = WorkspaceContextRoot {
                root_id: root_id(10),
                source_path: source_path.clone(),
                workspace_id: workspace(3),
                workspace_name: "workspace-3".to_owned(),
                parent_workspace_id: None,
                mount_path: None,
            };
            registry
                .adopt_root(context(1), parent_root.clone())
                .await
                .expect("parent adopts root");
            let child_root = WorkspaceContextRoot {
                root_id: root_id(10),
                source_path,
                workspace_id: workspace(4),
                workspace_name: "workspace-4".to_owned(),
                parent_workspace_id: Some(workspace(3)),
                mount_path: None,
            };
            let adopted = registry
                .adopt_root(context(2), child_root.clone())
                .await
                .expect("child adopts parent root");
            assert_eq!(adopted.roots.get(&root_id(10)), Some(&child_root));
            assert_eq!(
                registry
                    .adopt_root(context(2), child_root)
                    .await
                    .expect("exact retry"),
                adopted
            );
        }

        #[tokio::test]
        async fn child_cannot_adopt_a_root_outside_its_direct_parent() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(1), [root(9, 1, None)])
                .await
                .expect("root");
            registry
                .register_child(context(2), context(1), [root(9, 2, Some(1))])
                .await
                .expect("child");

            let unauthorized = WorkspaceContextRoot {
                root_id: root_id(10),
                source_path: PathBuf::from(if cfg!(windows) {
                    "C:\\outside"
                } else {
                    "/outside"
                }),
                workspace_id: workspace(4),
                workspace_name: "workspace-4".to_owned(),
                parent_workspace_id: Some(workspace(3)),
                mount_path: None,
            };
            assert!(matches!(
                registry.adopt_root(context(2), unauthorized).await,
                Err(WorkspaceContextError::InvalidRoots)
            ));
        }

        #[tokio::test]
        async fn mount_routing_wins_and_discard_is_recursive() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(1), [root(9, 1, None)])
                .await
                .expect("root");
            registry
                .register_child(context(2), context(1), [root(9, 2, Some(1))])
                .await
                .expect("child");
            registry
                .register_child(context(3), context(2), [root(9, 3, Some(2))])
                .await
                .expect("grandchild");
            let mount = PathBuf::from(if cfg!(windows) {
                "C:\\mounts\\child"
            } else {
                "/mounts/child"
            });
            registry
                .set_mount(context(2), root_id(9), Some(mount.clone()))
                .await
                .expect("set mount");
            let route = registry
                .route(context(2), &mount.join("src/lib.rs"))
                .await
                .expect("route")
                .expect("mapped");
            assert_eq!(route.kind, WorkspaceRouteKind::Mount);
            assert_eq!(route.relative_path, PathBuf::from("src/lib.rs"));

            let discarded = registry
                .discard_subtree(context(1), context(2), 8)
                .await
                .expect("discard");
            assert_eq!(discarded, vec![context(2), context(3)]);
            assert_eq!(
                registry
                    .resolve(context(3))
                    .await
                    .expect("grandchild")
                    .state,
                WorkspaceContextState::Discarded
            );
            assert!(matches!(
                registry.route(context(2), &mount).await,
                Err(WorkspaceContextError::Discarded)
            ));
            assert!(matches!(
                registry
                    .set_mount(context(2), root_id(9), Some(mount.clone()))
                    .await,
                Err(WorkspaceContextError::Discarded)
            ));
            assert!(matches!(
                registry.authorize_parent(context(2), context(1)).await,
                Err(WorkspaceContextError::Discarded)
            ));
            assert!(matches!(
                registry.discard_subtree(context(1), context(2), 8).await,
                Err(WorkspaceContextError::Discarded)
            ));
        }

        #[tokio::test]
        async fn workspace_rebinding_preserves_direct_parent_lineage() {
            let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
            registry
                .register_root(context(1), [root(9, 1, None)])
                .await
                .expect("root");
            registry
                .register_child(context(2), context(1), [root(9, 2, Some(1))])
                .await
                .expect("child");

            assert!(matches!(
                registry
                    .set_workspace(
                        context(2),
                        root_id(9),
                        workspace(3),
                        "topic".to_owned(),
                        Some(workspace(99)),
                    )
                    .await,
                Err(WorkspaceContextError::InvalidRoots)
            ));
            assert!(matches!(
                registry
                    .set_workspace(
                        context(1),
                        root_id(9),
                        workspace(4),
                        "root-topic".to_owned(),
                        Some(workspace(1)),
                    )
                    .await,
                Err(WorkspaceContextError::InvalidRoots)
            ));
            let child = registry.resolve(context(2)).await.expect("child");
            assert_eq!(
                child
                    .roots
                    .get(&root_id(9))
                    .expect("child root")
                    .workspace_id,
                workspace(2)
            );
        }

        #[tokio::test]
        async fn many_roots_and_deep_lineage_have_constant_store_work_per_step() {
            const ROOTS: u128 = 64;
            const DEPTH: u128 = 64;

            let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
            registry
                .register_root(numbered_context(1), scaled_roots(0, ROOTS))
                .await
                .expect("large root context");

            for generation in 1..DEPTH {
                registry.store().reset();
                registry
                    .register_child(
                        numbered_context(generation + 1),
                        numbered_context(generation),
                        scaled_roots(generation, ROOTS),
                    )
                    .await
                    .expect("deep child context");
                assert_eq!(
                    registry.store().counts(),
                    (1, 0, 1),
                    "child registration must inspect only its direct parent"
                );
            }

            registry.store().reset();
            let route = registry
                .route(
                    numbered_context(DEPTH),
                    Path::new(if cfg!(windows) {
                        "C:\\scale-root-63\\nested\\file"
                    } else {
                        "/scale-root-63/nested/file"
                    }),
                )
                .await
                .expect("route")
                .expect("matching root");
            assert_eq!(route.root_id, numbered_root(ROOTS));
            assert_eq!(registry.store().counts(), (1, 0, 0));
        }

        #[tokio::test]
        async fn ten_thousand_durable_contexts_and_128_active_children_remain_direct() {
            const CHILDREN: u128 = 10_000;
            const ACTIVE_CHILDREN: u128 = 128;

            let registry = WorkspaceContextRegistry::new(CountingContextStore::default());
            let parent_context = numbered_context(1);
            let root = scaled_roots(0, 1).pop().expect("root binding");
            let parent_workspace = root.workspace_id;
            registry
                .register_root(parent_context, [root.clone()])
                .await
                .expect("root context");

            for number in 0..CHILDREN {
                registry.store().reset();
                registry
                    .register_child(
                        numbered_context(number + 2),
                        parent_context,
                        [WorkspaceContextRoot {
                            workspace_id: numbered_workspace(number + 2),
                            workspace_name: format!("sibling-{number}"),
                            parent_workspace_id: Some(parent_workspace),
                            ..root.clone()
                        }],
                    )
                    .await
                    .expect("sibling child context");
                assert_eq!(
                    registry.store().counts(),
                    (1, 0, 1),
                    "registration must inspect only the direct parent"
                );
            }

            for number in ACTIVE_CHILDREN..CHILDREN {
                registry.store().reset();
                registry
                    .set_active(numbered_context(number + 2), false)
                    .await
                    .expect("freeze retained sibling");
                assert_eq!(registry.store().counts(), (1, 0, 1));
            }

            registry.store().reset();
            let records = registry.store().list().await.expect("durable contexts");
            assert_eq!(registry.store().counts(), (0, 1, 0));
            assert_eq!(
                records.len(),
                usize::try_from(CHILDREN + 1).expect("context count fits usize")
            );
            assert_eq!(
                records
                    .iter()
                    .filter(|record| {
                        record.parent_context_id == Some(parent_context)
                            && record.state == WorkspaceContextState::Active
                    })
                    .count(),
                usize::try_from(ACTIVE_CHILDREN).expect("active child count fits usize")
            );

            registry.store().reset();
            let route = registry
                .route(
                    numbered_context(ACTIVE_CHILDREN + 1),
                    &root.source_path.join("nested/file"),
                )
                .await
                .expect("route")
                .expect("matching root");
            assert_eq!(route.root_id, root.root_id);
            assert_eq!(registry.store().counts(), (1, 0, 0));

            registry.store().reset();
            assert!(matches!(
                registry
                    .discard_subtree(parent_context, numbered_context(2), 0)
                    .await,
                Err(WorkspaceContextError::TraversalLimit)
            ));
            assert_eq!(
                registry.store().counts(),
                (0, 0, 1),
                "the registry must delegate one atomic bounded discard without listing all contexts"
            );
            assert_eq!(
                registry
                    .resolve(numbered_context(2))
                    .await
                    .expect("bounded rejection changes nothing")
                    .state,
                WorkspaceContextState::Active
            );
        }
    }
}
