//! Shared physical roots use one SDK acquisition path.
use super::*;
use acyclic_fs::{
    MeasuredResult, NativeAllocation, NativeMaterializationWork, NativeRootIdentity,
    OperationFailure, OperationReceipt, WorkCounters,
};
use acyclic_harness::wire::ErrorCode;
use acyclic_harness::{Error as RootError, Result as RootResult};

mod acquisition;
pub(crate) use acquisition::{
    acquire, acquire_with_native_work, native_source_error, native_watch_error, native_work_error,
    shutdown_pending_with_native_work,
};

pub(crate) fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 4096,
        authority_records_appended: 64,
        authority_bytes_read: 8 * 1024 * 1024,
        authority_bytes_written: 1024 * 1024,
        object_probes: 4096,
        backend_read_operations: 4096,
        backend_write_operations: 4096,
        durability_operations: 4096,
        page_reads: 4096,
        page_writes: 4096,
        object_bytes_read: 8 * 1024 * 1024,
        object_bytes_written: 8 * 1024 * 1024,
        bytes_hashed: 8 * 1024 * 1024,
        bytes_copied: 8 * 1024 * 1024,
        bytes_encoded: 8 * 1024 * 1024,
        source_bytes_read: 8 * 1024 * 1024,
        source_path_components: 4096,
        source_entries_visited: 4096,
        output_bytes: 8 * 1024 * 1024,
        items_examined: 4096,
        items_returned: 4096,
        allocation_operations: 4096,
        peak_allocation_bytes: 8 * 1024 * 1024,
        materializations: 64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_or_cancelled_root_allowance_rejects_before_native_access() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("never-opened");
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let failure = registry
            .acquire(
                &missing,
                SharedRootAdmission::Fresh,
                WorkBudget::default(),
                &cancellation,
            )
            .await
            .err()
            .expect("no root probe admitted");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(registry.roots.lock().await.is_empty());
        cancellation.cancel();
        let failure = registry
            .acquire(
                &missing,
                SharedRootAdmission::Fresh,
                budget(),
                &cancellation,
            )
            .await
            .err()
            .expect("cancelled root probe");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(failure.error.contains("cancel"));
        assert!(!missing.exists());
    }

    #[tokio::test]
    async fn later_root_admission_failure_preserves_earlier_native_probe_work() {
        let directory = tempfile::tempdir().unwrap();
        let registry = SharedRootRegistry::default();
        let mut allowance = budget();
        allowance.backend_read_operations = 1;
        let failure = registry
            .acquire(
                directory.path(),
                SharedRootAdmission::Fresh,
                allowance,
                &CancellationToken::new(),
            )
            .await
            .err()
            .expect("watcher cannot reset the exhausted shared allowance");
        assert_eq!(failure.work.backend_read_operations, 1);
        assert!(failure.work.source_path_components > 0);
        assert_eq!(registry.live_roots().await, 0);
    }

    fn close_native_watch(
        physical: &SharedPhysicalRoot,
        work: &NativeMaterializationWork,
    ) -> RootResult<bool> {
        // The SDK observer ABI uses std::sync::Mutex and exposes poisoning as
        // a real typed failure; do not erase it with an immediate lock unwrap.
        physical
            .watcher
            .lock()
            .map_err(|_| RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "fixture original watcher state is poisoned",
            })?
            .shutdown_with_native_work(work)
            .map_err(native_watch_error)
    }

    fn selection(
        fixture: &crate::pi_fork::tests::Fixture,
    ) -> (&Path, SourceReference, NativeRootIdentity) {
        let crate::pi_root::PiRootIntent::Project { paths, .. } = &fixture.root_request.root else {
            panic!("actual project creator fixture");
        };
        let (key, path) = paths.first_key_value().unwrap();
        let created = &fixture.root_allocation.roots[key];
        (
            Path::new(path),
            SourceReference {
                identity: created.source_identity,
                epoch: created.source_epoch,
            },
            NativeRootIdentity::from_bytes(created.native_root_identity),
        )
    }

    async fn original_root_guard(
        fixture: &crate::pi_fork::tests::Fixture,
        work: &NativeMaterializationWork,
    ) -> RootResult<NativeRootIdentity> {
        let task = fixture.root_request.binding.task_id;
        if fixture.context.durable_task_id() != Some(task) || fixture.journal.task_id() != task {
            return Err(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "fixture source changed its original admitted owner context",
            });
        }
        let (key, project) = fixture
            .root_request
            .binding
            .project_volumes
            .first_key_value()
            .unwrap();
        fixture
            .context
            .verify_original_volume_read_with_native_work(&fixture.journal, project, work)
            .await?;
        fixture
            .journal
            .verify(
                false,
                &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                    acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;
        // Fixture::new obtained this protected result by observing the real
        // original Root effect and its actual Filesystem creator resources.
        let created = &fixture.root_allocation.roots[key];
        if created.repository_workspace_id
            != fixture.root_handle.workspace.workspace().id().into_bytes()
        {
            return Err(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "fixture source differs from its protected original creator",
            });
        }
        Ok(NativeRootIdentity::from_bytes(created.native_root_identity))
    }

    #[tokio::test]
    async fn native_root_resume_reuses_and_reopens_the_same_registered_source() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let before = work.receipt();
        let first = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        assert_eq!(first.source.inner().root_identity(), expected);
        assert_eq!(first.source.reference().identity, original.identity);
        assert!(first.source.reference().epoch > original.epoch);
        assert!(work.receipt().backend_read_operations > before.backend_read_operations);
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let warm = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &warm));
        let retained_epoch = warm.source.reference().epoch;
        assert!(close_native_watch(&warm, &query).unwrap());
        drop(warm);
        drop(first);
        assert_eq!(registry.live_roots().await, 0);
        let reopened = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert_eq!(reopened.source.inner().root_identity(), expected);
        assert_eq!(reopened.source.reference().identity, original.identity);
        assert!(reopened.source.reference().epoch > retained_epoch);
        assert!(close_native_watch(&reopened, &query).unwrap());
    }

    #[tokio::test]
    async fn native_source_backing_outlives_the_returned_physical_root() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        let source = Arc::clone(&physical.source);
        assert!(close_native_watch(&physical, &work).unwrap());
        drop(physical);
        let held = work.live_allocation_bytes();
        assert_eq!(source.inner().root_identity(), expected);
        assert_eq!(source.reference().identity, original.identity);
        drop(source);
        assert!(work.live_allocation_bytes() < held);
        registry.prune().await;
        assert!(registry.roots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_root_cancellation_and_quota_preserve_the_original_prefix() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        assert_eq!(work.receipt(), before);
        assert!(registry.roots.lock().await.is_empty());
        let mut denied = budget();
        denied.backend_read_operations = 0;
        let work = NativeMaterializationWork::new(denied, &CancellationToken::new()).unwrap();
        assert!(
            registry
                .acquire_with_native_work(
                    path,
                    SharedRootAdmission::Resume(original),
                    &work,
                    || original_root_guard(&fixture, &work)
                )
                .await
                .is_err()
        );
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert!(work.receipt().allocation_operations > 0);
        assert!(registry.roots.lock().await.is_empty());
        assert_eq!(
            std::fs::read(path.join("observed.txt")).unwrap(),
            b"original"
        );
    }

    #[tokio::test]
    async fn a_source_reference_never_substitutes_for_the_original_root_read_grant() {
        use acyclic_harness::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, expected) = selection(&fixture);
        let project = fixture
            .root_request
            .binding
            .project_volumes
            .values()
            .next()
            .unwrap();
        let foreign = VolumeRef::new(
            project.provider().clone(),
            "not-original-source",
            VolumeClass::Project,
            VolumeOwner::Project("not-original-root".into()),
        )
        .unwrap();
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let before = work.receipt().backend_read_operations;
        let error = registry
            .acquire_with_native_work(
                path,
                SharedRootAdmission::Resume(original),
                &work,
                || async {
                    fixture.journal.require_volume_grant(
                        &foreign,
                        VolumeOperation::Read,
                        &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                            acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?,
                        )),
                    )?;
                    Ok(expected)
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
        assert_eq!(work.receipt().backend_read_operations, before);
        assert!(registry.roots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_resume_rejects_registered_source_identity_and_root_replacement() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        let other = SourceReference {
            identity: [79; 16],
            epoch: original.epoch,
        };
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(other), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Conflict);
        assert_eq!(physical.source.reference().identity, original.identity);
        std::fs::rename(path, path.with_file_name("original-native-root")).unwrap();
        std::fs::create_dir(path).unwrap();
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(error, RootError::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected: old, actual },
            ..
        } if old == expected && actual != expected));
        assert_eq!(physical.source.inner().root_identity(), expected);
        assert_eq!(physical.source.reference().identity, original.identity);
        assert!(close_native_watch(&physical, &work).unwrap());
    }

    #[tokio::test]
    async fn failed_native_admission_retains_sdk_owners_until_actual_pending_shutdown() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let mut boundaries = 0;
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                boundaries += 1;
                if boundaries == 3 {
                    cancellation.cancel();
                }
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        let prefix = work.receipt();
        assert!(prefix.backend_read_operations > 0);
        assert!(prefix.allocation_operations > 1);
        registry.prune().await;
        let slot = registry.roots.lock().await.get(path).cloned().unwrap();
        assert!(slot.lock().await.pending_watcher.is_some());
        assert_eq!(registry.live_roots().await, 0);
        let cleanup = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        assert!(
            registry
                .shutdown_pending_with_native_work(path, &cleanup, || original_root_guard(
                    &fixture, &cleanup
                ))
                .await
                .unwrap()
        );
        let state = slot.lock().await;
        assert!(state.pending_raw_watcher.is_none());
        assert!(state.pending_watcher.is_none());
        assert!(state.pending_source.is_none());
        drop(state);
        assert!(work.receipt().backend_read_operations >= prefix.backend_read_operations);
        assert!(work.receipt().allocation_operations >= prefix.allocation_operations);
        drop(slot);
        registry.prune().await;
        assert!(registry.roots.lock().await.is_empty());
    }
    #[tokio::test]
    async fn native_observation_publishes_the_same_source_with_real_batch_custody() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(path.join("observed.txt"), b"changed after native resume").unwrap();
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let (observation, allocation) = physical
            .observe_with_native_work(&query, || async {
                original_root_guard(&fixture, &query).await.map(|_| ())
            })
            .await
            .unwrap();
        assert!(observation_consumes_changes(&observation.batch));
        assert_eq!(observation.prior_source, prior);
        assert_eq!(observation.source.identity, original.identity);
        assert!(observation.source.epoch > prior.epoch);
        assert_eq!(physical.source.reference(), observation.source);
        if let WatchBatch::Changes { changes, .. } = &observation.batch {
            assert!(!changes.is_empty());
            assert_eq!(
                allocation.bytes(),
                (changes.capacity() * std::mem::size_of::<acyclic_fs::WatchChange>()) as u64
            );
        }
        assert!(query.receipt().backend_read_operations > 0);
        let held = query.live_allocation_bytes();
        let owned = allocation.bytes();
        drop(observation);
        let remaining = query.live_allocation_bytes();
        assert!(remaining >= owned && remaining <= held);
        drop(allocation);
        assert_eq!(query.live_allocation_bytes(), remaining - owned);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }

    #[tokio::test]
    async fn native_observation_denial_does_not_consume_pending_watch_changes() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"still pending after denied observation",
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let cancelled = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let prefix = cancelled.receipt();
        cancellation.cancel();
        let mut authorized = false;
        let error = physical
            .observe_with_native_work(&cancelled, || {
                authorized = true;
                async { original_root_guard(&fixture, &cancelled).await.map(|_| ()) }
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        assert!(!authorized);
        assert_eq!(cancelled.receipt(), prefix);
        assert_eq!(physical.source.reference(), prior);
        let mut denied_allowance = budget();
        denied_allowance.backend_read_operations = 0;
        let denied =
            NativeMaterializationWork::new(denied_allowance, &CancellationToken::new()).unwrap();
        assert!(
            physical
                .observe_with_native_work(&denied, || async {
                    original_root_guard(&fixture, &denied).await.map(|_| ())
                })
                .await
                .is_err()
        );
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(physical.source.reference(), prior);
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let observed = physical
            .observe_with_native_work(&query, || async {
                original_root_guard(&fixture, &query).await.map(|_| ())
            })
            .await
            .unwrap();
        assert!(observation_consumes_changes(&observed.0.batch));
        assert_eq!(observed.0.prior_source, prior);
        assert!(observed.0.source.epoch > prior.epoch);
        drop(observed);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }

    #[tokio::test]
    async fn native_observation_rejected_after_io_keeps_actual_source_epoch_consistent() {
        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"published before final owner proof",
        )
        .unwrap();
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let mut boundaries = 0;
        let error = physical
            .observe_with_native_work(&query, || {
                boundaries += 1;
                let reject = boundaries == 2;
                let proof = original_root_guard(&fixture, &query);
                async move {
                    proof.await?;
                    if reject {
                        Err(RootError::NativeBoundary {
                            code: acyclic_harness::wire::ErrorCode::Unauthorized,
                            reason: "fixture original source owner withdrew publication",
                        })
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
        assert_eq!(boundaries, 2);
        assert_eq!(physical.source.reference().identity, original.identity);
        assert!(physical.source.reference().epoch > prior.epoch);
        let stored = physical
            .observe_transaction(
                || RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "fixture original source reference is poisoned",
                },
                |source, reference| {
                    assert_eq!(*reference, source);
                    Ok(source)
                },
            )
            .unwrap();
        assert_eq!(stored, physical.source.reference());
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }
    #[tokio::test]
    async fn native_source_invalidation_failure_preserves_a_real_required_rescan() {
        fn cancelled_source_publication(
            physical: &SharedPhysicalRoot,
            work: &NativeMaterializationWork,
            cancellation: &CancellationToken,
        ) -> RootResult<()> {
            let mut watcher = physical
                .watcher
                .lock()
                .map_err(|_| RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "fixture original watcher state is poisoned",
                })?;
            watcher
                .fence_with_native_work(WATCH_FENCE_TIMEOUT, work)
                .map_err(native_watch_error)?;
            let observed = watcher.poll_with_native_work_guarded(65_536, work, |batch| {
                assert!(!matches!(batch, acyclic_fs::WatchBatch::Changes { changes, .. } if changes.is_empty()));
                cancellation.cancel();
                // Fail in the genuine native source invalidation, not an
                // invented error or a facade around the numeric poll API.
                physical.source.inner().invalidate(&mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(work)).map(|_| ())
            }).map_err(native_watch_error)?;
            drop(observed);
            Ok(())
        }

        let fixture = crate::pi_fork::tests::Fixture::new().await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"hint must not disappear on native failure",
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let failed = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        original_root_guard(&fixture, &failed).await.unwrap();
        let error = cancelled_source_publication(&physical, &failed, &cancellation)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        let prefix = failed.receipt();
        assert!(prefix.backend_read_operations > 0);
        assert_eq!(physical.source.reference(), prior);
        assert_eq!(failed.live_allocation_bytes(), 0);
        // This is a separate legitimate acquisition, never uncancelling or
        // resetting the exhausted original operation. Warm lazy restore must
        // execute the same real required-rescan transaction before exposing
        // this resident source, even without an explicit observation caller.
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let warm = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&warm, &physical));
        assert_eq!(warm.source.reference().identity, original.identity);
        assert!(warm.source.reference().epoch > prior.epoch);
        assert_eq!(failed.receipt(), prefix);
        drop(warm);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }
}
