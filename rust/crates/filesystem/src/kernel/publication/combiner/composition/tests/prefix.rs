use super::*;

struct Fixture {
    _directory: tempfile::TempDir,
    owner: Owner,
    first: Arc<ProposalCore>,
    second: Arc<ProposalCore>,
}

impl Fixture {
    async fn new(
        first_accounting: &mut KernelAccounting<'_>,
        second_accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let fs = crate::LocalFs::local(crate::LocalOptions::new(directory.path())).await?;
        let (first, _) = core(&fs, first_accounting, cancellation).await?;
        // A real same-content merge still creates an unpublished checkpoint.
        // Its distinct parent must be present for closure authentication.
        let parent = crate::kernel::GenerationRoot {
            parents: vec![crate::GenerationId::new(
                first.request.generation_root.digest,
            )],
            ..first.base.value.clone()
        };
        let encoded = crate::kernel::encode_generation_root(
            &parent,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?;
        let parent = crate::HashedObject::new(ObjectKind::GenerationRoot, encoded.into());
        let parent_id = parent.object_id();
        fs.objects()
            .put_hashed(
                parent,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation),
            )
            .await?;
        let second = proposal_core(
            &fs,
            PublishGenerationRequest {
                operation_id: OperationId::new(),
                ..first.request
            },
            first.preconditions.config,
            Some(parent_id),
            second_accounting,
            cancellation,
        )
        .await?;
        let gate = Arc::new(Gate {
            armed: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            fail: false,
        });
        Ok(Self {
            _directory: directory,
            owner: Owner(Arc::new(Objects { fs, gate })),
            first,
            second,
        })
    }
}

fn accounting<'a>(
    native: bool,
    work: &'a crate::NativeMaterializationWork,
    cancellation: &'a CancellationToken,
) -> KernelAccounting<'a> {
    if native {
        KernelAccounting::native(work, cancellation)
    } else {
        KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation)
    }
}

async fn check_prefix(native: bool, cancel: bool) -> TestResult {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let first_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let second_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let first_baseline = first_work.live_allocation_bytes();
    let second_baseline = second_work.live_allocation_bytes();
    let mut first_accounting = accounting(native, &first_work, &cancellation);
    let mut second_accounting = accounting(native, &second_work, &cancellation);
    let Fixture {
        _directory,
        owner,
        first: first_core,
        second: second_core,
    } = Fixture::new(&mut first_accounting, &mut second_accounting, &cancellation).await?;
    let released = owner
        .0
        .fs
        .local_root_release_barrier()
        .ok_or("root ownership")?;
    let gate = Arc::clone(&owner.0.gate);
    let weak = Arc::downgrade(&owner.0);
    let limits = crate::facade::closure_limits(first_core.preconditions.config);
    let (first, second) = futures::try_join!(
        Ticket::enter_core(
            &owner,
            first_core,
            (limits, None),
            &mut first_accounting,
            &cancellation
        ),
        Ticket::enter_core(
            &owner,
            second_core,
            (limits, None),
            &mut second_accounting,
            &cancellation
        ),
    )?;
    let mut first = first.ok_or("first admission")?;
    let mut second = second.ok_or("second admission")?;
    gate.armed.store(true, Ordering::SeqCst);
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    composition_events(&mut boundaries, async {
        let mut delivery = Box::pin(schedule.scope(futures::future::join(
            first.prepare_prefix(owner.clone(), limits, &mut first_accounting, &cancellation),
            second.prepare_prefix(owner.clone(), limits, &mut second_accounting, &cancellation),
        )));
        drop(owner);
        tokio::select! {
            () = gate.entered.notified() => {},
            result = &mut delivery => panic!("prefix returned before blocked I/O: {result:?}"),
        }
        if cancel {
            cancellation.cancel();
            let (first, second) = delivery.await;
            assert!(matches!(first, Err(crate::FsError::Cancelled(_))));
            assert!(matches!(second, Err(crate::FsError::Cancelled(_))));
        } else {
            drop(delivery);
        }
        drop(first);
        drop(second);
        assert!(
            weak.upgrade().is_some(),
            "reply withdrawal destroyed the selected-prefix owner during object I/O"
        );
        assert!(
            !released.is_released(),
            "reply withdrawal released the prefix's local root"
        );
        assert!(futures::poll!(std::pin::pin!(schedule.drained())).is_pending());
        if native {
            assert!(first_work.live_allocation_bytes() > first_baseline);
            assert!(second_work.live_allocation_bytes() > second_baseline);
        }
        gate.release.notify_one();
        schedule.drained().await;
        assert!(
            weak.upgrade().is_none(),
            "selected-prefix owner survived its physical task retirement"
        );
        released.released().await;
        if native {
            assert_eq!(first_work.live_allocation_bytes(), first_baseline);
            assert_eq!(second_work.live_allocation_bytes(), second_baseline);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn ordinary_owned_prefix_drop_preserves_all_members_and_the_actual_store_owner() -> TestResult
{
    check_prefix(false, false).await
}

#[tokio::test]
async fn native_owned_prefix_drop_preserves_all_members_and_the_actual_store_owner() -> TestResult {
    check_prefix(true, false).await
}

#[tokio::test]
async fn ordinary_owned_prefix_cancellation_preserves_all_members_and_the_actual_store_owner()
-> TestResult {
    check_prefix(false, true).await
}

#[tokio::test]
async fn native_owned_prefix_cancellation_preserves_all_members_and_the_actual_store_owner()
-> TestResult {
    check_prefix(true, true).await
}

async fn check_pins(native: bool) -> TestResult {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let first_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let second_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut first_accounting = accounting(native, &first_work, &cancellation);
    let mut second_accounting = accounting(native, &second_work, &cancellation);
    let Fixture {
        _directory,
        owner,
        first,
        second,
    } = Fixture::new(&mut first_accounting, &mut second_accounting, &cancellation).await?;
    let limits = crate::facade::closure_limits(first.preconditions.config);
    let request = first.request;
    let orphan = crate::kernel::GenerationRoot {
        volume_id: crate::VolumeId::new(),
        ..first.base.value.clone()
    };
    let (first, second) = futures::try_join!(
        Ticket::enter_core(
            &owner,
            first,
            (limits, None),
            &mut first_accounting,
            &cancellation
        ),
        Ticket::enter_core(
            &owner,
            second,
            (limits, None),
            &mut second_accounting,
            &cancellation
        ),
    )?;
    let mut first = first.ok_or("first admission")?;
    let mut second = second.ok_or("second admission")?;
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    composition_events(&mut boundaries, async {
        let (first_prefix, second_prefix) = schedule
            .scope(async {
                futures::try_join!(
                    first.prepare_prefix(
                        owner.clone(),
                        limits,
                        &mut first_accounting,
                        &cancellation
                    ),
                    second.prepare_prefix(
                        owner.clone(),
                        limits,
                        &mut second_accounting,
                        &cancellation
                    ),
                )
            })
            .await?;
        let first_root = first_prefix.ok_or("first prefix")?.0;
        let (second_root, predecessor) = second_prefix.ok_or("second prefix")?;
        assert_eq!(predecessor, Some(first_root));
        assert_ne!(
            second_root, request.generation_root,
            "test needs an unpublished prefix"
        );
        sweep_prepared_prefix(&owner.0.fs, &orphan, second_root, &cancellation).await?;
        assert_eq!(
            owner
                .authority()
                .head(
                    request.authority_id,
                    &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
                )
                .await?,
            request.expected
        );
        drop(first);
        drop(second);
        let weak = Arc::downgrade(&owner.0);
        drop(owner);
        schedule.drained().await;
        assert!(
            weak.upgrade().is_none(),
            "prepared-prefix owner survived its physical task retirement"
        );
        Ok(())
    })
    .await
}

async fn sweep_prepared_prefix(
    fs: &crate::LocalFs,
    orphan: &crate::kernel::GenerationRoot,
    prefix: ObjectId,
    cancellation: &CancellationToken,
) -> TestResult {
    let encoded = crate::kernel::encode_generation_root(
        orphan,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let orphan = crate::HashedObject::new(ObjectKind::GenerationRoot, encoded.into());
    let orphan_id = orphan.object_id();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
    fs.objects().put_hashed(orphan, &mut work).await?;
    drop(
        fs.objects()
            .flush_before_publish(crate::PublicationScope::Everything, &mut work)
            .await?,
    );
    let collected = fs.collect_local_garbage(None, cancellation).await?;
    assert!(
        collected.removed > 0,
        "collector did not sweep a durable orphan"
    );
    assert!(!fs.objects().contains(orphan_id, &mut work).await?);
    assert!(
        fs.objects().contains(prefix, &mut work).await?,
        "collector removed a selected prefix before the group's durability attempt retired"
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_owned_prefix_pin_survives_a_real_collection_before_durability() -> TestResult {
    check_pins(false).await
}

#[tokio::test]
async fn native_owned_prefix_pin_survives_a_real_collection_before_durability() -> TestResult {
    check_pins(true).await
}
