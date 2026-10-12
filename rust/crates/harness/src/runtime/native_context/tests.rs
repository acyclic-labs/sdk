use super::*;
use crate::durable_host::tests::created_resource_access::CreationFixture;

const BUDGET: acyclic_fs::WorkBudget =
    acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);

fn work(cancellation: &acyclic_fs::CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(BUDGET, cancellation).map_err(|failure| Error::NativeWork {
        cause: failure.0,
        custody: None,
    })
}

#[tokio::test]
async fn protected_tool_acceptance_enables_original_admission() -> Result<()> {
    use crate::durable_host::tests::created_resource_access::{
        NativeCustomerFixture, NativeCustomerRuntime,
    };
    let fixture =
        NativeCustomerFixture::new_with_grants(&[], crate::tool::ToolRegistry::default()).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    let fence =
        NativeCustomerRuntime::original_lease_fence_with_io(&fixture.creation.root_lease, &io)?;
    let shared = fixture
        .runtime
        .task_context_with_native_work(fixture.creation.root, &fence, &original)
        .await?;
    let tool = ToolContext::new(
        shared,
        crate::OperationId::from_bytes([122; 16]),
        "protected-admission",
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?;
    assert!(matches!(
        tool.original_admission_with_native_work(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    tool.record_native_tool_acceptance(&original)?;
    let (admission, _custody) = tool.original_admission_with_native_work(&original)?.await?;
    assert_eq!(
        admission.operation_id,
        fixture.creation.root_lease.operation.operation_id
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_context_cannot_be_qualified_by_an_opaque_allocation() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let context = fixture
        .harness
        .durable_context(fixture.root, fixture.root_lease.operation.operation_id)
        .await?;
    let before = original.receipt();
    assert!(matches!(
        context.require_original_native_work(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    let custody = original
        .claim(65_536, 1, acyclic_fs::WorkCounters::default())
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let before = original.receipt();
    let rejected = context.into_native_shared(custody, &original);
    assert!(matches!(
        rejected,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    assert_eq!(
        original.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn native_metadata_receipt_rejects_clone_foreign_and_short_custody() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    assert!(custody.bytes() > 0);
    let clone = context.clone();
    let opaque = {
        let claim_bytes = custody.bytes();
        original.claim(
            claim_bytes,
            u64::from(claim_bytes != 0),
            acyclic_fs::WorkCounters::default(),
        )
    }
    .map_err(|cause| Error::NativeWork {
        cause,
        custody: None,
    })?;
    let before = original.receipt();
    let cloned = clone.into_native_shared(opaque, &original);
    assert!(matches!(
        cloned,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    drop(context);
    drop(custody);

    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let foreign = work(&cancellation)?;
    let before = original.receipt();
    let rejected = context.into_native_shared(custody, &foreign);
    assert!(matches!(
        rejected,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);

    let (context, mut custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let source_bytes = context
        .native_metadata_source
        .as_ref()
        .expect("genuine native metadata source")
        .bytes;
    assert!(source_bytes > 0);
    let removed = custody.bytes() - (source_bytes - 1);
    let released = custody
        .split_off(removed)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let before = original.receipt();
    let rejected = context.into_native_shared(custody, &original);
    assert!(matches!(
        rejected,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    drop(released);
    Ok(())
}

#[tokio::test]
async fn cancelled_shared_context_preserves_original_cause_and_metadata_custody() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let bytes = custody.bytes();
    cancellation.cancel();
    let before = original.receipt();
    let rejected = context.into_native_shared(custody, &original);
    match rejected {
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: Some(custody),
        }) => {
            assert_eq!(custody.bytes(), bytes);
            assert!(custody.work().shares_allocation_context(&original));
            drop(custody);
        }
        _ => panic!("cancelled shared allocation must preserve the original cause and custody"),
    }
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[tokio::test]
async fn shared_native_content_uses_original_owner_and_keeps_real_output_custody() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?,
        crate::Admission::Accepted(admitted) if admitted == task)
    );
    let observation = fixture.host.observe_task(task).await?;
    let reservation = observation
        .reservation
        .ok_or_else(|| Error::NotFound("created native context lease".into()))?;
    let fence = crate::scheduler::LeaseFence::from(&reservation);
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(task, created.request.child.operation_id, &original)
        .await?;
    let shared = context.into_native_shared(custody, &original)?;
    let mcp_tool = ToolContext::new(
        Arc::clone(&shared),
        crate::OperationId::from_bytes([120; 16]),
        "native-mcp-control",
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?;
    let before = original.receipt();
    assert!(matches!(
        mcp_tool.validate_native_mcp_context(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        mcp_tool.validate_native_mcp_reconciliation_context(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        mcp_tool.original_admission_with_native_work(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    let foreign = work(&cancellation)?;
    let foreign_before = foreign.receipt();
    assert!(matches!(
        mcp_tool.validate_native_mcp_reconciliation_context(&foreign),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        mcp_tool.original_admission_with_native_work(&foreign),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), foreign_before);
    let unqualified = mcp_tool.clone();
    assert!(matches!(
        unqualified.validate_native_mcp_context(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        unqualified.validate_native_mcp_reconciliation_context(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        unqualified.original_admission_with_native_work(&original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    drop(unqualified);
    let owner = fixture
        .host
        .journal_owner_with_native_work(task, &fence, &original)
        .await?
        .into_native_shared(&original)?;
    let reader = crate::runtime::content_residency_verifier(
        Arc::clone(&shared),
        Arc::clone(&owner),
        Some(&crate::executor::PreparationIo::from_native_work(
            &original,
        )?),
    )?;
    let content_io = crate::executor::PreparationIo::from_native_work(&original)?;
    let staged = shared
        .as_ref()
        .stage_file_once_with_native_work(
            crate::OperationId::from_bytes([118; 16]),
            "native/shared.txt",
            b"same issued context",
            "text/plain",
            "shared.txt",
            &original,
        )
        .await?;
    let before = original.live_allocation_bytes();
    let reads = original.receipt().backend_read_operations;
    let bytes = reader.read(&staged.0, 65_536, Some(&content_io))?.await?;
    assert!(original.receipt().backend_read_operations > reads);
    assert_eq!(bytes.as_ref(), b"same issued context");
    assert!(bytes.matches_meter(&original.runtime_meter()));
    let held = original.live_allocation_bytes() - before;
    assert!(held > 0);
    let physical = original.receipt();
    let shared_bytes = bytes.clone();
    drop(bytes);
    assert_eq!(shared_bytes.as_ref(), b"same issued context");
    assert!(shared_bytes.matches_meter(&original.runtime_meter()));
    assert_eq!(original.live_allocation_bytes(), before + held);
    drop(shared_bytes);
    assert_eq!(original.live_allocation_bytes(), before);
    assert_eq!(original.receipt(), physical);
    reader.verify(&staged.0, 65_536, Some(&content_io))?.await?;

    let root_fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
    let root_owner = fixture
        .host
        .journal_owner_with_native_work(fixture.root, &root_fence, &original)
        .await?
        .into_native_shared(&original)?;
    let before = original.receipt();
    let wrong_owner = crate::runtime::content_residency_verifier(
        Arc::clone(&shared),
        Arc::clone(&root_owner),
        Some(&crate::executor::PreparationIo::from_native_work(
            &original,
        )?),
    );
    assert!(matches!(
        wrong_owner,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);

    let unmeasured = Arc::new(fixture.host.journal_owner(task, fence.clone()).await?);
    let before = original.receipt();
    let rejected = crate::runtime::content_residency_verifier(
        Arc::clone(&shared),
        unmeasured,
        Some(&crate::executor::PreparationIo::from_native_work(
            &original,
        )?),
    );
    assert!(matches!(
        rejected,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);

    let (root, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let root = root.into_native_shared(custody, &original)?;
    let root_reader = crate::runtime::content_residency_verifier(
        Arc::clone(&root),
        root_owner,
        Some(&crate::executor::PreparationIo::from_native_work(
            &original,
        )?),
    )?;
    let reads = original.receipt().backend_read_operations;
    assert!(matches!(
        root_reader
            .read(&staged.0, 65_536, Some(&content_io))?
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt().backend_read_operations, reads);

    let available = BUDGET.peak_allocation_bytes - original.live_allocation_bytes();
    let quota = original
        .claim(
            available,
            u64::from((available) != 0),
            acyclic_fs::WorkCounters::default(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let mut buffer = Vec::<u8>::new();
    buffer
        .try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
        .expect("finite fixture allocation");
    let blocker = (buffer, quota);
    let reads = original.receipt().backend_read_operations;
    let denied = reader.read(&staged.0, 65_536, Some(&content_io));
    assert!(matches!(
        denied,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        })
    ));
    assert_eq!(original.receipt().backend_read_operations, reads);
    drop(blocker);
    cancellation.cancel();
    let before = original.receipt();
    assert!(matches!(
        mcp_tool.validate_native_mcp_context(&original),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        mcp_tool.validate_native_mcp_reconciliation_context(&original),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        mcp_tool.original_admission_with_native_work(&original),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        reader.read(&staged.0, 65_536, Some(&content_io)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[tokio::test]
async fn native_tool_context_borrows_the_issued_context_and_invalidates_clone_funding() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let shared = context.into_native_shared(custody, &original)?;
    assert!(shared.native_work()?.shares_allocation_context(&original));
    shared.as_ref().require_original_native_work(&original)?;
    let operation = crate::OperationId::from_bytes([119; 16]);
    let call_id = operation.to_string();
    let baseline = original.live_allocation_bytes();
    let before = original.receipt();
    let tool = ToolContext::new(
        Arc::clone(&shared),
        operation,
        &call_id,
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?;

    assert_eq!(tool.operation_id(), operation);
    assert_eq!(tool.call_id(), call_id);
    assert!(tool.native_metadata_matches(&original));
    assert!(tool.stock_native_interactions().is_none());
    assert!(original.receipt().bytes_copied >= before.bytes_copied + call_id.len() as u64);
    let held = tool
        .native_custody
        .as_ref()
        .expect("genuine SDK tool metadata")
        .bytes();

    assert_eq!(original.live_allocation_bytes(), baseline + held);

    let before = original.receipt();
    let clone = tool.clone();

    assert!(!clone.native_metadata_matches(&original));
    assert_eq!(clone.call_id(), tool.call_id());
    assert_eq!(original.receipt(), before);
    drop(tool);
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(clone);

    let foreign = work(&cancellation)?;
    let before = foreign.receipt();
    assert!(matches!(
        ToolContext::new(
            Arc::clone(&shared),
            operation,
            &call_id,
            Some(&crate::filesystem::PreparationIo::from_native_work(
                &foreign
            )?)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), before);
    let before = original.receipt().allocation_operations;
    assert!(matches!(
        ToolContext::new(
            Arc::clone(&shared),
            operation,
            "bad/call",
            Some(&crate::filesystem::PreparationIo::from_native_work(
                &original
            )?)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));

    let available = BUDGET.peak_allocation_bytes - original.live_allocation_bytes();
    let quota = original
        .claim(
            available,
            u64::from((available) != 0),
            acyclic_fs::WorkCounters::default(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let mut buffer = Vec::<u8>::new();
    buffer
        .try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
        .expect("finite fixture allocation");
    let blocker = (buffer, quota);

    assert!(matches!(
        ToolContext::new(
            Arc::clone(&shared),
            operation,
            &call_id,
            Some(&crate::filesystem::PreparationIo::from_native_work(
                &original
            )?)
        ),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        })
    ));

    drop(blocker);
    let original_io = crate::filesystem::PreparationIo::from_native_work(&original)?;
    cancellation.cancel();
    let before = original.receipt();
    assert!(matches!(
        ToolContext::new(shared, operation, &call_id, Some(&original_io)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[tokio::test]
async fn native_tool_scope_restricts_without_replacing_the_issued_context_or_lineage() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?,
        crate::Admission::Accepted(admitted) if admitted == task)
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(task, created.request.child.operation_id, &original)
        .await?;
    let shared = context.into_native_shared(custody, &original)?;
    let read = created
        .private
        .capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = created
        .private
        .capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    assert!(shared.as_ref().scope().grants().contains(&write));
    let limits = shared.as_ref().scope().limits();
    let requested = RuntimeScope::new(crate::Capabilities::new([read.as_str()]), limits)?
        .with_run_limits(shared.as_ref().scope().run_limits(), &mut None)?;
    let operation = crate::OperationId::from_bytes([121; 16]);
    let tool = ToolContext::new(
        Arc::clone(&shared),
        operation,
        "scoped-control",
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?
    .with_native_scope(&requested, requested.limits(), &original)?;

    assert!(tool.effective_scope().grants().contains(&read));
    assert!(!tool.effective_scope().grants().contains(&write));
    assert!(shared.as_ref().scope().grants().contains(&write));
    assert_eq!(
        tool.effective_scope().run_limits(),
        shared.as_ref().scope().run_limits()
    );
    assert_eq!(
        tool.effective_scope().resource_proofs,
        shared.as_ref().scope().resource_proofs
    );

    assert!(requested.created_access.is_none());
    assert!(tool.native_metadata_matches(&original));
    let (_, guard) = tool.effective_scope.as_ref().expect("one effective scope");
    assert!(
        guard
            .as_ref()
            .expect("scope copy guard")
            .work()
            .shares_ledger(&original)
    );
    let before = original.receipt();
    tool.validate_stock_scope_with_native_work(&requested, limits, &original)?;
    assert!(matches!(
        tool.validate_stock_scope_with_native_work(shared.as_ref().scope(), limits, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    let mut execution_limits = limits;
    execution_limits.file_bytes = 1;
    assert!(limits.file_bytes > execution_limits.file_bytes);
    let stock = ToolContext::new(
        Arc::clone(&shared),
        operation,
        "stock-execution-limits",
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?
    .with_native_scope(&requested, execution_limits, &original)?;

    assert_eq!(stock.effective_scope().limits(), execution_limits);
    assert_eq!(requested.limits(), limits);
    assert_eq!(stock.effective_scope().grants(), requested.grants());
    assert_eq!(stock.effective_scope().run_limits(), requested.run_limits());
    assert!(stock.native_metadata_matches(&original));
    let before = original.receipt();
    assert!(matches!(
        stock.validate_stock_scope_with_native_work(&requested, limits, &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);
    drop(stock);

    let clone = tool.clone();
    assert_eq!(
        clone.effective_scope().grants(),
        tool.effective_scope().grants()
    );
    assert_eq!(
        clone.effective_scope().limits(),
        tool.effective_scope().limits()
    );
    assert!(!clone.native_metadata_matches(&original));
    let before = original.receipt();
    assert!(matches!(
        clone.validate_stock_scope_with_native_work(&requested, limits, &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(original.receipt(), before);

    drop(clone);

    let widening = RuntimeScope::new(
        crate::Capabilities::new([read.as_str(), write.as_str()]),
        limits,
    )?
    .with_run_limits(shared.as_ref().scope().run_limits(), &mut None)?;
    let candidate = ToolContext::new(
        Arc::clone(&shared),
        operation,
        "scope-widen",
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &original,
        )?),
    )?
    .with_native_scope(&requested, requested.limits(), &original)?;
    let allocations = original.receipt().allocation_operations;
    assert!(matches!(
        candidate.with_native_scope(&widening, widening.limits(), &original),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    let mut invalid = limits;
    invalid.file_bytes += 1;
    let allocations = original.receipt().allocation_operations;
    assert!(matches!(
        shared
            .as_ref()
            .validate_stock_scope_with_native_work(&requested, invalid, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized | crate::wire::ErrorCode::Invalid,
            ..
        })
    ));

    let run_widening = RuntimeScope::new(crate::Capabilities::new([read.as_str()]), limits)?;
    assert!(shared.as_ref().scope().run_limits().concurrency.is_some());
    let allocations = original.receipt().allocation_operations;
    assert!(matches!(
        shared
            .as_ref()
            .validate_stock_scope_with_native_work(&run_widening, limits, &original,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));

    Ok(())
}

#[tokio::test]
async fn shared_context_requires_heap_headroom() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = work(&cancellation)?;
    let (context, custody) = fixture
        .harness
        .durable_context_with_native_work(
            fixture.root,
            fixture.root_lease.operation.operation_id,
            &original,
        )
        .await?;
    let remaining = original.remaining().map_err(Error::from)?;
    let _occupied = original
        .claim(
            remaining.peak_allocation_bytes,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )
        .map_err(Error::from)?;
    assert!(matches!(
        context.into_native_shared(custody, &original),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            custody: Some(_),
        })
    ));
    Ok(())
}
