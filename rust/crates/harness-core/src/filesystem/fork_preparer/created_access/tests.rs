use super::*;
use crate::durable_host::tests::created_fixture::CreationFixture;
use acyclic_test_support::differential::{self, Operation};

fn binding_count(trace: Vec<Operation>) -> usize {
    trace
        .into_iter()
        .filter(|(name, _)| *name == "created request binding")
        .count()
}

fn compare_binding<'a>(
    runtime: &tokio::runtime::Runtime,
    fixture: &CreationFixture,
    request: &'a ResourcePreparationRequest,
    proof: &'a ResourceCreationProofRef,
    io: &PreparationIo,
    owner: &Authority,
) -> Result<CreationRequest<'a>> {
    let (bound, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| {
            runtime.block_on(CreationRequest::new(
                &fixture.filesystem,
                request,
                proof,
                io,
            ))
        },
    );
    let bound = bound?;
    assert_eq!(binding_count(trace), 1);
    let (outcomes, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| {
            runtime.block_on(async {
                let mut volumes = Vec::new();
                for _ in 0..2 {
                    volumes.push(
                        bound
                            .authenticate(&fixture.filesystem, owner, Some(io))
                            .await?
                            .volume,
                    );
                }
                Result::Ok(volumes)
            })
        },
    );
    let outcomes = outcomes?;
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(
        binding_count(trace),
        0,
        "reads must consume the original binding"
    );
    let (old, trace) = differential::observe::<_, _, Operation>(
        || (),
        |_| {
            runtime.block_on(async {
                let repeated =
                    CreationRequest::new(&fixture.filesystem, request, proof, io).await?;
                repeated
                    .authenticate(&fixture.filesystem, owner, Some(io))
                    .await
                    .map(|v| v.volume)
            })
        },
    );
    assert_eq!(old?, outcomes[0]);
    assert_eq!(
        binding_count(trace),
        1,
        "negative control reconstructs the immutable input"
    );

    Ok(bound)
}

fn retained_creation_request(native: bool) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let fixture = runtime.block_on(CreationFixture::new())?;
    let created = runtime.block_on(fixture.create_filesystem_child())?;
    let request = &created.request;
    let proof = request
        .child
        .resource_proofs
        .first()
        .ok_or_else(|| Error::Invalid("fixture has no creation proof".into()))?;
    let cancel = CancellationToken::new();
    let work = native
        .then(|| {
            acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
                &cancel,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
        })
        .transpose()?;
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work)?,
        None => PreparationIo::new(request, &cancel)?,
    };
    let owner = &Authority {
        kind: crate::core::AggregateKind::Task,
        id: "created-owner".into(),
    };
    let bound = compare_binding(&runtime, &fixture, request, proof, &io, owner)?;
    negative_boundaries(&runtime, &fixture, &bound, request, proof, &io, owner)?;
    changed_allocation(
        &runtime,
        &fixture,
        &bound,
        request,
        &io,
        owner,
        created.seed.operation_id,
    )?;
    let restored = runtime.block_on(CreationRequest::new(
        &fixture.filesystem,
        request,
        proof,
        &io,
    ))?;
    changed_journal(
        &runtime,
        &fixture,
        &restored,
        &io,
        owner,
        created.seed.operation_id,
    )?;
    let rebound = runtime.block_on(CreationRequest::new(
        &fixture.filesystem,
        request,
        proof,
        &io,
    ))?;
    runtime.block_on(rebound.authenticate(&fixture.filesystem, owner, Some(&io)))?;
    io.remaining()?;
    cancel.cancel();
    assert!(matches!(
        runtime.block_on(bound.authenticate(&fixture.filesystem, owner, Some(&io))),
        Err(Error::NativeFilesystem { source, .. })
            if matches!(source.as_ref(), acyclic_fs::FsError::Cancelled(_)
                | acyclic_fs::FsError::NativeKernel(acyclic_fs::NativeTreeMaterializationError::Cancelled))
    ));
    Ok(())
}

fn negative_boundaries(
    runtime: &tokio::runtime::Runtime,
    fixture: &CreationFixture,
    bound: &CreationRequest<'_>,
    request: &ResourcePreparationRequest,
    proof: &ResourceCreationProofRef,
    io: &PreparationIo,
    owner: &Authority,
) -> Result<()> {
    let mut foreign = (*owner).clone();
    foreign.id.push_str("-foreign");
    assert!(matches!(
        runtime.block_on(bound.authenticate(&fixture.filesystem, &foreign, Some(io))),
        Err(Error::Unauthorized(_))
    ));
    let other = FilesystemHost::new(acyclic_fs::Fs::memory(), &fixture.filesystem_provider, None)?;
    let missing = runtime
        .block_on(bound.authenticate(&other, owner, Some(io)))
        .err()
        .ok_or_else(|| Error::Invalid("foreign store was accepted".into()))?;
    assert!(
        matches!(
            &missing,
            Error::NativeWorkspace { source, .. }
                if matches!(source.as_ref(), acyclic_fs::WorkspaceError::Filesystem(
                    acyclic_fs::FsError::Authority(acyclic_fs::AuthorityStoreError::Missing)))
        ),
        "retained parsing must never stand in for the protected journal: {missing:?}"
    );
    let mut forged = request.clone();
    forged.child.operation_id = OperationId::from_bytes([7; 16]);
    let forged = runtime.block_on(CreationRequest::new(
        &fixture.filesystem,
        &forged,
        proof,
        io,
    ))?;
    assert!(matches!(
        runtime.block_on(forged.authenticate(&fixture.filesystem, owner, Some(io))),
        Err(Error::Unauthorized(_))
    ));
    let mut forged = proof.clone();
    forged.evidence = forged.resource.clone();
    assert!(matches!(
        runtime.block_on(CreationRequest::new(
            &fixture.filesystem,
            request,
            &forged,
            io
        )),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

fn changed_journal(
    runtime: &tokio::runtime::Runtime,
    fixture: &CreationFixture,
    bound: &CreationRequest<'_>,
    io: &PreparationIo,
    owner: &Authority,
    operation: OperationId,
) -> Result<()> {
    let journal = preparation_journal(&fixture.filesystem_provider, operation, Some(io))?;
    runtime.block_on(fixture.filesystem.apply(
        &journal,
        None,
        &[WorkspaceMutation::PutFile {
            path: "/changed-generation".into(),
            bytes: b"changed".to_vec().into(),
        }],
        &IdempotencyKey::new("changed-generation")?,
    ))?;
    let refused = runtime
        .block_on(bound.authenticate(&fixture.filesystem, owner, Some(io)))
        .err()
        .ok_or_else(|| Error::Invalid("changed journal was accepted".into()))?;
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    Ok(())
}

fn changed_allocation(
    runtime: &tokio::runtime::Runtime,
    fixture: &CreationFixture,
    bound: &CreationRequest<'_>,
    request: &ResourcePreparationRequest,
    io: &PreparationIo,
    owner: &Authority,
    operation: OperationId,
) -> Result<()> {
    let proof = request
        .child
        .resource_proofs
        .first()
        .ok_or_else(|| Error::Invalid("fixture has no creation proof".into()))?;
    let journal = preparation_journal(&fixture.filesystem_provider, operation, Some(io))?;
    let original = runtime.block_on(fixture.filesystem.read(
        &journal,
        None,
        ALLOCATION_RECORD,
        MAX_REPORT_BYTES,
        None,
    ))?;
    let mut claim: crate::filesystem::fork_preparer::AllocationClaim =
        crate::contract::json_from_slice(&original)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    claim
        .task_origin
        .as_mut()
        .ok_or_else(|| Error::Invalid("fixture claim has no task origin".into()))?
        .owner
        .id
        .push_str("-changed");
    let changed = serde_json::to_vec(&claim).map_err(|error| Error::Invalid(error.to_string()))?;
    runtime.block_on(fixture.filesystem.apply(
        &journal,
        None,
        &[WorkspaceMutation::PutFile {
            path: ALLOCATION_RECORD.into(),
            bytes: changed.into(),
        }],
        &IdempotencyKey::new("change-allocation-owner")?,
    ))?;
    let refused = runtime
        .block_on(bound.authenticate(&fixture.filesystem, owner, Some(io)))
        .err()
        .ok_or_else(|| Error::Invalid("step=changed allocation generation accepted; recovery: cargo test --locked -p acyclic-harness-core --all-features --lib filesystem::fork_preparer::created_access::tests".into()))?;
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    // A fresh cut must reject the changed allocation owner too, independently
    // of the retained binding's generation fence.
    let changed = runtime.block_on(CreationRequest::new(
        &fixture.filesystem,
        request,
        proof,
        io,
    ))?;
    let refused = runtime
        .block_on(changed.authenticate(&fixture.filesystem, owner, Some(io)))
        .err()
        .ok_or_else(|| Error::Invalid("step=changed allocation owner accepted; recovery: cargo test --locked -p acyclic-harness-core --all-features --lib filesystem::fork_preparer::created_access::tests".into()))?;
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    runtime.block_on(fixture.filesystem.apply(
        &journal,
        None,
        &[WorkspaceMutation::PutFile {
            path: ALLOCATION_RECORD.into(),
            bytes: original,
        }],
        &IdempotencyKey::new("restore-allocation-owner")?,
    ))?;
    let refused = runtime
        .block_on(bound.authenticate(&fixture.filesystem, owner, Some(io)))
        .err()
        .ok_or_else(|| Error::Invalid("step=stale allocation generation accepted; recovery: cargo test --locked -p acyclic-harness-core --all-features --lib filesystem::fork_preparer::created_access::tests".into()))?;
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    let restored = runtime.block_on(CreationRequest::new(
        &fixture.filesystem,
        request,
        proof,
        io,
    ))?;
    runtime.block_on(restored.authenticate(&fixture.filesystem, owner, Some(io)))?;
    Ok(())
}

#[test]
fn created_request_has_one_owner_and_rechecks_live_evidence() -> Result<()> {
    retained_creation_request(false)
}

#[test]
fn native_created_request_has_one_owner_and_rechecks_live_evidence() -> Result<()> {
    retained_creation_request(true)
}
