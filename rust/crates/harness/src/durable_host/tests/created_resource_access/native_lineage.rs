use super::*;

#[cfg(not(target_arch = "wasm32"))]
async fn native_fixture_scope(
    fixture: &CreationFixture,
    operation: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(
    TaskAdmissionRecord,
    RuntimeScope,
    acyclic_fs::NativeAllocation,
)> {
    let mut io = crate::store::history::native_work_io(work)?;
    let admission = fixture
        .host
        .admission_with_work(operation, &mut Some(&mut io))
        .await?;
    let (scope, scope_held) = fixture
        .host
        .authenticated_scope_with_work(&admission, &mut Some(&mut io))
        .await?;
    let record_held = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
        .checked_add(admission.logical_owned_bytes()?)
        .expect("exact retained fixture record");
    let custody = io.take_native_custody(
        record_held
            .checked_add(scope_held)
            .expect("record and scope retain separate exact allowances"),
    )?;
    assert!(custody.work().shares_allocation_context(work));
    Ok((admission, scope, custody))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_original_root_child_grandchild_keep_exact_created_source() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes(created.request.child.operation_id.into_bytes());
    let read = created
        .private
        .capability(VolumeOperation::Read, &mut None)?;
    let write = created
        .private
        .capability(VolumeOperation::Write, &mut None)?;
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([97; 16]);
    descendant.parent = Some(child);
    descendant.grants = Capabilities::new([read.clone()]);
    descendant.limits.file_bytes = 1_024;
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(child, &mut None).await?
    else {
        return Err(Error::NotFound("native created parent lease".into()));
    };
    fixture
        .host
        .admit_owned(descendant.clone(), (&lease.reservation).into())
        .await?;
    fixture.start_next(TaskId::from_bytes([97; 16])).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )
    .unwrap();
    let live = work.live_allocation_bytes();
    let root = native_fixture_scope(
        &fixture,
        OperationId::from_bytes(fixture.root.into_bytes()),
        &work,
    )
    .await?;
    assert!(root.0.parent.is_none());
    assert!(root.0.resource_proofs.is_empty());
    assert!(
        root.1
            .current_created_agent(&mut Some(
                &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None
                })?
            ))?
            .is_none()
    );
    assert!(!root.1.grants().contains(&read));
    assert!(root.2.work().shares_allocation_context(&work));
    let current = native_fixture_scope(&fixture, created.request.child.operation_id, &work).await?;
    assert!(current.0 == created.request.child);
    assert_eq!(
        current.1.current_created_agent(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(created.seed.child_agent)
    );
    assert_eq!(
        current.1.current_created_authority(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(&created.seed.child)
    );
    assert_eq!(
        current.1.current_created_private_volume(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(&created.private)
    );
    assert!(current.1.grants().contains(&write));
    let inherited = native_fixture_scope(&fixture, descendant.operation_id, &work).await?;
    assert!(inherited.0 == descendant);
    assert_eq!(
        inherited.1.current_created_agent(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(created.seed.child_agent)
    );
    assert_eq!(
        inherited.1.current_created_authority(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(&created.seed.child)
    );
    assert!(inherited.1.grants().contains(&read));
    assert!(!inherited.1.grants().contains(&write));
    let readonly = current.1.narrow_with_native_work(
        Capabilities::new_with_native_work([read.as_str()], &work)?,
        current.1.limits(),
        &work,
    )?;
    assert!(!readonly.0.grants().contains(&write));
    assert!(
        readonly
            .0
            .narrow_with_native_work(
                Capabilities::new_with_native_work([write.as_str()], &work)?,
                readonly.0.limits(),
                &work,
            )
            .is_err()
    );
    let mut io = crate::store::history::native_work_io(&work)?;
    fixture
        .host
        .validate_parent_scope_with_work(Some(child), &readonly.0, &mut Some(&mut io))
        .await?;
    drop(io);
    drop(readonly);
    drop(inherited);
    drop(current);
    drop(root);
    assert_eq!(work.live_allocation_bytes(), live);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(!fixture.root_scope.grants().contains(&write));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_original_proof_cycle_and_foreign_revision_never_seal_access() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    for change in 0..4 {
        let mut admission = created.request.child.clone();
        match change {
            0 => {
                admission.resource_proofs[0].provider = ProviderRef::new(
                    "created-controls".into(),
                    "filesystem".into(),
                    "foreign".into(),
                    &mut None,
                )?
            }
            1 => {
                admission.resource_proofs[0].evidence =
                    workspace_ref(fixture.filesystem_provider.clone(), "foreign-evidence")?
                        .as_resource()
                        .clone()
            }
            2 => admission.resource_proofs[0].preparation = TaskId::from_bytes([85; 16]),
            _ => admission.parent = None,
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &cancellation,
        )
        .unwrap();
        let input_custody = work.claim(
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut io = crate::store::history::native_work_io(&work)?;
        let result = fixture
            .host
            .authenticated_scope_with_work(&admission, &mut Some(&mut io))
            .await;
        assert!(result.is_err());
        assert!(result.as_ref().err().unwrap().is_native_failure());
        drop(result);
        drop(io);
        drop(admission);
        drop(input_custody);
        assert!(
            fixture
                .host
                .admission_with_work(
                    OperationId::from_bytes(fixture.root.into_bytes()),
                    &mut None
                )
                .await?
                .parent
                .is_none()
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_stock_source_provenance_and_cancelled_preparation_are_not_authority() -> Result<()>
{
    for change in 0..5 {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        fixture.admit_child(created.request.child.clone()).await?;
        let claim = workspace_ref(
            fixture.filesystem_provider.clone(),
            &format!(
                "fork-allocation-{}",
                created.private.storage_name(&mut None)?
            ),
        )?;
        let bytes = fixture
            .filesystem
            .read(&claim, None, "/claim.json", 65_536)
            .await?;
        let mut source: Value = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        match change {
            0 => source["task_origin"]["owner"]["id"] = serde_json::json!("foreign-owner"),
            1 => {
                source["task_origin"]["preparation_task"] =
                    serde_json::to_value(TaskId::from_bytes([85; 16])).unwrap()
            }
            2 => {
                source["task_origin"]["intended_child"] =
                    serde_json::to_value(TaskId::from_bytes([85; 16])).unwrap()
            }
            3 => source["task_origin"]["request_digest"] = serde_json::to_value([0u8; 32]).unwrap(),
            _ => {
                source["task_origin"]["admission_digest"] = serde_json::to_value([0u8; 32]).unwrap()
            }
        }
        fixture
            .filesystem
            .apply(
                &claim,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/claim.json".into(),
                    bytes: crate::contract::canonical_json_bytes(&source)?.into(),
                }],
                &IdempotencyKey::new("native-corrupt-source-provenance")?,
            )
            .await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
            &cancellation,
        )
        .unwrap();
        let live = work.live_allocation_bytes();
        let error = native_fixture_scope(&fixture, created.request.child.operation_id, &work).await;
        assert!(error.is_err());
        assert!(error.as_ref().err().unwrap().is_native_failure());
        assert!(work.receipt().backend_read_operations > 0);
        drop(error);
        assert_eq!(work.live_allocation_bytes(), live);
        assert!(
            fixture
                .host
                .admission_with_work(
                    OperationId::from_bytes(fixture.root.into_bytes()),
                    &mut None
                )
                .await?
                .parent
                .is_none()
        );
    }
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture
        .host
        .cancel(TaskId::from_bytes([83; 16]), None)
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )
    .unwrap();
    let mut io = crate::store::history::native_work_io(&work)?;
    let error = fixture
        .host
        .authenticated_scope_with_work(&created.request.child, &mut Some(&mut io))
        .await;
    assert!(matches!(
        error,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    drop(error);
    drop(io);
    assert!(
        fixture
            .host
            .admission_with_work(
                OperationId::from_bytes(fixture.root.into_bytes()),
                &mut None
            )
            .await?
            .parent
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_declared_original_scope_mac_denial_precedes_admission_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let operation = OperationId::from_bytes(fixture.root.into_bytes());
    let mut declaration = fixture
        .host
        .retained_declaration_with_work(operation, &mut None)
        .await?
        .expect("genuine previously acknowledged root");
    let foreign = crate::core::AuthorityIssuer::new(
        "created-controls-foreign",
        [93; 32],
        fixture.host.verifier.audience().clone(),
    );
    declaration.owner_scope = foreign
        .issue(
            "foreign-declaration",
            &Capabilities::new(["operation:declare", "operation:observe"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &cancellation,
    )
    .expect("original finite scope authentication ledger");
    let mut io = crate::store::history::native_work_io(&work)?;
    let before = work.receipt();
    let denial = fixture
        .host
        .read_declared_admission_counted(&declaration, &mut Some(&mut io))
        .await
        .map(|(record, _)| record);
    assert!(denial.is_err());
    assert!(
        denial
            .as_ref()
            .err()
            .expect("foreign original Scope MAC")
            .is_native_failure()
    );
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    drop(denial);
    drop(io);
    let (root, custody) = fixture
        .host
        .observe_admission_with_native_work(TaskId::from_bytes(operation.into_bytes()), &work)
        .await?;
    assert!(custody.work().shares_allocation_context(&work));
    assert_eq!(root.parent, None);
    assert!(root.resource_proofs.is_empty());
    Ok(())
}
