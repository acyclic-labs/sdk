use super::*;

struct OpaqueRouter;
impl InteractionRouter for OpaqueRouter {
    fn route<'a>(
        &'a self,
        _: OperationId,
        _: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        panic!("ordinary route must not run");
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_scope_keeps_independent_input_custody_and_scalar_denial_codes() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 2,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
        bytes_copied: 4,
        items_examined: 1,
        ..WorkBudget::default()
    };
    let original =
        NativeMaterializationWork::new(budget, &cancellation).expect("original grants ledger");
    let construction = NativeMaterializationWork::new(budget, &cancellation)
        .expect("independent construction ledger");
    let paid = Capabilities::copy(["read"], &original).expect("paid input");
    let (scope, custody) =
        RuntimeScope::new_with_native_work(paid, Limits::default(), &construction)
            .expect("independent guards are legitimate");
    assert!(
        scope
            ._native_grants_custody
            .as_ref()
            .unwrap()
            .work()
            .shares_ledger(&original)
    );
    assert!(custody.work().shares_ledger(&construction));
    let invalid = Limits {
        file_bytes: 0,
        ..Limits::default()
    };
    assert!(matches!(
        crate::filesystem::PreparationIo::from_native_work(&construction)
            .map(crate::store::HistoryWork::new)
            .and_then(|mut io| (&invalid).validate(&mut Some(&mut io))),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    let scope = crate::filesystem::PreparationIo::from_native_work(&construction)
        .map(crate::store::HistoryWork::new)
        .and_then(|mut io| {
            scope.with_run_limits(
                TaskRunLimits {
                    concurrency: Some(1),
                    ..TaskRunLimits::default()
                },
                &mut Some(&mut io),
            )
        })
        .expect("bounded concurrency");
    assert!(matches!(
        crate::filesystem::PreparationIo::from_native_work(&construction)
            .map(crate::store::HistoryWork::new)
            .and_then(|mut io| scope.with_run_limits(TaskRunLimits::default(), &mut Some(&mut io))),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    drop(custody);
    cancellation.cancel();
    assert!(matches!(
        crate::filesystem::PreparationIo::from_native_work(&construction)
            .map(crate::store::HistoryWork::new)
            .and_then(|mut io| (&Limits::default()).validate(&mut Some(&mut io))),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_scope_clone_and_narrow_preserve_readonly_grants_and_original_bounds() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 8,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 8192,
            bytes_copied: 64,
            items_examined: 16,
            ..WorkBudget::default()
        },
        &cancellation,
    )
    .expect("original ledger");
    let (root, root_custody) = RuntimeScope::new_with_native_work(
        Capabilities::copy(["read"], &work).expect("paid read-only grants"),
        Limits::default(),
        &work,
    )
    .expect("read-only root");
    let root = crate::filesystem::PreparationIo::from_native_work(&work)
        .map(crate::store::HistoryWork::new)
        .and_then(|mut io| {
            root.with_run_limits(
                TaskRunLimits {
                    max_steps: Some(2),
                    ..TaskRunLimits::default()
                },
                &mut Some(&mut io),
            )
        })
        .expect("original execution bound");
    let (copy, copy_custody) = root.clone_with_native_work(&work).expect("native clone");
    assert_eq!(copy.grants(), root.grants());
    assert_eq!(copy.run_limits(), root.run_limits());
    assert!(!copy.is_sealed());
    assert!(
        copy._native_grants_custody
            .as_ref()
            .unwrap()
            .work()
            .shares_ledger(&work)
    );
    let narrower = Limits {
        file_bytes: 1,
        ..Limits::default()
    };
    let (child, child_custody) = root
        .narrow_with_native_work(
            Capabilities::copy(["read"], &work).expect("paid child grants"),
            narrower,
            &work,
        )
        .expect("native attenuation");
    assert!(child.is_sealed());
    assert_eq!(child.limits(), narrower);
    assert_eq!(child.run_limits(), root.run_limits());
    assert!(!child.grants().contains("write"));
    assert!(matches!(
        root.narrow_with_native_work(
            Capabilities::copy(["write"], &work).expect("requested widening"),
            Limits::default(),
            &work,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(root.grants().iter().collect::<Vec<_>>(), ["read"]);
    assert_eq!(root.limits(), Limits::default());
    drop(child);
    drop(child_custody);
    drop(copy);
    drop(copy_custody);
    drop(root);
    drop(root_custody);
}

#[test]
fn native_task_requirements_keep_provider_grants_bound() -> Result<()> {
    let definition = TaskDefinition::live("provider-bound", "1", |_context, _input: ()| async {
        Ok::<(), Error>(())
    })?
    .requires("model")?
    .requires("context")?
    .requires("interactions")?;
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let all_grants = ["model:generate", "context:build", "interaction:route"];
    let mut bindings = Bindings::local();
    bindings.tasks = tasks.clone();
    bindings.interactions = Some(Arc::new(OpaqueRouter));
    bindings.scope = RuntimeScope::new(Capabilities::new(all_grants), Limits::default())?;
    assert!(bindings.build(None).is_ok());
    for missing_grant in all_grants {
        let mut bindings = Bindings::local();
        bindings.tasks = tasks.clone();
        bindings.interactions = Some(Arc::new(OpaqueRouter));
        bindings.scope = RuntimeScope::new(
            Capabilities::new(
                all_grants
                    .into_iter()
                    .filter(|grant| *grant != missing_grant),
            ),
            Limits::default(),
        )?;
        assert!(bindings.build(None).is_err());
    }
    Ok(())
}

#[test]
fn native_task_requirements_accept_state_without_spawner() -> Result<()> {
    let state_task = TaskDefinition::live("state-only", "1", |_context, _input: ()| async {
        Ok::<(), Error>(())
    })?
    .requires("state")?;
    let mut bindings = Bindings::local();
    bindings.tasks.register(state_task)?;
    bindings.state = Some(Arc::new(SplitState {
        task_id: TaskId::new(),
        record: TaskAdmissionRecord::from_canonical_value(
            serde_json::from_str(include_str!("../../../fixtures/v2/task-admission.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            &mut None,
        )?
        .0,
    }));
    assert!(bindings.build(None).is_ok());
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_policy_approval_keeps_exact_binding_and_paid_utf8_prompt() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("original policy ledger");
    let (policy, definition, invocation) = native_policy_fixture_inputs();
    let scope = RuntimeScope::new(
        Capabilities::new(["tool:call:test.echo"]),
        Limits::default(),
    )?;
    let (identity, custody) = policy.identity_with_native_work(&work)?;
    check_policy_metadata(&identity, &custody, &work)?;
    validate_policy_identity(
        &identity,
        Some(&crate::executor::PreparationIo::from_native_work(&work)?),
    )?;
    drop((identity, custody));
    let (decision, custody) = policy
        .evaluate_with_native_work(&invocation, &scope, &work)?
        .await?;
    let ToolPolicyDecision::RequireApproval { prompt } = decision else {
        panic!("fixture approval");
    };
    assert_eq!(custody.bytes(), prompt.capacity() as u64);
    let task = TaskId::new();
    let expected = tool_approval_request(
        task,
        &policy.identity,
        &definition,
        &invocation,
        (prompt.clone(), None),
        None,
    )
    .map(|(value, _)| value)?;
    let (actual, custody) = tool_approval_request(
        task,
        &policy.identity,
        &definition,
        &invocation,
        (prompt, Some(custody)),
        Some(&work),
    )?;
    let custody = custody.expect("native request retains its prompt");
    assert_eq!(actual, expected);
    assert!(custody.work().shares_ledger(&work));
    let paid = (actual, custody);
    let live = work.live_allocation_bytes();
    assert!(
        live >= acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + paid.1.bytes()
    );
    drop(paid);
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_policy_identity_denies_cancelled_and_exhausted_copy_before_strings() -> Result<()> {
    let (policy, _, _) = native_policy_fixture_inputs();
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::WorkBudget::default();
    budget.bytes_copied = 0;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    assert!(matches!(
        policy.identity_with_native_work(&work),
        Err(Error::NativeWork { .. })
    ));
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("cancelled-policy original ledger");
    cancellation.cancel();
    assert!(matches!(
        policy.identity_with_native_work(&work),
        Err(Error::NativeWork { .. })
    ));
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_policy_approval_rejects_foreign_prompt_before_encoding() -> Result<()> {
    use crate::store::history::HistoryIo;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("original ledger");
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("foreign ledger");
    let (policy, definition, invocation) = native_policy_fixture_inputs();
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&foreign)?,
    );
    let prompt = io.clone_text("Foreign prompt")?;
    let custody = io.into_custody()?;
    assert!(matches!(
        tool_approval_request(
            TaskId::new(),
            &policy.identity,
            &definition,
            &invocation,
            (prompt, Some(custody)),
            Some(&original)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            ..
        })
    ));
    assert_eq!(
        foreign.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn opaque_policy_native_defaults_never_call_ordinary_body() -> Result<()> {
    struct OpaquePolicy;
    impl ToolPolicy for OpaquePolicy {
        fn identity(&self) -> ComponentIdentity {
            panic!("ordinary identity must not run");
        }
        fn evaluate<'a>(
            &'a self,
            _: &'a ToolInvocation,
            _: &'a RuntimeScope,
        ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
            panic!("ordinary evaluator must not run");
        }
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("original ledger");
    let (_, _, invocation) = native_policy_fixture_inputs();
    let scope = RuntimeScope::new(
        Capabilities::new(["tool:call:test.echo"]),
        Limits::default(),
    )?;
    assert!(matches!(
        OpaquePolicy.supports_native_policy(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(matches!(
        OpaquePolicy.identity_with_native_work(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(matches!(
        OpaquePolicy.evaluate_with_native_work(&invocation, &scope, &work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn opaque_interaction_router_rejects_native_before_ordinary_route_or_box() -> Result<()> {
    use crate::store::history::HistoryIo;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .expect("original interaction ledger");
    assert!(matches!(
        OpaqueRouter.supports_native_interactions(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&work)?);
    let prompt = io.clone_text("Approve ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â½ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»?")?;
    let request = Interaction::Approval {
        prompt,
        operation_id: OperationId::new(),
        action_digest: [1; 32],
    };
    let custody = io.into_custody().expect("native custody");
    assert!(matches!(
        OpaqueRouter.route_with_native_work(OperationId::new(), request, custody, &work,),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
