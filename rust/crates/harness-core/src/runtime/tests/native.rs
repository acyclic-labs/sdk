use super::*;

pub(super) struct OpaqueRouter;
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl InteractionRouter for OpaqueRouter {
    async fn route(&self, _: OperationId, _: Interaction) -> Result<InteractionOutcome> {
        panic!("ordinary route must not run");
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ToolPolicy for OpaqueRouter {
    fn identity(&self) -> ComponentIdentity {
        panic!("ordinary identity must not run");
    }
    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        _: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        panic!("native identity preflight must reject this policy")
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_work(
    cancellation: &acyclic_fs::CancellationToken,
) -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(256, 65_536),
        cancellation,
    )
    .expect("fixture ledger")
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_scope_keeps_independent_input_custody_and_scalar_denial_codes() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 2,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
        bytes_copied: 4,
        items_examined: 1,
        ..WorkBudget::default()
    };
    let original = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let construction = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (scope, custody) = RuntimeScope::new_with_native_work(
        Capabilities::copy(["read"], &original)?,
        Limits::default(),
        &construction,
    )?;
    assert!(
        scope
            ._native_grants_custody
            .as_ref()
            .unwrap()
            .work()
            .shares_ledger(&original)
    );
    assert!(custody.work().shares_ledger(&construction));
    let io = crate::filesystem::PreparationIo::from_native_work(&construction)?;
    let scope = crate::executor::with_history_io(Some(&io), |work| {
        scope.with_run_limits(
            TaskRunLimits {
                concurrency: Some(1),
                ..TaskRunLimits::default()
            },
            work,
        )
    })?;
    assert!(matches!(
        crate::executor::with_history_io(Some(&io), |work| scope
            .with_run_limits(TaskRunLimits::default(), work)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    cancellation.cancel();
    assert!(matches!(
        RuntimeScope::default().clone_with_native_work(&construction),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_scope_clone_and_narrow_preserve_readonly_grants_and_original_bounds() -> Result<()> {
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
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (root, _root_custody) = RuntimeScope::new_with_native_work(
        Capabilities::copy(["read"], &work)?,
        Limits::default(),
        &work,
    )?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let root = crate::executor::with_history_io(Some(&io), |work| {
        root.with_run_limits(
            TaskRunLimits {
                max_steps: Some(2),
                ..TaskRunLimits::default()
            },
            work,
        )
    })?;
    let (copy, _copy_custody) = root.clone_with_native_work(&work)?;
    assert_eq!(copy.grants(), root.grants());
    assert_eq!(copy.run_limits(), root.run_limits());
    assert!(!copy.is_sealed());
    let narrower = Limits {
        file_bytes: 1,
        ..Limits::default()
    };
    let (child, _child_custody) =
        root.narrow_with_native_work(Capabilities::copy(["read"], &work)?, narrower, &work)?;
    assert!(child.is_sealed());
    assert_eq!(child.limits(), narrower);
    assert_eq!(child.run_limits(), root.run_limits());
    assert!(!child.grants().contains("write"));
    assert!(matches!(
        root.narrow_with_native_work(
            Capabilities::copy(["write"], &work)?,
            Limits::default(),
            &work,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    Ok(())
}

#[test]
fn native_task_requirements_keep_provider_grants_bound() -> Result<()> {
    let tasks =
        dependency_fixture(&[("provider-bound", &["model", "context", "interactions"])])?.tasks;
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
    let mut bindings = dependency_fixture(&[("state-only", &["state"])])?;
    bindings.state = Some(Arc::new(SplitState {
        task_id: TaskId::new(),
        record: admission_fixture()?,
    }));
    assert!(bindings.build(None).is_ok());
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_policy_approval_keeps_exact_binding_and_paid_utf8_prompt() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = native_work(&cancellation);
    let baseline = work.live_allocation_bytes();
    let (policy, definition, invocation) = native_policy_fixture_inputs();
    let mut context = test_context(Bindings::local().build(None)?)?;
    let task = TaskId::new();
    context.durable_task = Some(task);
    let (actual, custody) = context
        .policy_approval(
            &policy,
            Some(&policy.identity),
            &definition,
            &invocation,
            Some(&work),
            context.scope(),
        )
        .await?
        .expect("approval");
    let expected = tool_approval_request(
        task,
        &policy.identity,
        &definition,
        &invocation,
        (
            match &policy.decision {
                ToolPolicyDecision::RequireApproval { prompt } => prompt.clone(),
                _ => unreachable!(),
            },
            None,
        ),
        None,
    )?
    .0;
    assert_eq!(actual, expected);
    let custody = custody.expect("paid approval");
    assert!(custody.work().shares_ledger(&work));
    assert!(work.live_allocation_bytes() >= baseline + custody.bytes());
    drop(actual);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_policy_identity_denies_cancelled_and_exhausted_copy_before_strings() -> Result<()> {
    let (policy, _, _) = native_policy_fixture_inputs();
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(256, 16_384);
    budget.bytes_copied = 0;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(matches!(
        policy.identity_with_native_work(&work),
        Err(Error::NativeFilesystem { source, custody: None })
            if matches!(*source, acyclic_fs::FsError::NativeKernel(
                acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { counter: "bytes_copied", .. }
                )
            ))
    ));
    let work = native_work(&cancellation);
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
    let original = native_work(&cancellation);
    let foreign = native_work(&cancellation);
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
fn opaque_interaction_router_rejects_native_before_ordinary_route_or_box() -> Result<()> {
    use crate::store::history::HistoryIo;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = native_work(&cancellation);
    assert!(matches!(
        OpaqueRouter.identity_with_native_work(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(matches!(
        OpaqueRouter.supports_native_interactions(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&work)?);
    let prompt = io.clone_text("Approve \u{03bb}?")?;
    let request = Interaction::Approval {
        prompt,
        operation_id: OperationId::new(),
        action_digest: [1; 32],
    };
    let custody = io.into_custody()?;
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_policy_denial_is_a_verdict_and_evaluation_failures_stay_errors() -> Result<()> {
    use crate::runtime::PolicyVerdict;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = native_work(&cancellation);
    let (mut policy, definition, invocation) = native_policy_fixture_inputs();
    policy.decision = ToolPolicyDecision::Deny {
        reason: "echo is forbidden".into(),
    };
    let context = test_context(Bindings::local().build(None)?)?;
    let verdict = context
        .policy_decision(
            &policy,
            Some(&policy.identity),
            &definition,
            &invocation,
            Some(&work),
            context.scope(),
        )
        .await?;
    assert!(matches!(
        verdict,
        PolicyVerdict::Deny(Error::NativeDiagnostic {
            code: crate::wire::ErrorCode::Unauthorized,
            ref message,
            ..
        }) if message == "echo is forbidden"
    ));
    drop(verdict);
    // Tool task commands keep the strict form: the same denial is an error there.
    assert!(
        context
            .policy_approval(
                &policy,
                Some(&policy.identity),
                &definition,
                &invocation,
                Some(&work),
                context.scope(),
            )
            .await
            .is_err()
    );
    // Negative control: a changed policy identity is a failure, never a denial verdict.
    let changed = ComponentIdentity {
        digest: [10; 32],
        ..policy.identity.clone()
    };
    assert!(
        context
            .policy_decision(
                &policy,
                Some(&changed),
                &definition,
                &invocation,
                Some(&work),
                context.scope(),
            )
            .await
            .is_err()
    );
    Ok(())
}
