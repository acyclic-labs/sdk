use super::*;

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
        let original = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original grants ledger");
        let construction = NativeMaterializationWork::new(budget, &cancellation)
            .expect("independent construction ledger");
        let paid = Capabilities::new_with_native_work(["read"], &original)
            .expect("paid input");
        let (scope, custody) = RuntimeScope::new_with_native_work(
            paid, Limits::default(), &construction,
        ).expect("independent guards are legitimate");
        assert!(scope._native_grants_custody.as_ref().unwrap().work().shares_ledger(&original));
        assert!(custody.work().shares_ledger(&construction));
        let invalid = Limits { file_bytes: 0, ..Limits::default() };
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| (&invalid).validate(&mut Some(&mut io))),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        let scope = crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| scope.with_run_limits(TaskRunLimits { concurrency: Some(1), ..TaskRunLimits::default() }, &mut Some(&mut io))).expect("bounded concurrency");
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| scope.with_run_limits(TaskRunLimits::default(), &mut Some(&mut io))), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        drop(custody);
        cancellation.cancel();
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| (&Limits::default()).validate(&mut Some(&mut io))), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_scope_clone_and_narrow_preserve_readonly_grants_and_original_bounds() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 8,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 8192,
            bytes_copied: 64,
            items_examined: 16,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let (root, root_custody) = RuntimeScope::new_with_native_work(
            Capabilities::new_with_native_work(["read"], &work).expect("paid read-only grants"),
            Limits::default(), &work,
        ).expect("read-only root");
        let root = crate::store::native_work_io(&work).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| root.with_run_limits(TaskRunLimits { max_steps: Some(2), ..TaskRunLimits::default() }, &mut Some(&mut io))).expect("original execution bound");
        let (copy, copy_custody) = root.clone_with_native_work(&work).expect("native clone");
        assert_eq!(copy.grants(), root.grants());
        assert_eq!(copy.run_limits(), root.run_limits());
        assert!(!copy.is_sealed());
        assert!(copy.resource_proofs().is_empty());
        assert!(copy.created_access().is_none());
        assert!(!copy.grants().contains("write"));
        assert!(copy._native_grants_custody.as_ref().unwrap().work().shares_ledger(&work));
        let narrower = Limits { file_bytes: 1, ..Limits::default() };
        let (child, child_custody) = root.narrow_with_native_work(
            Capabilities::new_with_native_work(["read"], &work).expect("paid child grants"),
            narrower, &work,
        ).expect("native attenuation");
        assert!(child.is_sealed());
        assert_eq!(child.limits(), narrower);
        assert_eq!(child.run_limits(), root.run_limits());
        assert!(child.resource_proofs().is_empty());
        assert!(!child.grants().contains("write"));
        assert!(matches!(root.narrow_with_native_work(
            Capabilities::new_with_native_work(["write"], &work).expect("requested widening"),
            Limits::default(), &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
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
        let definition =
            TaskDefinition::live("provider-bound", "1", |_context, _input: ()| async {
                Ok::<(), Error>(())
            })?
            .requires("model")?
            .requires("context")?
            .requires("interactions")?;
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let tools = ToolRegistry::default();
        let all_grants = ["model:generate", "context:build", "interaction:route"];
        let complete_scope = RuntimeScope::new(
            Capabilities::new(all_grants.iter().copied()),
            Limits::default(),
        )?;
        assert!(
            validate_task_dependencies(
                &tasks,
                &tools,
                &ResumableToolRegistry::default(),
                &complete_scope,
                false,
                false,
                false,
                true,
                None,
                None,
                false,
                None,
            )
            .is_ok()
        );

        for missing_grant in all_grants {
            let scope = RuntimeScope::new(
                Capabilities::new(
                    all_grants
                        .iter()
                        .copied()
                        .filter(|grant| *grant != missing_grant),
                ),
                Limits::default(),
            )?;
            assert!(
                validate_task_dependencies(
                    &tasks,
                    &tools,
                    &ResumableToolRegistry::default(),
                    &scope,
                    false,
                    false,
                    false,
                    true,
                    None,
                    None,
                    false,
                    None,
                )
                .is_err()
            );
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
        bindings.state = Some(Arc::new(StateOnlyProvider));
        assert!(bindings.build().is_ok());
        Ok(())
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_policy_approval_keeps_exact_binding_and_paid_utf8_prompt() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original policy ledger");
        let (policy, definition, invocation) = native_policy_fixture_inputs();
        let scope = RuntimeScope::new(Capabilities::new(["tool:call:test.echo"]), Limits::default())?;
        let before = work.receipt().allocation_operations;
        let (mut identity, custody) = policy.identity_with_native_work(&work)?;
        assert_eq!(custody.bytes(), std::mem::size_of::<ComponentIdentity>() as u64
            + identity.name.capacity() as u64 + identity.version.capacity() as u64);
        assert_eq!(work.receipt().allocation_operations, before + 2);
        validate_policy_identity_with_native_work(&identity, &custody, &work)?;
        identity.name.clear();
        let observed = work.receipt();
        assert!(matches!(validate_policy_identity_with_native_work(&identity, &custody, &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "policy name is invalid" })));
        assert_eq!(work.receipt(), observed);
        drop((identity, custody));
        let (decision, custody) = policy.evaluate_with_native_work(&invocation, &scope, &work)?.await?;
        let ToolPolicyDecision::RequireApproval { prompt } = decision else { panic!("fixture approval"); };
        assert_eq!(custody.bytes(), prompt.capacity() as u64);
        let pointer = prompt.as_ptr();
        let task = TaskId::new();
        let passive = tool_approval_binding_with_native_work(task, &policy.identity, &definition, &invocation, &work)?;
        assert_eq!(passive, tool_approval_binding(task, &policy.identity, &definition, &invocation)?);
        let expected = tool_approval_request(task, &policy.identity, &definition, &invocation, prompt.clone())?;
        let (actual, custody) = tool_approval_request_with_native_work(
            task, &policy.identity, &definition, &invocation, prompt, custody, &work,
        )?;
        assert_eq!(actual, expected);
        let Interaction::Approval { prompt, .. } = &actual.1 else { panic!("approval request"); };
        assert_eq!(prompt.as_ptr(), pointer);
        assert!(custody.work().shares_ledger(&work));
        assert!(work.receipt().bytes_hashed > 32);
        let paid = (actual, custody);
        let live = work.live_allocation_bytes();
        assert!(live >= acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + paid.1.bytes());
        drop(paid);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_policy_identity_denies_cancelled_and_exhausted_copy_before_strings() -> Result<()> {
        let (policy, _, _) = native_policy_fixture_inputs();
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::WorkBudget::default();
        budget.bytes_copied = 0;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let before = work.receipt();
        assert!(matches!(policy.identity_with_native_work(&work), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, 0);
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("cancelled-policy original ledger");
        cancellation.cancel();
        assert!(matches!(policy.identity_with_native_work(&work), Err(Error::NativeWork { .. })));
        Ok(())
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_policy_approval_rejects_foreign_prompt_before_encoding() -> Result<()> {
        use crate::store::history::HistoryIo;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original ledger");
        let foreign = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("foreign ledger");
        let (policy, definition, invocation) = native_policy_fixture_inputs();
        let mut io = crate::store::history::native_work_io(&foreign)?;
        let prompt = io.clone_text("Foreign prompt")?;
        let custody = io.into_custody();
        let before = original.receipt();
        assert!(matches!(tool_approval_request_with_native_work(
            TaskId::new(), &policy.identity, &definition, &invocation, prompt, custody, &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })));
        assert_eq!(original.receipt(), before);
        assert_eq!(foreign.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn opaque_policy_native_defaults_never_call_ordinary_body() -> Result<()> {
        struct OpaquePolicy;
        impl ToolPolicy for OpaquePolicy {
            fn identity(&self) -> ComponentIdentity { panic!("ordinary identity must not run"); }
            fn evaluate<'a>(
                &'a self, _: &'a ToolInvocation, _: &'a RuntimeScope,
            ) -> BoxFuture<'a, Result<ToolPolicyDecision>> { panic!("ordinary evaluator must not run"); }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original ledger");
        let (_, _, invocation) = native_policy_fixture_inputs();
        let scope = RuntimeScope::new(Capabilities::new(["tool:call:test.echo"]), Limits::default())?;
        let before = work.receipt();
        assert!(matches!(OpaquePolicy.supports_native_policy(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert!(matches!(OpaquePolicy.identity_with_native_work(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert!(matches!(OpaquePolicy.evaluate_with_native_work(&invocation, &scope, &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        Ok(())
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn opaque_interaction_router_rejects_native_before_ordinary_route_or_box() -> Result<()> {
        use crate::store::history::HistoryIo;
        struct OpaqueRouter;
        impl InteractionRouter for OpaqueRouter {
            fn route<'a>(
                &'a self, _: OperationId, _: Interaction,
            ) -> BoxFuture<'a, Result<InteractionOutcome>> { panic!("ordinary route must not run"); }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original interaction ledger");
        let before = work.receipt();
        assert!(matches!(OpaqueRouter.supports_native_interactions(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        let mut io = crate::store::history::native_work_io(&work)?;
        let prompt = io.clone_text("Approve Î»?")?;
        let request = Interaction::Approval { prompt, operation_id: OperationId::new(), action_digest: [1; 32] };
        let custody = io.into_custody();
        let before = work.receipt();
        assert!(matches!(OpaqueRouter.route_with_native_work(
            OperationId::new(), request, custody, &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }
