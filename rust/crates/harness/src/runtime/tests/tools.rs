use super::*;

#[tokio::test]
async fn scoped_live_policy_preserves_ancestor_and_blocks_denied_tool() -> Result<()> {
    let root_policy: Arc<dyn ToolPolicy> = Arc::new(FixedPolicy {
        identity: ComponentIdentity {
            name: "root.policy".into(),
            version: "1".into(),
            digest: [1; 32],
        },
        decision: ToolPolicyDecision::Allow,
    });
    let child_policy: Arc<dyn ToolPolicy> = Arc::new(FixedPolicy {
        identity: ComponentIdentity {
            name: "child.policy".into(),
            version: "1".into(),
            digest: [2; 32],
        },
        decision: ToolPolicyDecision::Deny {
            reason: "child veto".into(),
        },
    });
    let scope = RuntimeScope::new(
        Capabilities::new(["tool:call:test.echo"]),
        Limits::default(),
    )?;
    let harness = crate::runtime::Bindings {
        tasks: TaskRegistry::default(),
        tools: ToolRegistry::default(),
        scope: scope.clone(),
        concurrency: 1,
        durable_host: None,
        interactions: None,
        content: None,
        artifacts: None,
        policy: Some(root_policy),
        ..crate::runtime::Bindings::local()
    }
    .build(None)?;
    let context = TaskContext {
        harness,
        task_id: OperationId::new(),
        durable_task: None,
        descendants: TaskGroup::new(1, None).unwrap(),
        scope,
        policy_overrides: Vec::new(),
        owned_interactions: None,
        owned_mailbox: None,
        output_content: None,
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        native_root_output: std::sync::OnceLock::new(),
        model_steps: Arc::new((AtomicUsize::new(0), None)),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_metadata_source: None,
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        working_view: std::sync::OnceLock::new(),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_custody: None,
    };
    let narrowed = context.scoped_policy(child_policy)?;
    let definition = ToolDefinition {
        name: "test.echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: serde_json::json!({}),
        output_schema: serde_json::json!({}),
        projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
    };
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: OperationId::new().to_string(),
        name: definition.name.clone(),
        arguments: serde_json::json!({}),
    };
    assert!(
        matches!(narrowed.authorize_tool(&definition, &invocation).await,
            Err(Error::Unauthorized(reason)) if reason == "child veto")
    );
    assert_eq!(narrowed.policy_overrides.len(), 1);
    let (identity, policy) = narrowed
        .stock_tool_policy_binding()
        .expect("original root policy");
    assert_eq!(identity.name, "root.policy");
    assert!(std::ptr::eq(
        policy,
        narrowed.harness.policy.as_ref().expect("bound policy")
    ));
    let overrides = narrowed.stock_tool_policy_overrides().collect::<Vec<_>>();
    assert_eq!(overrides[0].0.name, "child.policy");
    assert!(std::ptr::eq(
        overrides[0].1,
        &narrowed.policy_overrides[0].1
    ));
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::default(),
            &cancellation,
        )
        .expect("native policy source ledger");
        struct OpaqueCommandRouter;
        impl InteractionRouter for OpaqueCommandRouter {
            fn route<'a>(
                &'a self,
                _: OperationId,
                _: Interaction,
            ) -> BoxFuture<'a, Result<InteractionOutcome>> {
                panic!("ordinary command route must not run");
            }
        }
        let before = work.receipt();
        assert!(matches!(
            narrowed
                .authorize_tool_policies(
                    &definition,
                    &invocation,
                    Some(&work),
                    Some(&OpaqueCommandRouter),
                    &narrowed.scope,
                )
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                ..
            })
        ));
        assert_eq!(work.receipt(), before);
        assert!(
            narrowed
                .policy_approval(
                    policy.as_ref(),
                    Some((identity, "runtime policy identity changed after binding")),
                    &definition,
                    &invocation,
                    Some(&work),
                    &narrowed.scope
                )
                .await?
                .is_none()
        );
        let error = narrowed
            .policy_approval(
                overrides[0].1.as_ref(),
                Some((
                    overrides[0].0,
                    "scoped policy identity changed after binding",
                )),
                &definition,
                &invocation,
                Some(&work),
                &narrowed.scope,
            )
            .await
            .err()
            .expect("original scoped veto");
        match error {
            Error::NativeDiagnostic {
                code: crate::wire::ErrorCode::Unauthorized,
                message,
                custody,
            } => {
                assert_eq!(message, "child veto");
                assert!(custody.work().shares_ledger(&work));
                assert_eq!(custody.bytes(), message.capacity() as u64);
                drop(message);
                drop(custody);
            }
            other => panic!("native scoped policy cause: {other:?}"),
        }
        struct ChangingPolicy {
            original: ComponentIdentity,
            changed: std::sync::atomic::AtomicBool,
            scope_address: usize,
        }
        impl ToolPolicy for ChangingPolicy {
            fn identity(&self) -> ComponentIdentity {
                let mut identity = self.original.clone();
                if self.changed.load(Ordering::SeqCst) {
                    identity.digest[0] ^= 1;
                }
                identity
            }
            fn evaluate<'a>(
                &'a self,
                _: &'a ToolInvocation,
                scope: &'a RuntimeScope,
            ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
                Box::pin(async move {
                    assert_eq!(scope as *const RuntimeScope as usize, self.scope_address);
                    self.changed.store(true, Ordering::SeqCst);
                    Ok(ToolPolicyDecision::Allow)
                })
            }
            fn supports_native_policy(
                &self,
                _: &acyclic_fs::NativeMaterializationWork,
            ) -> Result<()> {
                Ok(())
            }
            fn identity_with_native_work(
                &self,
                work: &acyclic_fs::NativeMaterializationWork,
            ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
                let (mut identity, custody) = native_fixture_policy_identity(&self.original, work)?;
                if self.changed.load(Ordering::SeqCst) {
                    identity.digest[0] ^= 1;
                }
                Ok((identity, custody))
            }
            fn evaluate_with_native_work<'a>(
                &'a self,
                _: &'a ToolInvocation,
                scope: &'a RuntimeScope,
                work: &'a acyclic_fs::NativeMaterializationWork,
            ) -> Result<
                crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>,
            > {
                crate::NativeFutureBundle::new(work, || async move {
                    assert_eq!(scope as *const RuntimeScope as usize, self.scope_address);
                    self.changed.store(true, Ordering::SeqCst);
                    Ok((
                        ToolPolicyDecision::Allow,
                        work.claim(0, 0, acyclic_fs::WorkCounters::default())?,
                    ))
                })
            }
        }
        let effective = narrowed.scope.narrow(
            Capabilities::new(std::iter::empty::<&str>()),
            narrowed.scope.limits,
        )?;
        narrowed.require_native_scope_restriction(&effective, &work)?;
        assert!(!effective.grants().contains("tool:call:test.echo"));
        assert!(narrowed.scope.grants().contains("tool:call:test.echo"));
        let mut foreign = effective.clone();
        foreign.grants = Capabilities::new(["tool:call:foreign"]);
        let before = work.receipt();
        assert!(matches!(
            narrowed.require_native_scope_restriction(&foreign, &work),
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
        assert_eq!(work.receipt(), before);
        let mut invalid = effective.clone();
        invalid.run_limits.max_steps = Some(0);
        assert!(matches!(
            narrowed.require_native_scope_restriction(&invalid, &work),
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
        let changing = ChangingPolicy {
            original: identity.clone(),
            changed: std::sync::atomic::AtomicBool::new(false),
            scope_address: &effective as *const RuntimeScope as usize,
        };
        assert!(matches!(
            narrowed
                .policy_approval(
                    &changing,
                    Some((identity, "runtime policy identity changed after binding")),
                    &definition,
                    &invocation,
                    Some(&work),
                    &effective
                )
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "policy identity changed during evaluation"
            })
        ));
        assert_eq!(
            work.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
    }
    Ok(())
}

#[test]
fn tool_approval_keeps_terminal_outcomes_distinct() {
    for (outcome, rejection) in [
        (
            InteractionOutcome::Declined,
            crate::InteractionRejection::Declined,
        ),
        (
            InteractionOutcome::Cancelled,
            crate::InteractionRejection::Cancelled,
        ),
        (
            InteractionOutcome::Expired,
            crate::InteractionRejection::Expired,
        ),
        (
            InteractionOutcome::Denied,
            crate::InteractionRejection::Denied,
        ),
    ] {
        assert!(matches!(check_tool_approval(outcome),
                Err(Error::InteractionRejected(actual)) if actual == rejection));
    }
    assert!(check_tool_approval(InteractionOutcome::Approved).is_ok());
}

#[tokio::test]
async fn tool_approval_binds_runtime_action_and_accepts_provider_call_names() -> Result<()> {
    let policy = FixedPolicy {
        identity: ComponentIdentity {
            name: "approval.policy".into(),
            version: "1".into(),
            digest: [9; 32],
        },
        decision: ToolPolicyDecision::RequireApproval {
            prompt: "Approve echo".into(),
        },
    };
    let task = TaskId::from_bytes([42; 16]);
    let scope = RuntimeScope::default();
    let context = TaskContext {
        harness: crate::runtime::Bindings {
            tasks: TaskRegistry::default(),
            tools: ToolRegistry::default(),
            scope: scope.clone(),
            concurrency: 1,
            durable_host: None,
            ..crate::runtime::Bindings::local()
        }
        .build(None)?,
        task_id: OperationId::from_bytes([41; 16]),
        durable_task: Some(task),
        descendants: TaskGroup::new(1, None).unwrap(),
        scope,
        policy_overrides: Vec::new(),
        owned_interactions: None,
        owned_mailbox: None,
        output_content: None,
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        native_root_output: std::sync::OnceLock::new(),
        model_steps: Arc::new((AtomicUsize::new(0), None)),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_metadata_source: None,
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        working_view: std::sync::OnceLock::new(),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_custody: None,
    };
    let definition = ToolDefinition {
        name: "test.echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: serde_json::json!({"type":"object"}),
        output_schema: serde_json::json!({}),
        projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
    };
    let invocation = ToolInvocation::for_model_call(
        OperationId::from_bytes([40; 16]),
        0,
        "provider-call-name".into(),
        definition.name.clone(),
        serde_json::json!({"text":"hello"}),
        None,
    )?;
    let approval = context
        .policy_approval(
            &policy,
            None,
            &definition,
            &invocation,
            None,
            &context.scope,
        )
        .await?
        .map(|paid| paid.0)
        .ok_or_else(|| Error::NotFound("policy approval".into()))?;
    assert!(
        matches!(&approval.1, Interaction::Approval { operation_id, .. } if *operation_id == invocation.operation_id)
    );
    assert_eq!(
        approval,
        tool_approval_request(
            task,
            &policy.identity,
            &definition,
            &invocation,
            ("Approve echo".into(), None),
            None
        )
        .map(|(value, _)| value)?
    );
    assert_eq!(
        context
            .policy_approval(
                &policy,
                None,
                &definition,
                &invocation,
                None,
                &context.scope
            )
            .await?
            .map(|paid| paid.0),
        Some(approval.clone())
    );
    for (changed_task, changed_policy, changed_definition, changed_invocation) in [
        (
            TaskId::from_bytes([43; 16]),
            policy.identity.clone(),
            definition.clone(),
            invocation.clone(),
        ),
        (
            task,
            ComponentIdentity {
                version: "2".into(),
                digest: [8; 32],
                ..policy.identity.clone()
            },
            definition.clone(),
            invocation.clone(),
        ),
        (
            task,
            policy.identity.clone(),
            ToolDefinition {
                revision: "2".into(),
                ..definition.clone()
            },
            invocation.clone(),
        ),
        (
            task,
            policy.identity.clone(),
            definition.clone(),
            ToolInvocation {
                arguments: serde_json::json!({"text":"different"}),
                ..invocation.clone()
            },
        ),
        (
            task,
            policy.identity.clone(),
            definition.clone(),
            ToolInvocation::for_model_call(
                OperationId::from_bytes([40; 16]),
                1,
                invocation.call_id.clone(),
                invocation.name.clone(),
                invocation.arguments.clone(),
                None,
            )?,
        ),
    ] {
        assert_ne!(
            approval.0,
            tool_approval_request(
                changed_task,
                &changed_policy,
                &changed_definition,
                &changed_invocation,
                ("Approve echo".into(), None),
                None
            )
            .map(|(value, _)| value)?
            .0
        );
    }
    Ok(())
}
