use super::*;

#[tokio::test]
async fn scoped_live_policy_preserves_ancestor_and_blocks_denied_tool() -> Result<()> {
    let (mut root_policy, _, _) = native_policy_fixture_inputs();
    root_policy.identity.name = "root.policy".into();
    root_policy.identity.digest = [1; 32];
    root_policy.decision = ToolPolicyDecision::Allow;
    let mut child_policy = root_policy.clone();
    child_policy.identity.name = "child.policy".into();
    child_policy.identity.digest = [2; 32];
    child_policy.decision = ToolPolicyDecision::Deny {
        reason: "child veto".into(),
    };
    let child_policy: Arc<dyn ToolPolicy> = Arc::new(child_policy);
    let scope = RuntimeScope::new(
        Capabilities::new(["tool:call:test.echo"]),
        Limits::default(),
    )?;
    let scope = scope.narrow(scope.grants().clone(), scope.limits())?;
    let harness = crate::runtime::Bindings {
        scope: scope.clone(),
        concurrency: 1,
        policy: Some(Arc::new(root_policy)),
        ..crate::runtime::Bindings::local()
    }
    .build(None)?;
    let context = test_context(harness)?;
    let narrowed = context.scoped_policy(child_policy)?;
    let (_, definition, invocation) = native_policy_fixture_inputs();
    assert!(matches!(
        narrowed.authorize_tool(&definition, &invocation).await,
        Err(Error::Unauthorized(_))
    ));
    let (identity, _native_policy) = narrowed
        .stock_tool_policy_binding()
        .expect("original root policy");
    assert_eq!(identity.name, "root.policy");
    let overrides = narrowed.stock_tool_policy_overrides().collect::<Vec<_>>();
    assert_eq!(overrides[0].0.name, "child.policy");
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = super::native::native_work(&cancellation);
        assert!(matches!(
            narrowed
                .authorize_tool_policies(
                    &definition,
                    &invocation,
                    Some(&work),
                    Some(&super::native::OpaqueRouter),
                    &narrowed.scope,
                )
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                ..
            })
        ));
        assert!(
            narrowed
                .policy_approval(
                    _native_policy.as_ref(),
                    Some(identity),
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
                Some(overrides[0].0),
                &definition,
                &invocation,
                Some(&work),
                &narrowed.scope,
            )
            .await
            .expect_err("original scoped veto");
        match error {
            Error::NativeDiagnostic {
                code: crate::wire::ErrorCode::Unauthorized,
                message,
                custody,
            } => {
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
        }
        impl ToolPolicy for ChangingPolicy {
            fn identity(&self) -> ComponentIdentity {
                panic!("native policy must not use ordinary identity")
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
            fn evaluate<'a>(
                &'a self,
                _: &'a ToolInvocation,
                scope: &'a RuntimeScope,
                io: Option<&'a crate::executor::PreparationIo>,
            ) -> Result<
                crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>,
            > {
                let work = task_original_work(io)?.expect("native policy ledger");
                crate::NativeFutureBundle::new(work, || async move {
                    assert!(!scope.grants().contains("tool:call:test.echo"));
                    self.changed.store(true, Ordering::SeqCst);
                    Ok((
                        ToolPolicyDecision::Allow,
                        Some(work.claim(0, 0, acyclic_fs::WorkCounters::default())?),
                    ))
                })
                .map(crate::store::HistoryContentFuture::Native)
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
        assert!(matches!(
            narrowed.require_native_scope_restriction(&foreign, &work),
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
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
        };
        assert!(matches!(
            narrowed
                .policy_approval(
                    &changing,
                    Some(identity),
                    &definition,
                    &invocation,
                    Some(&work),
                    &effective
                )
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                ..
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
            InteractionOutcome::Declined {},
            crate::InteractionRejection::Declined,
        ),
        (
            InteractionOutcome::Cancelled {},
            crate::InteractionRejection::Cancelled,
        ),
        (
            InteractionOutcome::Expired {},
            crate::InteractionRejection::Expired,
        ),
        (
            InteractionOutcome::Denied {},
            crate::InteractionRejection::Denied,
        ),
    ] {
        assert!(matches!(check_tool_approval(outcome),
                Err(Error::InteractionRejected(actual)) if actual == rejection));
    }
    assert!(check_tool_approval(InteractionOutcome::Approved {}).is_ok());
}

#[tokio::test]
async fn tool_approval_binds_runtime_action_and_accepts_provider_call_names() -> Result<()> {
    let (mut policy, definition, mut invocation) = native_policy_fixture_inputs();
    policy.decision = ToolPolicyDecision::RequireApproval {
        prompt: "Approve echo".into(),
    };
    invocation.call_id = "provider-call-name".into();
    invocation.arguments = serde_json::json!({"text":"hello"});
    let task = TaskId::from_bytes([42; 16]);
    let mut context = test_context(Bindings::local().build(None)?)?;
    context.durable_task = Some(task);
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
    for (changed_task, changed_policy, changed_invocation) in [
        (
            TaskId::from_bytes([43; 16]),
            policy.identity.clone(),
            invocation.clone(),
        ),
        (
            task,
            ComponentIdentity {
                version: "2".into(),
                digest: [8; 32],
                ..policy.identity.clone()
            },
            invocation.clone(),
        ),
        (
            task,
            policy.identity.clone(),
            ToolInvocation {
                arguments: serde_json::json!({"text":"different"}),
                ..invocation.clone()
            },
        ),
    ] {
        assert_ne!(
            approval.0,
            tool_approval_request(
                changed_task,
                &changed_policy,
                &definition,
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
