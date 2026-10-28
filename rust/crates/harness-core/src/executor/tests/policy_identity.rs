use super::*;

struct ObservedPolicy {
    fixed: crate::test_support::FixedToolPolicy,
    ordinary: AtomicUsize,
    native: AtomicUsize,
    evaluations: AtomicUsize,
    change_during_evaluation: bool,
}

impl ToolPolicy for ObservedPolicy {
    fn identity(&self) -> ComponentIdentity {
        self.ordinary.fetch_add(1, Ordering::SeqCst);
        self.fixed.identity()
    }

    fn identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        self.native.fetch_add(1, Ordering::SeqCst);
        let (mut identity, custody) = self.fixed.identity_with_native_work(work)?;
        if self.change_during_evaluation && self.evaluations.load(Ordering::SeqCst) > 0 {
            identity.digest[0] ^= 1;
        }
        Ok((identity, custody))
    }

    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        scope: &'a RuntimeScope,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        self.evaluations.fetch_add(1, Ordering::SeqCst);
        self.fixed.evaluate(invocation, scope, io)
    }
}

#[tokio::test]
async fn native_policy_identity_checks_use_original_work_before_and_after_evaluation() -> Result<()>
{
    for (copies, changed_before, changed_during) in [
        (0, false, false),
        (4096, false, false),
        (4096, true, false),
        (4096, false, true),
    ] {
        let policy = Arc::new(ObservedPolicy {
            fixed: crate::test_support::FixedToolPolicy::new(ToolPolicyDecision::Allow)?,
            ordinary: AtomicUsize::new(0),
            native: AtomicUsize::new(0),
            evaluations: AtomicUsize::new(0),
            change_during_evaluation: changed_during,
        });
        let tools = echo_tools(
            Arc::new(FakeTool(AtomicUsize::new(0))),
            json!({"type":"object"}),
            json!({"type":"object"}),
            crate::tool::json_projection_schema(json!({"type":"object"})),
        )?;
        let mut stock = StockExecutor::new(
            Model::new("test".into(), "policy".into(), "1".into(), Value::Null)?,
            Arc::new(scripted_model()),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(RuntimeScope::default(), policy.clone())?;
        if changed_before {
            stock.policy_identity.as_mut().unwrap().digest[0] ^= 1;
        }
        policy.ordinary.store(0, Ordering::SeqCst);
        let mut budget = acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024);
        budget.bytes_copied = copies;
        let work = acyclic_fs::NativeMaterializationWork::new(
            budget,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let mut execution = StockExecution::ordinary(&stock);
        execution.io = Some(&io);
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "policy".into(),
            name: "example.echo".into(),
            arguments: json!({}),
        };
        let tool = stock.tools.get(&invocation.name, None, None)?.unwrap();
        let baseline = work.live_allocation_bytes();
        let result = execution
            .require_tool_policy(
                &Journal::default(),
                invocation.operation_id,
                &invocation,
                tool,
                false,
            )
            .await;
        assert_eq!(policy.ordinary.load(Ordering::SeqCst), 0);
        assert_eq!(work.live_allocation_bytes(), baseline);
        if copies == 0 {
            assert!(result.is_err());
            assert_eq!(policy.evaluations.load(Ordering::SeqCst), 0);
            assert_eq!(work.receipt().bytes_copied, 0);
            // The actual former comparison still clones and admits the same
            // identity on this exhausted ledger without recording any work.
            let old = acyclic_test_support::measure(|| {
                assert!(stock.policy_identity.as_ref() == Some(&policy.identity()));
            });
            assert!(old.count_total > 0);
            assert_eq!(work.receipt().bytes_copied, 0);
        } else {
            let checks = if changed_before { 1 } else { 2 };
            assert_eq!(policy.native.load(Ordering::SeqCst), checks);
            assert_eq!(
                policy.evaluations.load(Ordering::SeqCst),
                usize::from(!changed_before)
            );
            let bytes = policy.fixed.identity.name.len() + policy.fixed.identity.version.len();
            assert_eq!(work.receipt().bytes_copied, (checks * bytes) as u64);
            if changed_before {
                assert!(
                    matches!(result, Err(Error::Conflict(ref reason)) if reason == "stock policy changed after binding")
                );
            } else if changed_during {
                assert!(
                    matches!(result, Err(Error::Conflict(ref reason)) if reason == "stock policy changed during evaluation")
                );
            } else {
                assert!(matches!(result, Ok(None)));
            }
        }
    }
    Ok(())
}
