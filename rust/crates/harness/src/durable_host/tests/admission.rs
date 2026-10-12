use super::*;

#[tokio::test]
async fn admission_retry_retains_original_owner_grant_after_renewal() -> Result<()> {
    for session in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "batch-owner".into(),
        };
        let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
        let owner_scope = issuer
            .issue(
                "owner",
                &Capabilities::new([
                    "operation:declare",
                    "operation:observe",
                    "operation:cancel",
                    "task:spawn:test.batch@1",
                ]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof");
        let runtime_scope =
            RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        let (task, machine, tasks, machines) = test_registries::<i64>(
            implementation,
            serde_json::json!({"type": "integer"}),
            serde_json::json!({"type": "integer"}),
        )?;
        let coordinator =
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            &(owner),
            owner_scope.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
            None,
        )?;
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 8,
            total_tasks: 16,
            depth: 4,
            model_steps: 100,
        };
        let host = if session {
            host.with_session_limits(limits, None)?
        } else {
            host
        };
        let root_admission = |operation_id, input| TaskAdmissionRecord {
            operation_id,
            resource_proofs: Vec::new(),
            task: task.clone(),
            machine: machine.clone(),
            input,
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: runtime_scope.extensions().cloned(),
            execution: None,
        };

        let operation_id = OperationId::from_bytes([88; 16]);
        let admission = root_admission(operation_id, serde_json::json!(0));
        assert!(matches!(
            host.admit(admission.clone()).await?,
            Admission::Accepted(_)
        ));
        let original = host
            .retained_declaration_with_work(operation_id, &mut None)
            .await?
            .ok_or_else(|| Error::NotFound("original admission".into()))?;
        let lease = host
            .coordinator
            .lock()
            .await
            .pull(&crate::distributed::Worker {
                id: "renewal-worker".into(),
                available: crate::scheduler::ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            })
            .await?
            .ok_or_else(|| Error::NotFound("parent lease".into()))?;
        host.start_task(&lease, None).await?;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let mut child = root_admission(OperationId::from_bytes([89; 16]), serde_json::json!(0));
        child.parent = Some(TaskId::from_bytes(operation_id.into_bytes()));
        assert!(matches!(
            host.admit_owned(child.clone(), fence.clone()).await?,
            Admission::Accepted(_)
        ));
        let original_child = host
            .retained_declaration_with_work(child.operation_id, &mut None)
            .await?
            .ok_or_else(|| Error::NotFound("original child admission".into()))?;
        let renewed = issuer
            .issue(
                "renewed-owner",
                owner_scope.capabilities(),
                None,
                None,
                &mut None,
            )
            .expect("scope proof");
        let reopened = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            &(owner),
            renewed,
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
            None,
        )?;
        let reopened = if session {
            reopened.with_session_limits(limits, None)?
        } else {
            reopened
        };
        let revision = reopened.coordinator.lock().await.revision();
        assert!(matches!(
            reopened.admit(admission.clone()).await?,
            Admission::Accepted(_)
        ));
        assert_eq!(reopened.coordinator.lock().await.revision(), revision);
        assert_eq!(
            reopened
                .retained_declaration_with_work(operation_id, &mut None)
                .await?,
            Some(original.clone())
        );
        assert!(matches!(
            reopened.admit_owned(child.clone(), fence.clone()).await?,
            Admission::Accepted(_)
        ));
        assert_eq!(reopened.coordinator.lock().await.revision(), revision);
        assert_eq!(
            reopened
                .retained_declaration_with_work(child.operation_id, &mut None)
                .await?,
            Some(original_child.clone())
        );
        let mut stale = fence.clone();
        stale.reservation_id.push_str("-stale");
        assert!(matches!(
            reopened.admit_owned(child.clone(), stale).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(reopened.coordinator.lock().await.revision(), revision);
        let mut changed = admission.clone();
        changed.input = serde_json::json!(1);
        assert!(matches!(
            reopened.admit(changed).await,
            Err(Error::Conflict(_))
        ));
        let impostor = AuthorityIssuer::new("batch-test", [8; 32], owner.clone());
        let impostor_host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            &(owner),
            impostor
                .issue(
                    "renewed-owner",
                    owner_scope.capabilities(),
                    None,
                    None,
                    &mut None,
                )
                .expect("scope proof"),
            impostor.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
            None,
        )?;
        let impostor_host = if session {
            impostor_host.with_session_limits(limits, None)?
        } else {
            impostor_host
        };
        assert!(matches!(
            impostor_host.admit(admission).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(
            reopened
                .retained_declaration_with_work(operation_id, &mut None)
                .await?,
            Some(original)
        );
        reopened
            .coordinator
            .lock()
            .await
            .cancel_operation(
                &owner,
                &reopened.owner_scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                operation_id,
                IdempotencyKey::new("renewal-parent-cancel")?,
                false,
                None,
                &mut None,
            )
            .await?;
        let cancelled_revision = reopened.coordinator.lock().await.revision();
        assert!(matches!(
            reopened.admit_owned(child.clone(), fence).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            reopened.coordinator.lock().await.revision(),
            cancelled_revision
        );
        assert_eq!(
            reopened
                .retained_declaration_with_work(child.operation_id, &mut None)
                .await?,
            Some(original_child)
        );
    }
    Ok(())
}
