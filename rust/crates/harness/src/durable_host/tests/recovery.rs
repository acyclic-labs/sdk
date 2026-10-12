use super::*;

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn bounded_routing_ack_loss_redelivery_cancel_and_replacement() -> Result<()> {
    use crate::distributed::{WorkPull, Worker};
    use std::sync::atomic::Ordering;
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    if a == b || a == c || a == d || b == c || b == d || c == d {
                        continue;
                    }
                    let provider =
                        Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
                    let stream = provider.clone();
                    let fixture = effect_task_fixture(
                        stream.clone(),
                        "test.routing",
                        vec![
                            "mail:send".into(),
                            "mail:read".into(),
                            "model:generate".into(),
                        ],
                    )
                    .await?;
                    let host = fixture.host;
                    let (task, fence) = fixture.binding.task_binding();
                    let fence = fence.clone();
                    let message = OperationId::from_bytes([54; 16]);
                    let payload = fixture.payloads.stage(message, "body", b"7", None)?.await?;
                    let mut fenced = false;
                    let mut published = false;
                    for action in [a, b, c, d] {
                        match action {
                            0 | 1 => {
                                if !fenced {
                                    host.claim_model_dispatch(
                                        task,
                                        message,
                                        0,
                                        [7; 32],
                                        fence.clone(),
                                    )
                                    .await?;
                                    assert!(
                                        host.claim_model_dispatch(
                                            task,
                                            message,
                                            0,
                                            [8; 32],
                                            fence.clone()
                                        )
                                        .await
                                        .is_err()
                                    );
                                }
                                if action == 0 && !fenced {
                                    provider.commit_lose_ack.store(true, Ordering::SeqCst);
                                }
                                let sent = host
                                    .send_mail(task, &fence, task, message, payload.clone(), None)
                                    .await;
                                provider.commit_lose_ack.store(false, Ordering::SeqCst);
                                if fenced {
                                    assert!(sent.is_err(), "stale or cancelled sender succeeded");
                                } else {
                                    // A lost reply may report uncertainty after the original atomic publication.
                                    let inbox = host.inbox(task, 0, 2).await?;
                                    assert_eq!(inbox.len(), 1);
                                    assert_eq!(inbox[0].message_id, message.to_string());
                                    published = true;
                                    assert_eq!(
                                        host.send_mail(
                                            task,
                                            &fence,
                                            task,
                                            message,
                                            payload.clone(),
                                            None
                                        )
                                        .await?
                                        .operation_id,
                                        message
                                    );
                                    assert!(
                                        host.send_mail(
                                            task,
                                            &fence,
                                            task,
                                            message,
                                            fixture
                                                .payloads
                                                .stage(message, "changed", b"8")
                                                .await?,
                                            None
                                        )
                                        .await
                                        .is_err()
                                    );
                                }
                            }
                            2 => {
                                if !host.cancellation_requested(task, None).await? {
                                    let WorkPull::Claimed(lease) =
                                        host.recover_work(task, &mut None).await?
                                    else {
                                        return Err(Error::NotFound(
                                            "routing original lease".into(),
                                        ));
                                    };
                                    host.coordinator
                                        .lock()
                                        .await
                                        .release_lease(
                                            &lease,
                                            IdempotencyKey::new("routing-replace")?,
                                        )
                                        .await?;
                                    let WorkPull::Claimed(replacement) = host
                                        .pull_operation(
                                            &Worker {
                                                id: "replacement".into(),
                                                available: Default::default(),
                                                labels: BTreeMap::new(),
                                            },
                                            OperationId::from_bytes(task.into_bytes()),
                                            &mut None,
                                        )
                                        .await?
                                    else {
                                        return Err(Error::NotFound(
                                            "routing replacement lease".into(),
                                        ));
                                    };
                                    host.start_task(&replacement, None).await?;
                                    assert_ne!(
                                        crate::scheduler::LeaseFence::from(
                                            &replacement.reservation
                                        ),
                                        fence
                                    );
                                }
                                fenced = true;
                            }
                            _ => {
                                host.cancel(task, None).await?;
                                fenced = true;
                            }
                        }
                        assert_eq!(
                            host.mailbox(task)?
                                .bounds()
                                .await
                                .map(|bounds| bounds.tail)
                                .unwrap_or(0),
                            u64::from(published)
                        );
                    }
                    let inbox = host.inbox(task, 0, 2).await?;
                    assert_eq!(inbox.len(), usize::from(published));
                    assert!(
                        host.claim_model_dispatch(task, message, 0, [7; 32], fence)
                            .await
                            .is_err()
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn recovery_after_pull_response_loss_keeps_original_reservation_without_writes() -> Result<()>
{
    use crate::distributed::{WorkLease, WorkPull, Worker};
    use std::sync::atomic::Ordering;

    for mode in 0..3 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let stream = provider.clone();
        let fixture = effect_task_fixture(stream.clone(), "test.recovery", Vec::new()).await?;
        let host = fixture.host;
        let (task, _) = fixture.binding.task_binding();
        let WorkPull::Claimed(previous) = host.recover_work(task, &mut None).await? else {
            return Err(Error::NotFound("original running reservation".into()));
        };
        host.coordinator
            .lock()
            .await
            .release_lease(&previous, IdempotencyKey::new("recover-fixture-release")?)
            .await?;
        if mode == 1 {
            provider.lose_ack.store(true, Ordering::SeqCst);
            provider.hide_receipt.store(true, Ordering::SeqCst);
        } else if mode == 2 {
            provider.location_fault.store(4, Ordering::SeqCst);
        }
        let response = host
            .pull_operation(
                &Worker {
                    id: "reopened-worker".into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                OperationId::from_bytes(task.into_bytes()),
                &mut None,
            )
            .await?;
        let expected = match response {
            WorkPull::Claimed(lease) if mode == 0 => lease,
            WorkPull::Unresolved { lease, .. } if mode != 0 => lease,
            _ => return Err(Error::Conflict("pull fault did not execute".into())),
        };
        let reopened = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, fixture.payloads.clone(), None, &mut None)
                .await?,
            stream.clone(),
            host.payloads.clone(),
            host.reader.clone(),
            &(host.verifier.audience()),
            host.owner_scope.clone(),
            host.verifier.clone(),
            host.root_scope.clone(),
            host.tasks.clone(),
            host.machines.clone(),
            host.clock.clone(),
            None,
        )?);
        drop(fixture.binding);
        drop(host);
        let journal = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&stream),
            "harness/v3/coordinator/events",
        )?;
        let tail = journal.tail().await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let recovered = reopened.recover_work(task, &mut None).await?;
        assert_eq!(journal.tail().await?, tail);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert!(
            reopened
                .recover_work(TaskId::from_bytes([201; 16]), &mut None)
                .await
                .is_err()
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        if mode == 2 {
            assert!(matches!(recovered, WorkPull::Idle));
            continue;
        }
        let WorkPull::Claimed(recovered) = recovered else {
            return Err(Error::NotFound("committed original reservation".into()));
        };
        assert_eq!(recovered, expected);
        assert_ne!(recovered.reservation, previous.reservation);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        reopened.start_task(&recovered, None).await?;
        assert!(reopened.start_task(&previous, None).await.is_err());
        reopened.cancel(task, None).await?;
        let tail = journal.tail().await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let WorkPull::Unresolved { lease, .. } = reopened.recover_work(task, &mut None).await?
        else {
            return Err(Error::Conflict(
                "cancelled reservation was made ready".into(),
            ));
        };
        let WorkLease {
            reservation,
            operation,
            ..
        } = lease;
        assert_eq!(reservation, recovered.reservation);
        assert_eq!(operation, recovered.operation);
        assert_eq!(journal.tail().await?, tail);
        assert!(reopened.start_task(&recovered, None).await.is_err());
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn task_effect_dispatch_race_and_cancelled_settlement_use_one_owner() -> Result<()> {
    use crate::{
        core::{EffectGuarantee, EffectStatus, SchemaRegistry},
        effect_host::{ConversationEffectHost, TaskEffectPlan},
        effects::{EffectDispatch, EffectObservation, EffectProvider, EffectRegistry},
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct ReceiptProvider {
        calls: AtomicUsize,
        known: AtomicBool,
        request: Mutex<Option<EffectDispatch>>,
    }
    impl EffectProvider for ReceiptProvider {
        fn id(&self) -> &str {
            "test.receipt"
        }
        fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
            BTreeSet::from([EffectGuarantee::AtMostOnce])
        }
        fn linearizable_reconciliation(&self) -> bool {
            false
        }
        fn dispatch<'a>(
            &'a self,
            request: EffectDispatch,
        ) -> BoxFuture<'a, Result<EffectObservation>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                *self.request.lock().await = Some(request);
                Err(Error::Storage("lost provider reply".into()))
            })
        }
        fn reconcile<'a>(
            &'a self,
            attempt: crate::EffectAttemptId,
        ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
            Box::pin(async move {
                if !self.known.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                let request = self
                    .request
                    .lock()
                    .await
                    .clone()
                    .ok_or_else(|| Error::NotFound("receipt".into()))?;
                assert_eq!(request.attempt_id, attempt);
                Ok(Some(EffectObservation {
                    provider: request.provider,
                    effect_id: request.effect_id,
                    attempt_id: attempt,
                    request_digest: request.request_digest,
                    guarantee: request.guarantee,
                    status: EffectStatus::Failed {
                        message: "observed exit".into(),
                    },
                }))
            })
        }
    }

    let EffectTaskFixture {
        host,
        binding,
        payloads,
        runtime_scope,
    } = effect_task_fixture(
        Arc::new(MemoryStream::default()),
        "test.receipt",
        Vec::new(),
    )
    .await?;
    let stream = binding.stream();
    let operation = binding.operation_id();
    let (task, fence) = binding.task_binding();
    let request = payloads.stage(operation, "request", b"{}", None)?.await?;
    let schema = FileRef::new(
        request.volume().clone(),
        request.path(),
        request.version(),
        FileDescriptor::from_bytes(b"{}", "application/schema+json")?,
        "schema.json",
    )?;
    let plan = TaskEffectPlan {
        provider: "test.receipt".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: "test.exit.v1".into(),
        request,
        result_schema: schema,
    };
    let receipt = Arc::new(ReceiptProvider {
        calls: AtomicUsize::new(0),
        known: AtomicBool::new(false),
        request: Mutex::new(None),
    });
    let mut providers = EffectRegistry::default().with_result_resolver(payloads.clone());
    providers.register(receipt.clone())?;
    let conversation = Authority {
        kind: AggregateKind::Conversation,
        id: "effect-conversation".into(),
    };
    let conversation_issuer =
        AuthorityIssuer::new("effect-conversation", [54; 32], conversation.clone());
    let conversation_scope = conversation_issuer
        .issue("owner", &runtime_scope.grants(), None, None, &mut None)
        .expect("scope proof");
    let effects = ConversationEffectHost::new(
        Arc::new(MemoryStream::default()),
        conversation.clone(),
        conversation_issuer.clone(),
        conversation_scope,
        SchemaRegistry::new(),
        payloads.clone(),
        providers,
        None,
        None,
    )?;
    let command = OperationId::from_bytes([55; 16]);
    let (first, second) = tokio::join!(
        effects.run_task_effect(&binding, command, plan.clone()),
        effects.run_task_effect(&binding, command, plan.clone())
    );
    assert!(matches!(
        (&first, &second),
        (Err(_), Ok(EffectStatus::Indeterminate)) | (Ok(EffectStatus::Indeterminate), Err(_))
    ));
    assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
    let mut mutated = plan.clone();
    mutated.effect_kind = "changed.v1".into();
    assert!(matches!(
        effects
            .run_task_effect(&binding, command, mutated.clone())
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        effects
            .reconcile_task_effect(&binding, command, &mutated)
            .await,
        Err(Error::Conflict(_))
    ));
    host.cancel(task, None).await?;
    assert!(
        effects
            .run_task_effect(&binding, command, plan.clone())
            .await
            .is_err()
    );
    assert_eq!(
        effects
            .reconcile_task_effect(&binding, command, &plan)
            .await?,
        EffectStatus::Indeterminate
    );
    receipt.known.store(true, Ordering::SeqCst);
    assert_eq!(
        effects
            .reconcile_task_effect(&binding, command, &plan)
            .await?,
        EffectStatus::Failed {
            message: "observed exit".into()
        }
    );
    let aggregate = crate::store::StreamAggregate::open(
        &stream,
        conversation,
        conversation_issuer
            .verifier(&mut None)
            .expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(aggregate.reducer().revision(), 3);
    assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
    host.settle_task(task, &fence, Outcome::Cancelled, &mut None)
        .await?;
    assert!(
        effects
            .reconcile_task_effect(&binding, command, &plan)
            .await
            .is_err()
    );
    Ok(())
}
