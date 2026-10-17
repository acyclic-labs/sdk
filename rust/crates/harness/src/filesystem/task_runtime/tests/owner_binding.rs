//! Binding preserves admitted authority and rejects substituted identities.
use super::*;
use proptest::prelude::*;

mod dispatch;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn ordinary_tool_owner_is_bound_to_the_exact_context_and_fence(
        suffix in "[a-z0-9]{1,16}",
        changed_placement in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                let fixture = Fixture::new().await?;
                let runtime = fixture.runtime(None).await?;
                let lease = fixture.admit(&runtime).await?;
                let lease = match runtime.resume_task(lease, &runtime.commands(), 1, None).await {
                    TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => lease,
                    TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
                    _ => return Err(Error::Invalid("owner fixture did not yield its outbox".into())),
                };
                let task = TaskId::from_bytes([104; 16]);
                let fence = LeaseFence::from(&lease.reservation);
                let context = runtime.harness().durable_context(
                    task, OperationId::from_bytes(task.into_bytes()), None,
                ).await?;
                assert!(context.owned_mailbox.is_none());
                let bound = runtime.bind_task_owner(context.as_ref().clone(), task, fence.clone()).await?;
                assert_eq!(bound.id(), context.id());
                assert_eq!(bound.scope().grants(), context.scope().grants());
                assert!(bound.original_task_binding_matches(task, &fence));
                bound.owned_mailbox.as_ref().unwrap().verify_owner(None)?.await?;

                let token = acyclic_fs::CancellationToken::new();
                let cancellable = runtime.bind_task_owner_cancellable(
                    context.as_ref().clone(), task, fence.clone(), Some(&token),
                ).await?;
                let cancellable = runtime.bind_task_owner(cancellable, task, fence.clone()).await?;
                let owner = cancellable.owned_mailbox.as_ref().unwrap();
                assert!(!owner.invocation_cancellation().unwrap().is_cancelled());
                token.cancel();
                assert!(owner.invocation_cancellation().unwrap().is_cancelled());
                // Stopping an invocation preserves its fence for cleanup and
                // result settlement; it does not invent permanent task loss.
                assert!(owner.verify_owner(None)?.await.is_err());
                let mailbox = (owner.as_ref() as &dyn std::any::Any)
                    .downcast_ref::<TaskJournalMailbox<acyclic_stream::MemoryStream>>()
                    .unwrap();
                mailbox.owner.verify(true, &mut None).await?;

                let other = TaskId::from_bytes([103; 16]);
                assert!(runtime.bind_task_owner(context.as_ref().clone(), other, fence.clone()).await.is_err());
                let reopened = fixture.runtime(None).await?;
                assert!(reopened.bind_task_owner(context.as_ref().clone(), task, fence.clone()).await.is_err());
                let mut changed = fence;
                if changed_placement { changed.placement.push_str(&suffix); }
                else { changed.reservation_id.push_str(&suffix); }
                assert!(runtime.bind_task_owner(context.as_ref().clone(), task, changed).await.is_err());
                Ok::<_, Error>(())
            }).unwrap();
    }
}
