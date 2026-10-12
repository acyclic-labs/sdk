use super::*;
use crate::distributed::{WorkPull, Worker};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn b11_pull_response_loss_keeps_original_reservation() -> Result<()> {
    for mode in 0..3 {
        let stream = Arc::new(crate::test_stream::LostSessionAck::default());
        let f = Fixture::new(
            stream.clone(),
            acyclic_fs::Fs::memory(),
            json!({"type":"integer"}),
            None,
            &[],
        )
        .await?;
        f.host.admit(f.admission(1, json!(0))).await?;
        if mode == 1 {
            stream.lose_ack.store(true, Ordering::SeqCst);
            stream.hide_receipt.store(true, Ordering::SeqCst);
        }
        if mode == 2 {
            stream.location_fault.store(4, Ordering::SeqCst);
        }
        let response = f
            .host
            .pull_operation(
                &Worker {
                    id: "original".into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                OperationId::from_bytes([1; 16]),
                &mut None,
            )
            .await?;
        let expected = match response {
            WorkPull::Claimed(lease) if mode == 0 => lease,
            WorkPull::Unresolved { lease, .. } if mode != 0 => lease,
            _ => return Err(Error::Conflict("pull fault did not run".into())),
        };
        let reopened = f.reopen(&f.issuer, "owner").await?;
        stream.forbid_writes.store(true, Ordering::SeqCst);
        let recovered = reopened
            .recover_work(TaskId::from_bytes([1; 16]), &mut None)
            .await?;
        assert_eq!(stream.observation_writes.load(Ordering::SeqCst), 0);
        if mode == 2 {
            assert!(matches!(recovered, WorkPull::Idle));
            continue;
        }
        let WorkPull::Claimed(lease) = recovered else {
            return Err(Error::NotFound("original reservation".into()));
        };
        assert_eq!(lease, expected);
        stream.forbid_writes.store(false, Ordering::SeqCst);
        reopened.start_task(&lease, None).await?;
        reopened.cancel(TaskId::from_bytes([1; 16])).await?;
        stream.forbid_writes.store(true, Ordering::SeqCst);
        let WorkPull::Unresolved {
            lease: cancelled, ..
        } = reopened
            .recover_work(TaskId::from_bytes([1; 16]), &mut None)
            .await?
        else {
            return Err(Error::Conflict("cancelled lease became runnable".into()));
        };
        assert_eq!(cancelled.reservation, lease.reservation);
        assert!(reopened.start_task(&lease, None).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn b12_wake_pages_filter_owners_and_current_cancellation() -> Result<()> {
    let f = fixture::fixture().await?;
    let foreign = AuthorityIssuer::new(
        "foreign",
        [8; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        },
    );
    let other = f.reopen(&foreign, "owner").await?;
    for id in 1..=18 {
        let host = if id % 2 == 0 { &other } else { &f.host };
        host.admit(f.admission(id, json!(0))).await?;
        let WorkPull::Claimed(lease) = host
            .pull_operation(
                &Worker {
                    id: "wake".into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                OperationId::from_bytes([id; 16]),
                &mut None,
            )
            .await?
        else {
            return Err(Error::NotFound("wake lease".into()));
        };
        host.start_task(&lease, None).await?;
        host.suspend_workflow_command(
            TaskId::from_bytes([id; 16]),
            crate::scheduler::LeaseFence::from(&lease.reservation),
            1,
            (id != 1).then_some(OperationId::from_bytes([99; 16])),
        )
        .await?;
    }
    f.host.cancel(TaskId::from_bytes([3; 16])).await?;
    let first = f.host.workflow_wait_page(0, None, 64).await?;
    assert!(first.events_read <= 64);
    assert!(!first.tasks.contains(&TaskId::from_bytes([17; 16])));
    f.host.cancel(TaskId::from_bytes([17; 16])).await?;
    let mut tasks = first.tasks;
    let mut cursor = first.next_revision;
    while cursor < first.through_revision {
        let page = f
            .host
            .workflow_wait_page(cursor, Some(first.through_revision), 64)
            .await?;
        assert!(page.events_read <= 64 && page.next_revision > cursor);
        assert_eq!(
            page.tasks,
            f.host
                .workflow_wait_page(cursor, Some(first.through_revision), 64)
                .await?
                .tasks
        );
        cursor = page.next_revision;
        tasks.extend(page.tasks);
    }
    tasks.sort();
    tasks.dedup();
    assert_eq!(
        tasks,
        [5, 7, 9, 11, 13, 15].map(|id| TaskId::from_bytes([id; 16]))
    );
    Ok(())
}
