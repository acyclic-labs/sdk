use super::*;

#[tokio::test]
async fn sdk_and_native_timer_retries_share_fenced_retention() -> Result<()> {
    let f = Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[crate::contract::capability::TIMER_WAIT],
    )
    .await?;
    f.host.admit(f.admission(1, json!(0))).await?;
    let lease = f.claim(1, "timer").await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let task = TaskId::from_bytes([1; 16]);
    for (index, first) in [(2, 0), (3, 1), (4, 2)] {
        let operation = OperationId::from_bytes([index; 16]);
        let record =
            acyclic_actors::timers::TimerRecord::new(task.to_string(), operation.to_string(), 1)?;
        if first == 0 {
            assert!(f.host.schedule_timer(task, &fence, &record, None).await?);
        } else if first == 1 {
            assert!(f.host.poll_timer(task, &fence, operation, 1, None).await?);
        } else {
            assert!(f.host.wait_until(task, &fence, operation, 1, None).await?);
        }
        assert!(f.host.schedule_timer(task, &fence, &record, None).await?);
        assert!(f.host.poll_timer(task, &fence, operation, 1, None).await?);
        assert!(f.host.wait_until(task, &fence, operation, 1, None).await?);
        let (tail, found) = f.host.timer_state(task, operation, 1, &mut None).await?;
        assert!(found);
        assert_eq!(tail, u64::from(index - 1));
        let changed = acyclic_actors::timers::TimerRecord {
            deadline_unix_ms: 2,
            ..record
        };
        assert!(matches!(
            f.host.schedule_timer(task, &fence, &changed, None).await,
            Err(Error::Conflict(_))
        ));
    }
    let record = acyclic_actors::timers::TimerRecord::new(
        task.to_string(),
        OperationId::from_bytes([2; 16]).to_string(),
        1,
    )?;
    let replacement = f.replace(1).await?;
    let replacement_fence = replacement.task_binding().1;
    for (index, entry) in [(5, 0), (6, 1), (7, 2)] {
        let operation = OperationId::from_bytes([index; 16]);
        let fresh =
            acyclic_actors::timers::TimerRecord::new(task.to_string(), operation.to_string(), 1)?;
        let rejected = match entry {
            0 => f
                .host
                .schedule_timer(task, &fence, &fresh, None)
                .await
                .map(|_| ()),
            1 => f
                .host
                .poll_timer(task, &fence, operation, 1, None)
                .await
                .map(|_| ()),
            _ => f
                .host
                .wait_until(task, &fence, operation, 1, None)
                .await
                .map(|_| ()),
        };
        assert!(
            matches!(rejected, Err(Error::Unauthorized(ref reason))
                if reason == "stale or foreign lease fence"),
            "unexpected stale entry point {entry} result: {rejected:?}"
        );
        let (tail, found) = f.host.timer_state(task, operation, 1, &mut None).await?;
        assert!(!found);
        assert_eq!(tail, u64::from(index - 2));
        // A rejected stale deadline must not conflict with the new owner's deadline.
        assert!(
            f.host
                .poll_timer(task, replacement_fence, operation, 2, None)
                .await?
        );
    }
    let pending_operation = OperationId::from_bytes([8; 16]);
    // Retention returns readiness; it must not wait for or poll the runtime's clock.
    let ready = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        f.host
            .wait_until(task, replacement_fence, pending_operation, u64::MAX, None),
    )
    .await
    .expect("scoped retention waited for the deadline")?;
    assert!(!ready);
    assert!(
        !f.host
            .poll_timer(task, replacement_fence, pending_operation, u64::MAX, None)
            .await?
    );
    let pending = acyclic_actors::timers::TimerRecord::new(
        task.to_string(),
        pending_operation.to_string(),
        u64::MAX,
    )?;
    assert!(
        !f.host
            .schedule_timer(task, replacement_fence, &pending, None)
            .await?
    );
    let other = acyclic_actors::timers::TimerRecord {
        task_id: TaskId::from_bytes([9; 16]).to_string(),
        ..record
    };
    assert!(matches!(
        f.host.schedule_timer(task, &fence, &other, None).await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
