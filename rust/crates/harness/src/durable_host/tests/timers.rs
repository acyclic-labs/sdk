use super::*;

#[tokio::test]
async fn sdk_and_native_timer_retries_share_fenced_retention() -> Result<()> {
    let f = fixture::fixture().await?;
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
            f.host.wait_until(task, operation, 1).await?;
        }
        assert!(f.host.schedule_timer(task, &fence, &record, None).await?);
        assert!(f.host.poll_timer(task, &fence, operation, 1, None).await?);
        f.host.wait_until(task, operation, 1).await?;
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
    let _replacement = f.replace(1).await?;
    assert!(
        f.host
            .schedule_timer(task, &fence, &record, None)
            .await
            .is_err()
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
