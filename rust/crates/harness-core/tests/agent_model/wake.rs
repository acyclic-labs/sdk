//! Primitive-backed ordering, cursor and calendar regressions; no model calls.
use super::*;
use acyclic_harness_core::agent::wake::{
    Cron, Lifetime, Missed, Monitor, MonitorCursor, MonitorFilter, Recurrence, Schedule,
};
use acyclic_stream::{AppendRequest, IdempotencyKey};

fn notice(index: usize, wake: bool) -> InboxRecord {
    InboxRecord::Notice {
        id: intent(&format!("event-{index}")),
        notice: acyclic_harness_core::conversation::wire::Notice {
            source: "events/source".into(),
            kind: "test.event".into(),
            body: serde_json::json!({"index": index}),
            wake,
            coalesce_key: None,
        },
    }
}

#[tokio::test]
async fn steering_keeps_followups_and_interrupts_behind_their_source_barriers() -> Result {
    let stream = MemoryStream::default();
    let path = create(&stream, id(1)).await?;
    let message = Message {
        from: user(),
        id: intent("followup"),
        body: Box::new(super::driver::file("message.md")?),
        attachments: None,
    };
    for record in [
        notice(1, false),
        notice(2, true),
        InboxRecord::FollowUp(message),
        notice(3, true),
        cancel("stop"),
        notice(4, true),
    ] {
        submit(&stream, send(&path, &user(), &record)?).await?;
    }
    let first = select_boundary(&stream, &path, 1, 0, None).await?;
    assert_eq!((first.from, first.through, first.inputs.len()), (1, 3, 2));
    // An explicitly started follow-up can admit steering that arrived after it.
    let next = select_boundary(&stream, &path, 4, 3, None).await?;
    assert_eq!((next.through, next.inputs.len()), (5, 1));
    assert_eq!(
        select_boundary(&stream, &path, 5, 3, None).await?.through,
        5
    );
    // The frozen range cannot absorb records that arrived after preparation.
    assert_eq!(
        select_boundary(&stream, &path, 1, 0, Some(first.through)).await?,
        first
    );
    assert!(
        select_boundary(&stream, &path, 1, 0, Some(4))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn monitor_resume_uses_source_commit_identity_and_provider_filter() -> Result {
    let stream = MemoryStream::default();
    let source = StreamPath::new("source/events")?;
    stream
        .append(AppendRequest {
            path: source.clone(),
            records: vec![
                acyclic_stream::AppendRecord::latest("quiet", "q", false)?,
                acyclic_stream::AppendRecord::latest("attention", "a", true)?,
            ],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(b"source".to_vec())?,
        })
        .await?;
    let monitor = Monitor {
        version: 1,
        id: intent("monitor"),
        source: source.as_str().into(),
        cursor: MonitorCursor {
            next: 0,
            preceding: None,
        },
        filter: MonitorFilter {
            keys: Vec::new(),
            attributes: [("attention".into(), vec![b"true".to_vec()])].into(),
        },
        wake: true,
        deadline_unix_ms: None,
        coalesce_key: None,
    };
    let mut follow = monitor.follow(&stream).await?;
    let (matched, cursor) = loop {
        let event = follow.next().await?.ok_or("source ended")?;
        if let Some(record) = event.notice {
            break (record, event.cursor);
        }
    };
    assert_eq!(cursor.next, 2);
    let InboxRecord::Notice { notice, .. } = &matched else {
        return Err("missing notice".into());
    };
    assert_eq!(notice.body["sequence"], 1);
    assert!(notice.wake);
    let replayed = Monitor {
        cursor: MonitorCursor {
            next: 0,
            preceding: None,
        },
        ..monitor.clone()
    };
    let mut replay = replayed.follow(&stream).await?;
    let replayed = loop {
        if let Some(record) = replay.next().await?.ok_or("source ended")?.notice {
            break record;
        }
    };
    assert_eq!(matched, replayed);
    let mut resumed = Monitor { cursor, ..monitor }.follow(&stream).await?;
    // Append after follow registration: delivery is the primitive's event.
    stream
        .append(AppendRequest {
            path: source,
            records: vec![acyclic_stream::AppendRecord::latest("next", "b", true)?],
            if_tail: Some(2),
            idempotency_key: IdempotencyKey::new(b"next".to_vec())?,
        })
        .await?;
    loop {
        let event = resumed.next().await?.ok_or("source ended")?;
        if let Some(InboxRecord::Notice { notice, .. }) = event.notice {
            assert_eq!(notice.body["sequence"], 2);
            break;
        }
    }
    Ok(())
}

fn timestamp(text: &str) -> Result<u64> {
    Ok(u64::try_from(
        text.parse::<jiff::Timestamp>()?.as_millisecond(),
    )?)
}

#[test]
fn cron_skips_spring_gaps_and_retains_both_autumn_instants() -> Result {
    let cron = Cron::parse("30 1 * * *")?;
    let first = cron
        .next(timestamp("2026-10-25T00:00:00Z")?, "Europe/London")?
        .ok_or("missing first")?;
    let second = cron.next(first, "Europe/London")?.ok_or("missing second")?;
    assert_eq!(first, timestamp("2026-10-25T00:30:00Z")?);
    assert_eq!(second, timestamp("2026-10-25T01:30:00Z")?);
    assert_eq!(
        cron.next(timestamp("2026-03-29T00:00:00Z")?, "Europe/London")?,
        Some(timestamp("2026-03-30T00:30:00Z")?)
    );
    assert_eq!(
        Cron::parse("0 0 31 2 *")?.next(timestamp("2026-01-01T00:00:00Z")?, "UTC")?,
        None
    );
    Ok(())
}

#[test]
fn schedule_restart_preserves_trigger_identity_and_explicit_lifetime() -> Result {
    let schedule = Schedule {
        version: 1,
        id: intent("cadence"),
        owner: AgentPath::root(&id(1)),
        body: serde_json::json!({"prompt": "continue"}),
        timezone: "UTC".into(),
        next_due_unix_ms: 100,
        recurrence: Some(Recurrence::Every { milliseconds: 10 }),
        missed: Missed::CatchUp,
        lifetime: Lifetime::Session,
        expires_unix_ms: None,
        remaining: None,
    };
    let first = schedule.fire(135, false)?.ok_or("not fired")?;
    let restart = schedule.fire(135, false)?.ok_or("not fired")?;
    assert_eq!(first.record, restart.record);
    assert_eq!(
        first.next.ok_or("missing recurrence")?.next_due_unix_ms,
        110
    );
    let coalesced = Schedule {
        missed: Missed::Coalesce,
        ..schedule.clone()
    };
    assert_eq!(
        coalesced
            .fire(135, false)?
            .ok_or("not fired")?
            .next
            .ok_or("missing recurrence")?
            .next_due_unix_ms,
        140
    );
    assert!(schedule.fire(135, true)?.is_none());
    let daemon = Schedule {
        lifetime: Lifetime::Daemon,
        ..schedule
    };
    assert!(daemon.fire(135, true)?.is_some());
    Ok(())
}
