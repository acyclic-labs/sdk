use super::*;
use crate::conversation::ContentPublisher;

#[tokio::test]
async fn b08_one_intent_keeps_recipient_and_payload() -> Result<()> {
    let stream = Arc::new(crate::test_stream::LostSessionAck::default());
    let f = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    for id in 1..=3 {
        f.host.admit(f.admission(id, json!(0))).await?;
    }
    let sender = TaskId::from_bytes([1; 16]);
    let recipient = TaskId::from_bytes([2; 16]);
    let message = OperationId::from_bytes([4; 16]);
    let payload = f
        .publisher
        .stage(
            message,
            "body.json",
            b"7",
            "application/json",
            "body.json",
            None,
        )?
        .await?
        .0;
    stream
        .commit_lose_ack
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let _reply = f
        .host
        .send(sender, recipient, message, payload.clone())
        .await;
    f.host
        .send(sender, recipient, message, payload.clone())
        .await?;
    let inbox = f.host.inbox(recipient, 0, 2).await?;
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].payload, payload);
    assert!(matches!(
        f.host
            .send(
                sender,
                TaskId::from_bytes([3; 16]),
                message,
                payload.clone()
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let changed = f
        .publisher
        .stage(
            message,
            "changed.json",
            b"8",
            "application/json",
            "changed.json",
            None,
        )?
        .await?
        .0;
    assert!(matches!(
        f.host.send(sender, recipient, message, changed).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(f.host.inbox(recipient, 0, 2).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn b09_inbox_rejects_hostile_envelopes() -> Result<()> {
    let f = fixture::fixture().await?;
    for id in 1..=2 {
        f.host.admit(f.admission(id, json!(0))).await?;
    }
    let recipient = TaskId::from_bytes([2; 16]);
    let message = OperationId::from_bytes([5; 16]);
    let payload = f
        .publisher
        .stage(
            message,
            "body.json",
            b"7",
            "application/json",
            "body.json",
            None,
        )?
        .await?
        .0;
    f.host
        .send(TaskId::from_bytes([1; 16]), recipient, message, payload)
        .await?;
    let mailbox = f.host.mailbox(recipient)?;
    let records = mailbox.read(0, 1).await?.try_collect::<Vec<_>>().await?;
    let original: Value = crate::contract::json_from_slice(&records[0].value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for (index, field) in ["sender", "recipient", "message_id", "payload"]
        .into_iter()
        .enumerate()
    {
        let mut changed = original.clone();
        changed[field] = Value::Null;
        mailbox
            .append_at(
                OwnedBytes::from(crate::contract::canonical_json_bytes(&changed)?),
                1 + index as u64,
            )
            .await?;
        assert!(f.host.inbox(recipient, 1 + index as u64, 1).await.is_err());
    }
    assert!(f.host.inbox(recipient, 0, 0).await.is_err());
    assert_eq!(f.host.inbox(recipient, 0, 1).await?.len(), 1);
    Ok(())
}

