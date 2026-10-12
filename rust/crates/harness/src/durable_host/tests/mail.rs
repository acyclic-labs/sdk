use super::*;

#[tokio::test]
async fn atomic_mail_envelope_reuse_and_hostile_inbox_controls() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(MemoryPayloads::new()?);
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "mail-envelope-owner".into(),
    };
    let issuer = AuthorityIssuer::new("mail-envelope", [7; 32], authority.clone());
    let grants = Capabilities::new([
        "operation:declare".to_owned(),
        "operation:observe".to_owned(),
        "operation:cancel".to_owned(),
        "task:spawn:test.mail@1".to_owned(),
        "mail:send".to_owned(),
        "mail:read".to_owned(),
        payloads
            .volume
            .capability(crate::conversation::VolumeOperation::Read, &mut None)?,
    ]);
    let signed = issuer
        .issue("owner", &grants, None, None, &mut None)
        .expect("scope proof");
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let implementation = Arc::new(BatchMachine {
        identity: MachineIdentity {
            name: "test.mail".into(),
            version: "1".into(),
            digest: [3; 32],
        },
        schema: serde_json::json!({"type":"integer"}),
    });
    let (task_identity, machine, tasks, machines) = test_registries::<Value>(
        implementation,
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"integer"}),
    )?;
    let host = CoordinatorTaskHost::new(
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
        stream.clone(),
        payloads.clone(),
        payloads.clone(),
        &(authority),
        signed,
        issuer.verifier(&mut None).expect("verifier copy"),
        runtime_scope.clone(),
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
        None,
    )?;
    for index in 1..=3_u8 {
        host.admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([index; 16]),
            task: task_identity.clone(),
            machine: machine.clone(),
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    }
    let sender = TaskId::from_bytes([1; 16]);
    let recipient = TaskId::from_bytes([2; 16]);
    let other = TaskId::from_bytes([3; 16]);
    let message = OperationId::from_bytes([8; 16]);
    let payload = payloads.stage(message, "body", b"7", None)?.await?;
    let changed = payloads.stage(message, "changed", b"8", None)?.await?;
    host.send(sender, recipient, message, payload.clone())
        .await?;
    host.send(sender, recipient, message, payload.clone())
        .await?;
    let retained = host
        .mailbox(recipient)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let event = crate::contract::json_from_slice::<MailEvent>(&retained[0].value)
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(event.sender, sender);
    assert_eq!(event.recipient, recipient);
    assert_eq!(event.message_id, message);
    assert_eq!(event.schema_revision, MAIL_SCHEMA_REVISION);
    assert_eq!(event.route_revision, MAIL_ROUTE_REVISION);
    assert_eq!(host.inbox(recipient, 0, 4).await?.len(), 1);
    for (destination, body) in [(other, payload.clone()), (recipient, changed)] {
        assert!(matches!(
            host.send(sender, destination, message, body).await,
            Err(Error::Conflict(_))
        ));
    }
    for (schema_revision, route_revision) in [(2, 1), (1, 2)] {
        let altered = MailEvent {
            schema_revision,
            route_revision,
            ..crate::contract::json_from_slice::<MailEvent>(&retained[0].value)
                .map_err(|error| Error::Storage(error.to_string()))?
        };
        let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&altered)?);
        assert!(matches!(
            host.publish_mail(sender, recipient, message, bytes, None, None, None)
                .await,
            Err(Error::Conflict(_))
        ));
    }
    assert_eq!(host.mailbox(recipient)?.bounds().await?.tail, 1);
    assert!(host.inbox(other, 0, 4).await?.is_empty());
    assert!(host.outcome(recipient).await?.is_none());

    let original =
        serde_json::to_value(&event).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut hostile = Vec::new();
    let mut wrong_recipient = original.clone();
    wrong_recipient["recipient"] = serde_json::json!(other);
    hostile.push(("recipient".to_owned(), wrong_recipient));
    for field in ["schema_revision", "route_revision"] {
        for revision in [0_u32, 2, u32::MAX] {
            let mut value = original.clone();
            value[field] = serde_json::json!(revision);
            hostile.push((format!("{field}:{revision}"), value));
        }
    }
    for field in [
        "sender",
        "recipient",
        "message_id",
        "schema_revision",
        "route_revision",
        "payload",
    ] {
        let mut value = original.clone();
        assert!(
            value
                .as_object_mut()
                .expect("mail object")
                .remove(field)
                .is_some()
        );
        hostile.push((format!("missing:{field}"), value));
    }
    let mut unknown = original.clone();
    unknown["unexpected"] = serde_json::json!(true);
    hostile.push(("unknown-field".into(), unknown));
    for (index, (label, mut value)) in hostile.into_iter().enumerate() {
        let operation = OperationId::from_bytes([100 + index as u8; 16]);
        if value.get("message_id").is_some() {
            value["message_id"] = serde_json::json!(operation);
        }
        let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&value)?);
        host.publish_mail(sender, recipient, operation, bytes, None, None, None)
            .await?;
        assert!(
            matches!(
                host.inbox(recipient, 1 + index as u64, 1).await,
                Err(Error::Storage(_))
            ),
            "hostile envelope exposed: {label}"
        );
    }
    for fault in 0..7_u8 {
        use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest};
        let operation = OperationId::from_bytes([150 + fault; 16]);
        let event = MailEvent {
            sender,
            recipient,
            message_id: operation,
            schema_revision: MAIL_SCHEMA_REVISION,
            route_revision: MAIL_ROUTE_REVISION,
            payload: payload.clone(),
        };
        let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&event)?);
        let mailbox = host.mailbox(recipient)?;
        let sequence = mailbox.bounds().await?.tail;
        let location = MailLocation {
            sender,
            recipient,
            message_id: operation,
            sequence,
            schema_revision: MAIL_SCHEMA_REVISION,
            route_revision: MAIL_ROUTE_REVISION,
            value_digest: *blake3::hash(&bytes).as_bytes(),
        };
        let mut receipt =
            serde_json::to_value(location).map_err(|error| Error::Invalid(error.to_string()))?;
        match fault {
            0 => receipt["schema_revision"] = serde_json::json!(0),
            1 => receipt["schema_revision"] = serde_json::json!(2),
            2 => receipt["route_revision"] = serde_json::json!(0),
            3 => receipt["route_revision"] = serde_json::json!(2),
            4 => {
                receipt
                    .as_object_mut()
                    .expect("location object")
                    .remove("schema_revision");
            }
            5 => {
                receipt
                    .as_object_mut()
                    .expect("location object")
                    .remove("route_revision");
            }
            _ => receipt["unexpected"] = serde_json::json!(true),
        }
        let location_path = acyclic_stream::StreamPath::new(format!(
            "harness/v3/mail/by-intent/{sender}/{operation}"
        ))?;
        let committed = stream
            .commit(CommitRequest {
                conditions: vec![
                    CommitCondition::Tail {
                        path: mailbox.path().clone(),
                        expected: sequence,
                    },
                    CommitCondition::Absent {
                        path: location_path.clone(),
                    },
                ],
                mutations: vec![
                    CommitMutation::Append {
                        path: mailbox.path().clone(),
                        records: vec![bytes],
                    },
                    CommitMutation::Append {
                        path: location_path,
                        records: vec![OwnedBytes::from(crate::contract::canonical_json_bytes(
                            &receipt,
                        )?)],
                    },
                ],
                idempotency_key: StreamKey::new(OwnedBytes::from(vec![200 + fault]))?,
            })
            .await?;
        assert!(matches!(committed, CommitOutcome::Committed(_)));
        assert!(
            matches!(
                host.inbox(recipient, sequence, 1).await,
                Err(Error::Storage(_))
            ),
            "malformed atomic location exposed: {fault}"
        );
    }
    Ok(())
}
