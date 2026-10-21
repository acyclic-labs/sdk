use super::*;

#[test]
fn batch_members_have_stable_indexed_identities_and_pinned_authority() -> Result<()> {
    let admission = admission_fixture()?;
    let scope = RuntimeScope::new(
        Capabilities::new(["task:spawn:test.task@1"]),
        Limits::default(),
    )?;
    require_descendant_grant(scope.grants(), &admission.task)?;
    assert!(matches!(
        require_descendant_grant(&Capabilities::default(), &admission.task),
        Err(Error::Unauthorized(_))
    ));
    let mut request = DurableBatchRequest {
        group_id: GroupId::from_bytes([1; 16]),
        batch_id: BatchId::from_bytes([2; 16]),
        group_policy: BatchGroupPolicy::CollectAll,
        task: admission.task,
        machine: admission.machine,
        inputs: vec![Value::from(1), Value::from(2)],
        input_schema: admission.input_schema,
        output_schema: admission.output_schema,
        parent: Some(TaskId::from_bytes([5; 16])),
        scope,
        policy: None,
        execution: None,
    };
    assert_ne!(request.operation_id(0), request.operation_id(1));
    let original = crate::executor::preparation_digest(&request.canonical_value(), None)?;
    let restored = DurableBatchRequest::from_canonical_value(request.canonical_value())?;
    assert_eq!(restored.canonical_value(), request.canonical_value());
    for (key, value) in [
        ("legacy_manifest", Value::Null),
        ("inputs", serde_json::json!([false])),
        ("input_schema", serde_json::json!({})),
    ] {
        let mut invalid = request.canonical_value();
        invalid[key] = value;
        assert!(DurableBatchRequest::from_canonical_value(invalid).is_err());
    }
    let mut missing = request.canonical_value();
    missing.as_object_mut().unwrap().remove("group_policy");
    assert!(DurableBatchRequest::from_canonical_value(missing).is_err());
    request.inputs.swap(0, 1);
    assert_ne!(
        original,
        crate::executor::preparation_digest(&request.canonical_value(), None)?
    );
    request.inputs.swap(0, 1);
    request.scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
    assert_ne!(
        original,
        crate::executor::preparation_digest(&request.canonical_value(), None)?
    );
    Ok(())
}

#[tokio::test]
async fn batch_join_keeps_rejection_and_unknown_admission_in_original_slots() -> Result<()> {
    let group_id = GroupId::from_bytes([20; 16]);
    let batch_id = BatchId::from_bytes([21; 16]);
    let batch = AdmissionBatch::<u32> {
        group_id,
        batch_id,
        policy: BatchGroupPolicy::CollectAll,
        spawner: Arc::new(SplitState {
            task_id: TaskId::from_bytes([23; 16]),
            record: admission_fixture()?,
        }),
        extensions: None,
        entries: vec![
            BatchEntry {
                key: InputKey { batch_id, index: 0 },
                admission: Admission::Rejected {
                    reason: "capacity".into(),
                },
            },
            BatchEntry {
                key: InputKey { batch_id, index: 1 },
                admission: Admission::Indeterminate {
                    operation_id: OperationId::from_bytes([22; 16]),
                },
            },
        ],
    };
    let observed = batch.join().await;
    assert_eq!(observed.group_id, group_id);
    assert_eq!(observed.batch_id, batch_id);
    assert!(!observed.is_complete());
    assert!(matches!(
        observed.entries[0].admission,
        Admission::Rejected { .. }
    ));
    assert!(matches!(
        observed.entries[1].admission,
        Admission::Indeterminate { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn recursive_child_admission_does_not_consume_the_parent_permit() -> Result<()> {
    let mut tasks = TaskRegistry::default();
    let definition =
        TaskDefinition::live("test.recursive", "1", |context, depth: u32| async move {
            if depth == 0 {
                return Ok(0_u32);
            }
            let definition = context.task::<u32, u32>("test.recursive")?;
            match context
                .spawn(&definition, depth - 1)
                .await?
                .result()
                .await?
            {
                Outcome::Succeeded(value) => Ok(value + 1),
                outcome => Err(Error::Conflict(format!(
                    "recursive child did not succeed: {outcome:?}"
                ))),
            }
        })?;
    tasks.register(definition)?;
    let harness = local_runtime(tasks, &["task:spawn:test.recursive@1"], 1)?;
    let definition = harness.task::<u32, u32>("test.recursive")?;
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        harness.spawn(&definition, 128).await?.result(),
    )
    .await
    .map_err(|error| Error::Conflict(error.to_string()))??;
    assert_eq!(observed, Outcome::Succeeded(128));
    Ok(())
}

#[tokio::test]
async fn group_map_preserves_input_order_and_partial_failure() -> Result<()> {
    let mut tasks = TaskRegistry::default();
    tasks.register(TaskDefinition::live(
        "test.map.leaf",
        "1",
        |_, value: u32| async move {
            if value == 3 {
                Err(Error::Conflict("leaf rejected three".into()))
            } else {
                Ok(value * 2)
            }
        },
    )?)?;
    tasks.register(TaskDefinition::live(
        "test.map.parent",
        "1",
        |context, inputs: Vec<u32>| async move {
            let leaf = context.task::<u32, u32>("test.map.leaf")?;
            let group = context.group();
            let entries = group.map(&leaf, inputs).await;
            if entries
                .iter()
                .all(|entry| matches!(entry, Ok(Outcome::Succeeded(_))))
            {
                return entries
                    .into_iter()
                    .map(|entry| match entry {
                        Ok(Outcome::Succeeded(value)) => Ok(value),
                        _ => unreachable!(),
                    })
                    .collect();
            }
            assert_eq!(entries.len(), 4);
            assert!(matches!(entries[0], Ok(Outcome::Succeeded(2))));
            assert!(matches!(entries[1], Ok(Outcome::Succeeded(4))));
            assert!(matches!(&entries[2], Ok(Outcome::Failed { .. })));
            assert!(matches!(entries[3], Ok(Outcome::Succeeded(8))));
            let members = group
                .spawn_many(&leaf, [1, 3])
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            let partial = group
                .quorum(members, 2)
                .await
                .err()
                .ok_or_else(|| Error::Conflict("impossible quorum succeeded".into()))?;
            assert_eq!(partial.len(), 2);
            assert!(
                partial
                    .iter()
                    .any(|(_, result)| matches!(result, Ok(Outcome::Succeeded(2))))
            );
            assert!(
                partial
                    .iter()
                    .any(|(_, result)| matches!(result, Ok(Outcome::Failed { .. })))
            );
            Ok(Vec::new())
        },
    )?)?;
    let harness = local_runtime(tasks, &["task:spawn:test.map.leaf@1"], 1)?;
    let parent = harness.task::<Vec<u32>, Vec<u32>>("test.map.parent")?;
    assert_eq!(
        harness
            .spawn(&parent, vec![1, 2, 3, 4])
            .await?
            .result()
            .await?,
        Outcome::Succeeded(Vec::new())
    );
    assert_eq!(
        harness
            .spawn(&parent, vec![4, 2, 1])
            .await?
            .result()
            .await?,
        Outcome::Succeeded(vec![8, 4, 2])
    );
    Ok(())
}

#[tokio::test]
async fn ordered_join_polls_later_tasks_while_first_is_waiting() -> Result<()> {
    let signal = Arc::new(tokio::sync::Notify::new());
    let mut tasks = TaskRegistry::default();
    let definition = TaskDefinition::live("test.join", "1", {
        let signal = Arc::clone(&signal);
        move |_, wait: bool| {
            let signal = Arc::clone(&signal);
            async move {
                if wait {
                    signal.notified().await;
                } else {
                    signal.notify_one();
                }
                Ok(wait)
            }
        }
    })?;
    tasks.register(definition)?;
    let harness = local_runtime(tasks, &[], 2)?;
    let definition = harness.task::<bool, bool>("test.join")?;
    let first = harness.spawn(&definition, true).await?;
    let second = harness.spawn(&definition, false).await?;
    let joined = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        join_runtime(vec![first, second]),
    )
    .await
    .map_err(|error| Error::Conflict(error.to_string()))?;
    assert_eq!(joined.len(), 2);
    assert!(matches!(&joined[0], Ok(Outcome::Succeeded(true))));
    assert!(matches!(&joined[1], Ok(Outcome::Succeeded(false))));
    Ok(())
}
