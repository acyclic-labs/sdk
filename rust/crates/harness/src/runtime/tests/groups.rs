use super::*;

#[test]
fn batch_members_have_stable_indexed_identities_and_pinned_authority() -> Result<()> {
        let task = ComponentIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [3; 32],
        };
        let scope = RuntimeScope::new(
            Capabilities::new(["task:spawn:test.batch@1"]),
            Limits::default(),
        )?;
        require_descendant_grant(scope.grants(), &task)?;
        assert!(matches!(
            require_descendant_grant(&Capabilities::new([] as [String; 0]), &task),
            Err(Error::Unauthorized(_))
        ));
        let mut request = DurableBatchRequest {
            group_id: GroupId::from_bytes([1; 16]),
            batch_id: BatchId::from_bytes([2; 16]),
            group_policy: BatchGroupPolicy::CollectAll,
            task,
            machine: MachineIdentity {
                name: "test.batch".into(),
                version: "1".into(),
                digest: [4; 32],
            },
            inputs: vec![
                serde_json::json!({"value": 1}),
                serde_json::json!({"value": 2}),
            ],
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: Some(TaskId::from_bytes([5; 16])),
            scope,
            policy: None,
            execution: None,
        };
        assert_ne!(request.operation_id(0), request.operation_id(1));
        let original = crate::contract::canonical_json_digest(&request.canonical_value(), u64::MAX, &mut None)?;
        let restored = DurableBatchRequest::from_canonical_value(request.canonical_value())?;
        assert_eq!(restored.canonical_value(), request.canonical_value());
        let mut unknown = request.canonical_value();
        unknown
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("legacy_manifest".into(), Value::Null);
        assert!(DurableBatchRequest::from_canonical_value(unknown).is_err());
        let mut missing_policy = request.canonical_value();
        missing_policy
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .remove("group_policy");
        assert!(DurableBatchRequest::from_canonical_value(missing_policy).is_err());
        let mut invalid_input = request.canonical_value();
        invalid_input
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("inputs".into(), serde_json::json!([1]));
        assert!(DurableBatchRequest::from_canonical_value(invalid_input).is_err());
        let mut invalid_schema = request.canonical_value();
        invalid_schema
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("input_schema".into(), serde_json::json!({}));
        assert!(DurableBatchRequest::from_canonical_value(invalid_schema).is_err());
        request.inputs.swap(0, 1);
        assert_ne!(
            original,
            crate::contract::canonical_json_digest(&request.canonical_value(), u64::MAX, &mut None)?
        );
        request.inputs.swap(0, 1);
        request.scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        assert_ne!(
            original,
            crate::contract::canonical_json_digest(&request.canonical_value(), u64::MAX, &mut None)?
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
                record: TaskAdmissionRecord::from_canonical_value(
                    serde_json::from_str(include_str!("../../../fixtures/v2/task-admission.json"))
                        .map_err(|error| Error::Invalid(format!("fixture is invalid: {error}")))?,
                    &mut None,
                )?.0,
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
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(
                Capabilities::new(["task:spawn:test.recursive@1"]),
                Limits::default(),
            )?,
            concurrency: 1,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
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
                match group.map(&leaf, inputs).await {
                    RuntimeMap::Complete(values) => Ok(values),
                    RuntimeMap::Incomplete(entries) => {
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
                        let partial =
                            group.quorum(members, 2).await.err().ok_or_else(|| {
                                Error::Conflict("impossible quorum succeeded".into())
                            })?;
                        assert_eq!(partial.required, 2);
                        assert_eq!(partial.successes.len(), 1);
                        assert_eq!(partial.successes[0].1, 2);
                        assert_eq!(partial.others.len(), 1);
                        Ok(Vec::new())
                    }
                }
            },
        )?)?;
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(
                Capabilities::new(["task:spawn:test.map.leaf@1"]),
                Limits::default(),
            )?,
            concurrency: 1,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
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
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
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
