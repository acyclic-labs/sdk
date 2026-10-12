//! Bounded controls use actual allocated resources and the production machine.

use super::*;
use crate::distributed::SchedulerPayloadStore as _;
use crate::filesystem::{ForkPreparationMachine, ForkToolArguments, FORK_PREPARE_TASK_COMMAND_KIND};
use crate::workflow::{MachineStatus, ResumableMachine, WorkflowCommand};

#[tokio::test]
async fn fork_preparation_outbox_replay_binds_actual_report_and_original_request() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let operation = created.report.request.operation_id;
    let bytes = crate::contract::canonical_json_bytes(&created.request.payload)?;
    let payload = fixture.host.payloads.stage(operation, "recursive-request", &bytes).await?;
    let command = WorkflowCommand { operation_id: operation,
        kind: FORK_PREPARE_TASK_COMMAND_KIND.into(), payload };
    let mut input = created.request.clone();
    input.command = Some(command.clone());
    let machine = ForkPreparationMachine::new();
    let input_value = serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?;
    let initial = machine.initialize(&input_value)?;
    let admitted = machine.transition(&initial, &Value::Null)?;
    assert_eq!(admitted.commands, [command]);
    assert!(matches!(admitted.status, MachineStatus::Suspended));
    let reopened = ForkPreparationMachine::new();
    let receipt = serde_json::json!({"commands":[{"operation_id":operation,
        "value":Outcome::Succeeded(created.report.clone())}]});
    let settled = reopened.transition(&admitted.state, &receipt)?;
    let MachineStatus::Completed { value } = &settled.status else {
        return Err(Error::Invalid("actual preparation did not settle".into()));
    };
    let report: crate::fork::ForkReport = serde_json::from_value(value.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(report, created.report);
    assert!(reopened.transition(&settled.state, &receipt).is_err());

    for count in 0..=3 {
        for changed_identity in [false, true] {
            let observation = serde_json::json!({"operation_id": if changed_identity {
                OperationId::from_bytes([201;16])
            } else { operation }, "value":Outcome::Succeeded(created.report.clone())});
            let candidates = Value::Array(vec![observation; count]);
            let result = reopened.transition(&admitted.state, &serde_json::json!({"commands":candidates}));
            assert_eq!(result.is_ok(), count == 1 && !changed_identity);
        }
    }
    for field in ["parent_revision", "inherited_bytes", "child_agent", "source_project"] {
        let mut changed = created.report.clone();
        match field {
            "parent_revision" => {
                changed.request.parent_revision += 1;
                for selection in &mut changed.request.selections {
                    if let crate::fork::ResourceRevision::History(reference) = &mut selection.revision {
                        *reference = crate::resources::StreamRef::new(reference.as_resource().provider().clone(),
                            reference.as_resource().key().to_vec(), Some(changed.request.parent_revision.to_string()))?;
                    }
                }
                for capture in &mut changed.captures {
                    if let crate::fork::Capture::Captured(resource) = capture
                        && let crate::fork::ResourceRevision::History(reference) = &mut resource.source {
                        *reference = crate::resources::StreamRef::new(reference.as_resource().provider().clone(),
                            reference.as_resource().key().to_vec(), Some(changed.request.parent_revision.to_string()))?;
                        resource.revision = resource.source.clone();
                    }
                }
            }
            "inherited_bytes" => changed.request.preparation.maximum_inherited_bytes += 1,
            "child_agent" => changed.request.child_agent = AgentId::from_bytes([202;16]),
            _ => changed.request.selections.reverse(),
        }
        let changed = serde_json::json!({"commands":[{"operation_id":operation,"value":Outcome::Succeeded(changed)}]});
        assert!(reopened.transition(&admitted.state, &changed).is_err(), "{field}");
    }
    let uncertain = serde_json::json!({"commands":[{"operation_id":operation,
        "value":Outcome::<crate::fork::ForkReport>::Indeterminate { operation_id: operation }}]});
    assert!(reopened.transition(&admitted.state, &uncertain).is_err());

    let preparation_task = TaskId::from_bytes([83;16]);
    let crate::distributed::WorkPull::Claimed(lease) = fixture.host.recover_work(preparation_task).await? else {
        return Err(Error::NotFound("original preparation lease".into()));
    };
    let owner = Arc::new(fixture.host.journal_owner(preparation_task, (&lease.reservation).into()).await?);
    assert!(owner.resource_preparation(input).await.is_err());
    assert!(owner.resource_preparation(created.request).await.is_ok());
    Ok(())
}

#[tokio::test]
async fn chosen_child_default_source_and_foreign_project_do_not_import_transcripts() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let original = created.report.request;
    let arguments = ForkToolArguments { id: original.child_agent, msg: "review exact source".into(), source: None };
    assert_eq!(arguments.source_task(fixture.root, fixture.root_scope.limits(), &mut None)?, fixture.root);
    let project = original.selections.iter().find_map(|selection| match &selection.revision {
        crate::fork::ResourceRevision::Project { volume, generation } => Some((volume.clone(), generation.clone())),
        _ => None,
    }).ok_or_else(|| Error::Invalid("actual fork project missing".into()))?;
    let mut request = original.clone();
    arguments.select_project(fixture.root, fixture.root, project.0.clone(), project.1.clone(), fixture.root_scope.limits(), &mut request)?;
    assert_eq!(request, original);
    let selected = TaskId::from_bytes([205;16]);
    let explicit = ForkToolArguments { source: Some(selected), ..arguments.clone() };
    let foreign = VolumeRef::new(fixture.filesystem_provider.clone(), "authorized-review-source", VolumeClass::Project,
        VolumeOwner::Project("created-project".into()))?;
    let generation = fixture.filesystem.create_volume(&foreign).await?.generation;
    explicit.select_project(fixture.root, selected, foreign, generation, fixture.root_scope.limits(), &mut request)?;
    assert_eq!(request.parent, original.parent);
    assert_eq!(request.parent_revision, original.parent_revision);
    assert_eq!(request.child_agent, arguments.id);
    for (before, after) in original.selections.iter().zip(&request.selections) {
        if !matches!(before.revision, crate::fork::ResourceRevision::Project { .. }) {
            assert_eq!(before, after, "foreign workspace selection must not inject history/scratch");
        }
    }
    let mut unchanged = original.clone();
    assert!(explicit.select_project(fixture.root, fixture.root, project.0, project.1,
        fixture.root_scope.limits(), &mut unchanged).is_err());
    assert_eq!(unchanged, original);
    assert!(serde_json::from_value::<ForkToolArguments>(serde_json::json!({
        "id":arguments.id,"msg":"review","effect_operation":OperationId::from_bytes([206;16])
    })).is_err());
    Ok(())
}

#[tokio::test]
async fn authenticated_output_selection_preserves_child_authority_and_rejects_root_fallback() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(_)));
    let task = created.request.child.task_id();
    let context = fixture.harness.durable_context(task, created.request.child.operation_id).await?;
    let original_grants = context.scope().grants().clone();
    let original_task = context.durable_task_id();
    let binding = context.created_content_bindings(&created.private)?;
    let selected = context.clone().with_output_volume(&created.private)?;
    assert_eq!(selected.durable_task_id(), original_task);
    assert_eq!(selected.scope().grants(), &original_grants);
    let same = selected.created_content_bindings(&created.private)?;
    assert!(Arc::ptr_eq(&binding.reader, &same.reader));
    assert!(Arc::ptr_eq(binding.writer.as_ref().ok_or_else(|| Error::Invalid("created writer missing".into()))?,
        same.writer.as_ref().ok_or_else(|| Error::Invalid("selected writer missing".into()))?));
    let root_volume = VolumeRef::new(fixture.filesystem_provider.clone(), "scheduler",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([80;16])))?;
    assert!(selected.created_content_bindings(&root_volume).is_err());
    assert!(context.clone().with_output_volume(&root_volume).is_err());
    let root = fixture.harness.durable_context(fixture.root, fixture.root_lease.operation.operation_id).await?;
    assert!(root.created_content_bindings(&created.private).is_err());
    let file = selected.stage_file(OperationId::from_bytes([207;16]), "selected/output.txt",
        b"original child destination", "text/plain", "output.txt").await?;
    assert_eq!(file.volume(), &created.private);
    assert_eq!(selected.read_file(&file).await?.as_ref(), b"original child destination".as_ref());
    assert_eq!(context.scope().grants(), &original_grants);
    Ok(())
}
