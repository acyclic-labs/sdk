use acyclic_harness::TaskId;
use super::*;
use crate::pi_fork::tests::work;
use crate::{pi_caller, pi_fork::tests::Fixture};
use acyclic_harness::{core::Scope, filesystem::FilesystemExecutionJournal,
    executor::ExecutionJournal, interaction::{Interaction, InteractionResponse}, tool::ToolApprovalVerifier,
    InteractionId};

#[test]
fn operator_cli_requires_explicit_original_operation_and_decision() {
    let args = |items: &[&str]| items.iter().map(|item| (*item).to_owned()).collect::<Vec<_>>();
    assert!(parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk", "--json"])).is_ok());
    assert!(parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk"])).is_err());
    assert!(parse_cli(&args(&["approve", "--session", "root", "--operation", "sdk", "--digest", &"a".repeat(64), "--decision", "approve"])).is_ok());
    assert!(parse_cli(&args(&["approve", "--session", "root", "--operation", "sdk", "--digest", &"a".repeat(64)])).is_err());
    assert!(parse_cli(&args(&["approve", "--session", "root", "--operation", "sdk", "--digest", "bad", "--decision", "approve"])).is_err());
    assert!(parse_cli(&args(&["approvals", "--session", "root", "--session", "foreign", "--operation", "sdk", "--json"])).is_err());
    let metadata = parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk", "--json"])).unwrap();
    assert!(!metadata.project_files);
    let evidence = parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk", "--json",
        "--project-files", "--file", "selected.txt", "--offset", "262144", "--maximum-bytes", "4096"])).unwrap();
    assert!(evidence.project_files);
    assert_eq!(evidence.offset, 262_144);
    assert_eq!(evidence.maximum_bytes, 4096);
    assert!(parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk", "--json",
        "--file", "selected.txt"])).is_err());
    assert!(parse_cli(&args(&["approvals", "--session", "root", "--operation", "sdk", "--json",
        "--project-files", "--file", "selected.txt", "--maximum-bytes", "262145"])).is_err());
}

#[test]
fn prepared_descriptor_admission_fails_before_typed_growth_without_fake_allocation_work() {
    let original_work = work();
    let mut existing_custody = original_work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).unwrap();
    existing_custody.resize_retained(original_work.remaining().unwrap().peak_allocation_bytes).unwrap();
    let result = decode_prepared_approval(b"{}", &original_work);
    assert!(matches!(&result, Err(Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }),
        custody: Some(_),
    })));
    if let Err(Error::NativeWork { custody: Some(custody), .. }) = &result {
        assert!(custody.shares_ledger(&existing_custody));
    }
}

#[tokio::test]
async fn real_root_operator_rejects_pi_caller_forgery_foreign_operation_and_foreign_owner() {
    let fixture = Fixture::new(None).await;
    let state = crate::AdapterState { root_session_id: fixture.root_request.binding.native_session_id.clone(),
        root_agent_id: "fork-root".into(), task_binding: crate::PluginTaskBinding::Pi(Box::new(fixture.root_request.binding.clone())),
        ..crate::AdapterState::default() };
    let body = PiOperatorBody::Inspect { session: state.root_session_id.clone(), operation: "original-sdk-writeback".into(),
        project_files: false, file_evidence: None };
    let original_work = work();
    let request_custody;
    let request;
    (request, request_custody) = build_control_request(&fixture.owner, std::env::current_dir().unwrap(), body, &original_work).unwrap();
    let parsed_custody;
    let parsed_request;
    (parsed_request, parsed_custody) = PiOperatorRequest::from_value_with_native_work(&request.arguments, &original_work).unwrap();
    assert!(parsed_custody.shares_ledger(&request_custody));
    drop(request);
    drop(request_custody);
    let mut request = parsed_request;
    verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).unwrap();
    let caller = pi_caller::issue(&fixture.owner, &state.root_session_id, &fixture.root_request.binding, &original_work).unwrap();
    let original = request.operator.clone();
    let original_body = request.body.clone();
    request.operator = caller.0;
    assert!(verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).is_err());
    request.operator = original;
    let PiOperatorBody::Inspect { project_files, .. } = &mut request.body else { unreachable!() };
    *project_files = true;
    assert!(verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).is_err());
    request.body = PiOperatorBody::Inspect { session: state.root_session_id.clone(), operation: "foreign-sdk-operation".into(),
        project_files: false, file_evidence: None };
    assert!(verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).is_err());
    request.body = original_body;
    verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).unwrap();
    let foreign = Fixture::new(None).await;
    assert!(verify_request(&foreign.owner, &state.root_session_id, &request, &original_work).is_err());
    let scope: Scope = fixture.owner.authority().unwrap().issuer.issue(&request.operator.id(), request.operator.capabilities(), Some(fixture.commit.child.agent_id), None, &mut None).expect("scope proof");
    request.operator = scope;
    assert!(verify_request(&fixture.owner, &state.root_session_id, &request, &original_work).is_err());
    drop(request);
    drop(parsed_custody);
}

#[tokio::test]
async fn original_root_interaction_rejects_wrong_digest_foreign_operation_and_decline_survives_reopen() {
    let fixture = Fixture::new(None).await;
    fixture.journal.verify_dispatch().await.unwrap();
    let root = fixture.owner.policy().root_task;
    let operation = crate::pi_fork::derived(crate::pi_fork::original_operation(root, root, "writeback-sdk", &crate::pi_fork::tests::work()).unwrap(), "selected-writeback", &crate::pi_fork::tests::work()).unwrap();
    let grants = Capabilities::new(fixture.owner.policy().grants.iter().map(str::to_owned).chain([
        "interaction:open".into(), "interaction:resolve".into(), "interaction:observe".into(),
    ]));
    let work = work();
    let authority = fixture.owner.conversation_authority_with_native_work(root, fixture.owner.policy().agent, &grants, &work).unwrap();
    let (_, issuer, scope) = &authority.0;
    let make_journal = || FilesystemExecutionJournal::new(fixture.stream.clone(), fixture.filesystem.clone(),
        &fixture.root_request.binding.scratch.volume, &issuer.verifier(&mut None).unwrap(), &acyclic_harness::core::SchemaRegistry::new(), scope, fixture.owner.policy().maximum_payload_bytes, None).unwrap();
    let approval = InteractionId::from_bytes([211; 16]);
    let digest = *blake3::hash(b"exact-original-root-selected-writeback-input").as_bytes();
    let journal = make_journal();
    journal.open_interaction(approval, Interaction::approval("Exact Root writeback".into(), operation, digest).unwrap()).await.unwrap();
    let response_grants = Capabilities::new(["interaction:resolve".into(), format!("interaction:respond:{approval}")]);
    let foreign = acyclic_harness::core::AuthorityIssuer::new(issuer.id(), [213; 32], issuer.verifier(&mut None).unwrap().audience().clone()).issue("foreign-root", &response_grants, Some(fixture.owner.policy().agent), None, &mut None).expect("scope proof");
    assert!(journal.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &foreign).await.is_err());
    let other = fixture.owner.conversation_authority_with_native_work(TaskId::from_bytes([213; 16]),
        fixture.owner.policy().agent, &response_grants, &work).unwrap();
    assert!(journal.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &other.0.2).await.is_err());
    let responder = fixture.owner.conversation_authority_with_native_work(root, fixture.owner.policy().agent, &response_grants, &work).unwrap();
    journal.resolve_interaction(approval, InteractionResponse::Approval { approved: false, reason: None }, &responder.0.2).await.unwrap();
    drop(journal); // Host ACK was not retained by the client.
    let reopened = make_journal();
    assert!(ToolApprovalVerifier::verify(&reopened, approval, operation, digest).await.is_err());
    reopened.resolve_interaction(approval, InteractionResponse::Approval { approved: false, reason: None }, &responder.0.2).await.unwrap();
    assert!(reopened.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &responder.0.2).await.is_err());
    let approved = InteractionId::from_bytes([212; 16]);
    reopened.open_interaction(approved, Interaction::approval("Another exact original Root writeback".into(), operation, digest).unwrap()).await.unwrap();
    let responder = fixture.owner.conversation_authority_with_native_work(root, fixture.owner.policy().agent,
        &Capabilities::new(["interaction:resolve".into(), format!("interaction:respond:{approved}")]), &work).unwrap();
    reopened.resolve_interaction(approved, InteractionResponse::Approval { approved: true, reason: None }, &responder.0.2).await.unwrap();
    drop(reopened);
    let reopened = make_journal();
    ToolApprovalVerifier::verify(&reopened, approved, operation, digest).await.unwrap();
    assert!(reopened.resolve_interaction(InteractionId::from_bytes([215; 16]),
        InteractionResponse::Approval { approved: true, reason: None }, &responder.0.2).await.is_err());
    assert!(reopened.open_interaction(approved, Interaction::approval("Changed reused request".into(), operation, [213; 32]).unwrap()).await.is_err());
    assert!(ToolApprovalVerifier::verify(&reopened, approved, operation, [213; 32]).await.is_err());
    assert!(ToolApprovalVerifier::verify(&reopened, approved, OperationId::from_bytes([214; 16]), digest).await.is_err());
    fixture.journal.verify_dispatch().await.unwrap();
}

#[tokio::test]
async fn actual_admitted_child_cannot_read_root_only_operator_proof_or_upgrade_its_caller() {
    use acyclic_harness::{filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
        distributed::Worker, scheduler::{LeaseFence, ResourceSnapshot}};
    let fixture = Fixture::new(None).await;
    let work = work();
    let plan = fixture.plan(&work).await;
    let coordinator = crate::pi_fork::PiForkCoordinator::new(
        fixture.runtime.clone(), fixture.filesystem.clone(), fixture.stream.clone(),
        &fixture.resources, fixture.owner.clone(), &work).unwrap();
    let child = fixture.advance_with(&coordinator, &fixture.context, &fixture.journal, &fixture.fence,
        &plan, fixture.root_handle.workspace.clone(), work.remaining().unwrap(), work.cancellation().clone())
        .await.unwrap().unwrap();
    let worker = Worker { id: "operator-private-child".into(), available: ResourceSnapshot::default(),
        labels: Default::default() };
    let attempt_custody;
    let attempt;
    (attempt, attempt_custody) = (async {
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
        let value = fixture.runtime.runtime.run_operation(&worker, child.binding.task_id, &fixture.runtime.runtime.commands(), 1, Some(&io)).await?;
        let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
            return Err(acyclic_harness::Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "worker run requires original native custody" });
        };
        Ok::<_, acyclic_harness::Error>((value, custody.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner)))
    }).await.unwrap();
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt else {
        panic!("actual admitted child command lease");
    };
    let child_fence = LeaseFence::from(&lease.reservation);
    let child_context = fixture.runtime.runtime.task_context_with_native_work(
        child.binding.task_id, &child_fence, &work).await.unwrap();
    let session = &fixture.root_request.binding.native_session_id;
    let request_custody;
    let request;
    (request, request_custody) = build_control_request(&fixture.owner, std::env::current_dir().unwrap(),
        PiOperatorBody::Inspect { session: session.clone(), operation: "original-operator-inspection".into(),
            project_files: false, file_evidence: None }, &work).unwrap();
    let mut parsed_custody;
    let parsed_request;
    (parsed_request, parsed_custody) = PiOperatorRequest::from_value_with_native_work(&request.arguments, &work).unwrap();
    assert!(parsed_custody.shares_ledger(&request_custody));
    drop(request);
    drop(request_custody);
    let mut request = parsed_request;
    let original = crate::pi_fork::original_operation(fixture.owner.policy().root_task,
        fixture.owner.policy().root_task, request.body.operation(), &work).unwrap();
    let operation = crate::pi_fork::derived(original, "selected-writeback-publish", &work).unwrap();
    let private_custody;
    let private;
    (private, private_custody) = pi_root_provider::stage_pi_operator_input(
        &fixture.owner, &fixture.filesystem, operation, "operator-proof-isolation", &request, &work).await.unwrap();
    assert!(private.path().starts_with(crate::pi_root_provider::OPERATOR_INPUT_DIRECTORY));
    assert!(!child_context.context().scope().grants().contains(&fixture.owner.policy().journal.directory_read_capability(
        crate::pi_root_provider::OPERATOR_INPUT_DIRECTORY, &mut None).unwrap()));
    assert!(matches!(child_context.context().read_file(&private).await, Err(Error::Unauthorized(_))));
    let (caller, caller_custody) = pi_caller::issue(&fixture.owner, session, &child.binding, &work).unwrap();
    request.operator = caller.0;
    parsed_custody.absorb(caller_custody).unwrap();
    assert!(verify_request(&fixture.owner, session, &request, &work).is_err());
    drop(private);
    drop(private_custody);
    drop(request);
    drop(parsed_custody);
    drop(child_context);
    drop(child_fence);
    drop(lease);
    drop(attempt_custody);
}

#[tokio::test]
async fn passive_original_operation_pending_encodes_own_rpc_identity_without_task_graph() {
    let fixture = Fixture::new(None).await;
    let session = "original-passive-rpc-session";
    let native_session = &fixture.root_request.binding.native_session_id;
    let caller = fixture.owner.policy().root_task;
    let root = fixture.root_request.binding.project_volumes.keys().next().unwrap();
    let paths = vec!["observed.txt".to_owned()];
    let original_work = work();
    let custody;
    let value;
    (value, custody) = crate::pi_operator_host::passive_status(&fixture.runtime, &fixture.filesystem, &fixture.stream,
        &fixture.resources, &fixture.owner, session, native_session, caller, "original-sdk-pending-writeback",
        fixture.commit.child.task_id, root, &paths, &original_work).await.unwrap();
    assert_eq!(value["session"], session);
    assert_eq!(value["native_session"], serde_json::Value::String(native_session.clone()));
    assert_eq!(value["current_task"], serde_json::to_value(caller).unwrap());
    assert_eq!(value["operation"], "original-sdk-pending-writeback");
    assert_eq!(value["status"], "pending");
    assert!(value.get("tasks").is_none());
    assert!(value.get("operator").is_none());
    assert!(custody.work().shares_ledger(&original_work));
    fixture.journal.verify_dispatch().await.unwrap();
    drop(value);
    drop(custody);
}
