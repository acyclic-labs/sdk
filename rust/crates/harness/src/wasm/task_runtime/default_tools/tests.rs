use super::*;
use crate::{AgentId, Capabilities, IdempotencyKey,
    conversation::{ConversationMessage, MessageKind, VolumeClass, VolumeOwner},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    executor::TurnInput,
    runtime::{TaskAdmissionRecord, task_id_for_admission},
    store::StreamAggregate,
};
use acyclic_fs::WorkBudget;
use wasm_bindgen_test::wasm_bindgen_test;

fn wire(value: &Value) -> std::result::Result<JsValue, JsValue> {
    js_sys::JSON::parse(&serde_json::to_string(value).map_err(|error| JsValue::from_str(&error.to_string()))?)
}
fn field(value: &JsValue, name: &str) -> JsValue { js_sys::Reflect::get(value, &name.into()).unwrap() }
fn describe(value: &JsValue) -> String {
    Function::new_with_args("value", "return JSON.stringify(value,(_,item)=>typeof item==='bigint'?`${item}n`:item);")
        .call1(&JsValue::UNDEFINED,value).unwrap().as_string().unwrap()
}
fn budget() -> WorkBudget {
    WorkBudget { authority_records_read: 32768, authority_records_appended: 32768,
        authority_bytes_read: 67108864, authority_bytes_written: 67108864,
        object_probes: 32768, backend_read_operations: 32768, backend_write_operations: 32768,
        durability_operations: 32768, page_reads: 32768, page_writes: 32768,
        object_bytes_read: 67108864, object_bytes_written: 67108864,
        bytes_hashed: 67108864, bytes_copied: 67108864, bytes_encoded: 67108864,
        source_bytes_read: 67108864, source_path_components: 32768, source_entries_visited: 32768,
        output_bytes: 67108864, items_examined: 32768, items_returned: 32768,
        allocation_operations: 32768, peak_allocation_bytes: 67108864, materializations: 32768 }
}
fn binding(authority: &Authority, agent: AgentId, grants: &Capabilities) -> Value {
    let issuer = AuthorityIssuer::new("browser-recursive-owner", [77; 32], authority.clone());
    serde_json::json!({"authority":authority,"issuer":"browser-recursive-owner","key":vec![77;32],
        "scope":issuer.root_for_agent(agent,"configured",grants.clone())})
}
fn child(template: &TaskAdmissionRecord, operation: OperationId, parent: TaskId, msg: &str, grants: Capabilities) -> TaskAdmissionRecord {
    let mut record = template.clone();
    record.operation_id = operation; record.parent = Some(parent);
    record.input = serde_json::json!({"msg":msg}); record.grants = grants;
    record.resource_proofs.clear(); record
}
async fn claim(runtime: &WasmTaskRuntime, operation: OperationId, worker: &Worker) -> std::result::Result<WorkLease, JsValue> {
    let lease = match runtime.runtime.task_host().pull_operation(worker, operation).await.map_err(js_error)? {
        WorkPull::Claimed(lease) => lease,
        _ => return Err(JsValue::from_str("original browser caller lease unavailable")),
    };
    runtime.runtime.task_host().start_task(&lease, None).await.map_err(js_error)?;
    Ok(lease)
}
async fn settle_tools(runtime: &WasmTaskRuntime, execution: &super::super::headless::WasmHeadlessExecution,
    task: TaskId, input: &TurnInput, worker: &Worker) -> std::result::Result<JsValue, JsValue> {
    for _ in 0..24 {
        let progress = execution.resolve_model_tools(to_js(input)?, 0).await?;
        if field(&progress,"kind").as_string().as_deref() == Some("ready") { return Ok(progress); }
        assert_eq!(field(&progress,"kind").as_string().as_deref(), Some("pending"));
        let children = runtime.runtime.children(task, None, None, 32).await.map_err(js_error)?;
        for child in children.entries {
            let admission = runtime.runtime.task_host().observe_admission(child.task_id).await.map_err(js_error)?;
            if admission.machine.name == "acyclic.fork_preparation" && runtime.runtime.task_host().outcome(child.task_id).await.map_err(js_error)?.is_none() {
                let work = runtime.run_operation(to_js(worker)?, child.task_id.to_string(), 16).await?;
                if !work.is_null() {
                    assert_ne!(field(&work,"kind").as_string().as_deref(), Some("unresolved"), "canonical browser preparation failed: {:?}", work);
                }
            }
        }
    }
    Err(JsValue::from_str("ordinary browser preparation did not settle within fixture allowance"))
}
async fn append(runtime: &WasmTaskRuntime, authority: &Authority, agent: AgentId, scope: crate::core::Scope,
    volume: &VolumeRef, marker: &str) -> std::result::Result<FileRef, JsValue> {
    let issuer = AuthorityIssuer::new("browser-recursive-owner", [77;32], authority.clone());
    let writer = ContentGrant::verify(&issuer.verifier(), &scope, volume, VolumeOperation::Write).map_err(js_error)?;
    let file = runtime.filesystem.put_content(volume, &writer, "transcript/own.txt", marker.as_bytes(), "text/plain", "own.txt",
        65536, &IdempotencyKey::new(format!("transcript-{agent}")).map_err(js_error)?).await.map_err(js_error)?;
    let mut aggregate = StreamAggregate::open(&runtime.stream, authority.clone(), issuer.verifier(), SchemaRegistry::default()).await.map_err(js_error)?
        .with_content_verifier(Arc::new(crate::filesystem::FilesystemContentVerifier::new(runtime.filesystem.clone(), issuer.verifier(), scope.clone(), 65536).map_err(js_error)?));
    let sequence = aggregate.reducer().conversation().unwrap().logical_revision() + 1;
    aggregate.execute(Command { operation_id:OperationId::new(), idempotency_key:IdempotencyKey::new(format!("append-{agent}")).map_err(js_error)?,
        expected_revision:aggregate.reducer().revision(), scope, causal_parent:None,
        action:Action::AppendConversationMessage { message:Box::new(ConversationMessage { id:uuid::Uuid::new_v4(), sequence,
            kind:MessageKind::User, content:file.clone(), attachments:Default::default(), reply_to:None, tool_call_id:None, extensions:Default::default() }) }
    }).await.map_err(js_error)?;
    Ok(file)
}
fn configure_events(runtime: &mut WasmTaskRuntime, events: &Value) -> std::result::Result<(), JsValue> {
    let generate = Function::new_with_args("bytes,dispatch,signal", &format!("const events={events}; let i=0; return {{next:()=>Promise.resolve(i<events.length?{{done:false,value:events[i++]}}:{{done:true}}),return:()=>({{done:true}})}};"));
    runtime.configure_model(to_js(&Model::new("fixture","portable-browser","1",serde_json::json!({})).map_err(js_error)?)?, generate, Function::new_no_args("return null;"),
        Some(Function::new_no_args("return {context_tokens:131072,output_tokens:8192};")), Some(Function::new_with_args("bytes,request_digest","return {request_digest,fixed_tokens:bytes.byteLength,message_tokens:JSON.parse(new TextDecoder().decode(bytes)).messages.map(()=>0)};")),
        to_js(&crate::context::CompactionPolicy::Disabled)?)
}

#[wasm_bindgen_test]
async fn browser_defaults_root_two_children_grandchild_and_foreign_project_use_actual_owner() -> std::result::Result<(), JsValue> {
    let id = OperationId::new().to_string();
    let owner = WasmReducer::new(to_js(&Authority { kind:AggregateKind::Task, id:id.clone() })?,
        "browser-recursive-runtime".into(), vec![76;32], to_js(&Vec::<Value>::new())?)?;
    let agents = [AgentId::new(), AgentId::new(), AgentId::new(), AgentId::new()];
    let operations = [OperationId::new(), OperationId::new(), OperationId::new(), OperationId::new()];
    let tasks = operations.map(task_id_for_admission);
    let authorities = ["root","left","right","grandchild"].map(|label| Authority { kind:AggregateKind::Conversation, id:format!("{id}-{label}") });
    let provider = ProviderRef::new("browser-recursive", "filesystem", "2").map_err(js_error)?;
    let stream_provider = ProviderRef::new("browser-recursive", "stream", "1").map_err(js_error)?;
    let projects = ["root","left","right","grandchild"].map(|label| VolumeRef::new(provider.clone(),format!("{id}-{label}-project"),VolumeClass::Project,VolumeOwner::Project(id.clone())).unwrap());
    let private = [0,1,2,3].map(|index| VolumeRef::new(provider.clone(),format!("{id}-{index}-scratch"),VolumeClass::AgentPrivate,VolumeOwner::Agent(agents[index])).unwrap());
    let bootstrap_agent = AgentId::new();
    let bootstrap_operation = OperationId::new();
    let bootstrap_task = task_id_for_admission(bootstrap_operation);
    let bootstrap_authority = Authority { kind:AggregateKind::Conversation, id:format!("{id}-bootstrap") };
    let service_volume = VolumeRef::new(provider.clone(),format!("{id}-service-journal"),VolumeClass::AgentPrivate,VolumeOwner::Agent(bootstrap_agent)).map_err(js_error)?;
    let bootstrap_project = VolumeRef::new(provider.clone(),format!("{id}-bootstrap-project"),VolumeClass::Project,VolumeOwner::Project(id.clone())).map_err(js_error)?;
    let mut common = vec!["host", "operation:declare", "operation:observe", "operation:cancel", "operation:wake", "model:generate",
        "fork:publish", "project:merge", "task:spawn:test.recursive@1", "task:spawn:acyclic.fork_preparation@1",
        "tool:call:acyclic.fork", "tool:call:acyclic.merge", "tool:call:acyclic.read_file", "tool:call:acyclic.write_file", "tool:call:acyclic.edit_file"]
        .into_iter().map(str::to_owned).collect::<Vec<_>>();
    for project in &projects { common.push(project.capability(VolumeOperation::Read).map_err(js_error)?); }
    let common = Capabilities::new(common);
    let scopes = [0,1,2,3].map(|index| {
        let mut grants = common.iter().map(str::to_owned).collect::<Vec<_>>();
        grants.push("conversation:bind".into()); grants.push("conversation:append".into());
        for volume in [&projects[index], &private[index]] {
            grants.push(volume.capability(VolumeOperation::Read).unwrap()); grants.push(volume.capability(VolumeOperation::Write).unwrap());
        }
        Capabilities::new(grants)
    });
    let mut bootstrap_grants = common.iter().map(str::to_owned).collect::<Vec<_>>();
    bootstrap_grants.extend(["conversation:bind".into(),"conversation:append".into()]);
    for volume in [&service_volume, &bootstrap_project] {
        bootstrap_grants.push(volume.capability(VolumeOperation::Read).map_err(js_error)?);
        bootstrap_grants.push(volume.capability(VolumeOperation::Write).map_err(js_error)?);
    }
    let bootstrap_grants = Capabilities::new(bootstrap_grants);
    let runtime_scope = owner.issuer.root_for_agent(bootstrap_agent, "runtime", bootstrap_grants.clone());
    let mut registry = WasmTaskRegistry::new();
    registry.register_fork_preparation()?;
    registry.register_machine(wire(&serde_json::json!({"name":"test.recursive","version":"1","digest":vec![79;32],
        "state_schema":{"type":"object"},"input_schema":{"type":"object"},"output_schema":{"type":"null"},"requirements":[]}))?,
        Function::new_with_args("input","return input;"), Function::new_with_args("state,input","return {state,commands:[],status:{kind:'suspended'}};"))?;
    let mut options = serde_json::json!({"filesystem_database":format!("recursive-fs-{id}"),"stream_database":format!("recursive-stream-{id}"),
        "maximum_object_bytes":16777216,"maximum_payload_bytes":65536,"filesystem_provider":provider,"volume":service_volume,
        "stream_limits":{"commands":100000,"journal_bytes":67108864,"paths":4096,"path_bytes":1048576,"records":100000,"payload_bytes":67108864,"commits":100000,"idempotency_results":100000},
        "limits":{"file_bytes":65536,"path_bytes":4096,"attachments":32,"render_bytes":65536,"model_steps":16,"model_events_per_step":64,"tool_calls_per_step":16,"context_messages":128},
        "run_limits":{"concurrency":8,"max_steps":64,"deadline_epoch_ms":null},"session_limits":{"active_tasks":8,"total_tasks":32,"depth":8,"model_steps":64},"concurrency":8});
    let runtime = WasmTaskRuntime::open(wire(&options)?, &owner, &registry, to_js(&runtime_scope)?).await?;
    for name in ["acyclic.read_file", "acyclic.write_file", "acyclic.edit_file", "acyclic.fork", "acyclic.merge"] {
        assert!(runtime.runtime.tool_registry().get(name).is_some());
    }
    assert!(runtime.filesystem.resolve(&crate::filesystem::workspace_ref(
        service_volume.provider().clone(), &service_volume.storage_name().map_err(js_error)?,
    ).map_err(js_error)?).await.is_err());
    runtime.initialize_volume().await?;
    runtime.filesystem.create_volume(&bootstrap_project).await.map_err(js_error)?;
    let bootstrap_issuer = AuthorityIssuer::new("browser-recursive-owner",[77;32],bootstrap_authority.clone());
    let bootstrap_scope = bootstrap_issuer.root_for_agent(bootstrap_agent,"configured",bootstrap_grants.clone());
    let mut aggregate = StreamAggregate::open(&runtime.stream,bootstrap_authority.clone(),bootstrap_issuer.verifier(),SchemaRegistry::default()).await.map_err(js_error)?;
    aggregate.execute(Command { operation_id:OperationId::new(),idempotency_key:IdempotencyKey::new("bind-bootstrap").map_err(js_error)?,expected_revision:0,
        scope:bootstrap_scope,causal_parent:None,action:Action::BindConversation {agent:bootstrap_agent} }).await.map_err(js_error)?;
    let admitted = runtime.admit(bootstrap_operation.to_string(),"test.recursive".into(),"1".into(),wire(&serde_json::json!({"msg":"bootstrap-work"}))?,None).await?;
    assert_eq!(field(&admitted,"kind").as_string().as_deref(),Some("accepted"));
    let template = runtime.runtime.task_host().observe_admission(bootstrap_task).await.map_err(js_error)?;
    let root_allocation = child(&template,operations[0],bootstrap_task,"root-work",common.clone());
    let children = [child(&template,operations[1],tasks[0],"left-work",common.clone()), child(&template,operations[2],tasks[0],"right-work",common.clone()),
        child(&template,operations[3],tasks[1],"grandchild-work",common.clone())];
    let mut callers = (0..4).map(|index|serde_json::json!({"task":tasks[index],"owner":binding(&authorities[index],agents[index],&scopes[index]),"project":projects[index],"private_volume":private[index]})).collect::<Vec<_>>();
    callers.push(serde_json::json!({"task":bootstrap_task,"owner":binding(&bootstrap_authority,bootstrap_agent,&bootstrap_grants),"project":bootstrap_project,"private_volume":service_volume}));
    let mut forks = (1..4).map(|index|serde_json::json!({"caller":if index==3 {tasks[1]} else {tasks[0]},"child":binding(&authorities[index],agents[index],&scopes[index]),"project":projects[index],"private_volume":private[index],
        "admission":children[index-1].canonical_value(),"child_message_pointer":"/msg","child_message_file_pointer":null,"input_files":[],
        "limits":{"resources":64,"work":16777216,"bytes":16777216}})).collect::<Vec<_>>();
    forks.push(serde_json::json!({"caller":bootstrap_task,"child":binding(&authorities[0],agents[0],&scopes[0]),"project":projects[0],"private_volume":private[0],
        "admission":root_allocation.canonical_value(),"child_message_pointer":"/msg","child_message_file_pointer":null,"input_files":[],
        "limits":{"resources":64,"work":16777216,"bytes":16777216}}));
    options["recursive_tools"] = serde_json::json!({"callers":callers,"forks":forks,"merges":[],"stream_provider":stream_provider,"project_policy_graph_root":bootstrap_task,
        "history_limits":{"maximum_events":256,"maximum_bytes":4194304},"inherited_messages":64,"inherited_references":64,"content_budget":budget()});
    drop(runtime);
    let mut runtime = WasmTaskRuntime::open(wire(&options)?, &owner, &registry, to_js(&runtime_scope)?).await?;
    let worker = Worker { id:"browser-recursive-worker".into(),available:Default::default(),labels:Default::default() };
    configure_events(&mut runtime,&serde_json::json!([
        {"kind":"tool_call","call_id":"root-allocate","name":"acyclic.fork","arguments":{"id":agents[0],"msg":"root-work"}},
        {"kind":"completed","metadata":{}}]))?;
    let bootstrap_lease = claim(&runtime,bootstrap_operation,&worker).await?;
    let bootstrap_execution = runtime.headless_execution(bootstrap_task.to_string(),to_js(&crate::scheduler::LeaseFence::from(&bootstrap_lease.reservation))?,OperationId::new().to_string()).await?;
    let bootstrap_input = TurnInput {operation_id:OperationId::parse(&bootstrap_execution.operation_id()).map_err(js_error)?,input:crate::model::ModelContent::Text("bootstrap-work".into()),selected_context:None,max_steps:1};
    bootstrap_execution.model_step(to_js(&bootstrap_input)?,0,to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    settle_tools(&runtime,&bootstrap_execution,bootstrap_task,&bootstrap_input,&worker).await?;
    let root_admission = runtime.runtime.task_host().observe_admission(tasks[0]).await.map_err(js_error)?;
    assert_eq!(root_admission.parent,Some(bootstrap_task));
    assert!(!root_admission.resource_proofs.is_empty());
    assert!(!root_admission.grants.contains(&service_volume.capability(VolumeOperation::Read).map_err(js_error)?));
    assert!(!root_admission.grants.contains(&service_volume.capability(VolumeOperation::Write).map_err(js_error)?));
    let root_scope = AuthorityIssuer::new("browser-recursive-owner",[77;32],authorities[0].clone()).root_for_agent(agents[0],"configured",scopes[0].clone());
    let root_file = append(&runtime,&authorities[0],agents[0],root_scope,&private[0],"ROOT ORIGINAL").await?;
    let notice = ConversationMessage {id:uuid::Uuid::new_v4(),sequence:3,kind:MessageKind::User,content:root_file.clone(),attachments:Default::default(),reply_to:None,tool_call_id:None,extensions:Default::default()};
    options["recursive_tools"]["merges"] = serde_json::json!([{"caller":tasks[1],"source":tasks[2],"notice":notice}]);
    drop(runtime);
    let mut runtime = WasmTaskRuntime::open(wire(&options)?, &owner, &registry, to_js(&runtime_scope)?).await?;
    let root_events = serde_json::json!([
        {"kind":"tool_call","call_id":"root-read","name":"acyclic.read_file","arguments":{"file":root_file}},
        {"kind":"tool_call","call_id":"root-write","name":"acyclic.write_file","arguments":{"path":"scratch/public-write.txt","text":"written through original owner","media_type":"text/plain","display_name":"public-write.txt"}},
        {"kind":"tool_call","call_id":"left","name":"acyclic.fork","arguments":{"id":agents[1],"msg":"left-work"}},
        {"kind":"tool_call","call_id":"right","name":"acyclic.fork","arguments":{"id":agents[2],"msg":"right-work"}},
        {"kind":"completed","metadata":{}}]);
    let left_events = serde_json::json!([
        {"kind":"tool_call","call_id":"grandchild","name":"acyclic.fork","arguments":{"id":agents[3],"msg":"grandchild-work","source":tasks[2]}},
        {"kind":"tool_call","call_id":"merge-right","name":"acyclic.merge","arguments":{"source":tasks[2]}},
        {"kind":"completed","metadata":{}}]);
    configure_events(&mut runtime, &root_events)?;
    let root_lease = claim(&runtime,operations[0],&worker).await?;
    let execution = runtime.headless_execution(tasks[0].to_string(),to_js(&crate::scheduler::LeaseFence::from(&root_lease.reservation))?,OperationId::new().to_string()).await?;
    let root_input = TurnInput {operation_id:OperationId::parse(&execution.operation_id()).map_err(js_error)?,input:crate::model::ModelContent::Text("root-work".into()),selected_context:None,max_steps:1};
    execution.model_step(to_js(&root_input)?,0,to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    let root_results = settle_tools(&runtime,&execution,tasks[0],&root_input,&worker).await?;
    assert!(describe(&root_results).contains("ROOT ORIGINAL"));
    let written = runtime.filesystem.read(&crate::filesystem::workspace_ref(provider.clone(),&private[0].storage_name().map_err(js_error)?).map_err(js_error)?,None,"/scratch/public-write.txt",65536).await.map_err(js_error)?;
    assert_eq!(written,b"written through original owner");
    let left_source = runtime.source_private_volume(tasks[0].to_string(),tasks[1].to_string()).await?;
    assert_eq!(field(&field(&left_source,"volume"),"id").as_string().as_deref(),Some(private[1].id()));
    let left_workspace = crate::filesystem::workspace_ref(provider.clone(),&private[1].storage_name().map_err(js_error)?).map_err(js_error)?;
    assert!(runtime.filesystem.read(&left_workspace,None,"/scratch/public-write.txt",65536).await.is_err());
    assert!(runtime.filesystem.read(&left_workspace,None,"/transcript/own.txt",65536).await.is_err());
    let root_issuer = AuthorityIssuer::new("browser-recursive-owner",[77;32],authorities[0].clone());
    let root_published = StreamAggregate::open(&runtime.stream,authorities[0].clone(),root_issuer.verifier(),SchemaRegistry::default()).await.map_err(js_error)?;
    let left_seed = root_published.reducer().fork(&authorities[1]).unwrap();
    assert!(left_seed.reference_grants.iter().any(|grant| grant.reader==agents[1] && grant.file==root_file));
    let left_scope = AuthorityIssuer::new("browser-recursive-owner",[77;32],authorities[1].clone()).root_for_agent(agents[1],"configured",scopes[1].clone());
    let left_file = append(&runtime,&authorities[1],agents[1],left_scope,&private[1],"LEFT PRIVATE ORIGINAL").await?;
    let right_scope = AuthorityIssuer::new("browser-recursive-owner",[77;32],authorities[2].clone()).root_for_agent(agents[2],"configured",scopes[2].clone());
    append(&runtime,&authorities[2],agents[2],right_scope,&private[2],"RIGHT FOREIGN TRANSCRIPT MUST NOT IMPORT").await?;
    let left_lease = claim(&runtime,operations[1],&worker).await?;
    let edit_events = serde_json::json!([
        {"kind":"tool_call","call_id":"left-edit","name":"acyclic.edit_file","arguments":{"file":left_file,"old_text":"ORIGINAL","new_text":"EDITED"}},
        {"kind":"completed","metadata":{}}]);
    configure_events(&mut runtime, &edit_events)?;
    let edit_execution = runtime.headless_execution(tasks[1].to_string(),to_js(&crate::scheduler::LeaseFence::from(&left_lease.reservation))?,OperationId::new().to_string()).await?;
    let edit_input = TurnInput {operation_id:OperationId::parse(&edit_execution.operation_id()).map_err(js_error)?,input:crate::model::ModelContent::Text("left-edit-work".into()),selected_context:None,max_steps:1};
    edit_execution.model_step(to_js(&edit_input)?,0,to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    settle_tools(&runtime,&edit_execution,tasks[1],&edit_input,&worker).await?;
    let edited = runtime.filesystem.read(&crate::filesystem::workspace_ref(provider.clone(),&private[1].storage_name().map_err(js_error)?).map_err(js_error)?,None,"/transcript/own.txt",65536).await.map_err(js_error)?;
    assert_eq!(edited,b"LEFT PRIVATE EDITED");
    configure_events(&mut runtime, &left_events)?;
    let execution = runtime.headless_execution(tasks[1].to_string(),to_js(&crate::scheduler::LeaseFence::from(&left_lease.reservation))?,OperationId::new().to_string()).await?;
    let left_input = TurnInput {operation_id:OperationId::parse(&execution.operation_id()).map_err(js_error)?,input:crate::model::ModelContent::Text("left-work".into()),selected_context:None,max_steps:1};
    execution.model_step(to_js(&left_input)?,0,to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    let foreign_results = settle_tools(&runtime,&execution,tasks[1],&left_input,&worker).await?;
    assert!(describe(&foreign_results).contains("no_changes"), "selected unchanged project import was not actually observed");
    let grand_source = runtime.source_private_volume(tasks[1].to_string(),tasks[3].to_string()).await?;
    assert_eq!(field(&field(&grand_source,"volume"),"id").as_string().as_deref(),Some(private[3].id()));
    let actual = runtime.runtime.inspect_source(tasks[1],tasks[3]).await.map_err(js_error)?;
    assert_eq!(actual.agent().map_err(js_error)?,agents[3]);
    assert_ne!(actual.agent().map_err(js_error)?,agents[0]);
    let left_issuer = AuthorityIssuer::new("browser-recursive-owner",[77;32],authorities[1].clone());
    let published = StreamAggregate::open(&runtime.stream,authorities[1].clone(),left_issuer.verifier(),SchemaRegistry::default()).await.map_err(js_error)?;
    let seed = published.reducer().fork(&authorities[3]).unwrap();
    assert_eq!(seed.parent,authorities[1]);
    assert_eq!(seed.child_agent,agents[3]);
    assert!(seed.resources.iter().any(|capture| matches!(&capture.source,crate::fork::ResourceRevision::Project{volume,..} if volume==&projects[2])));
    assert!(!seed.reference_grants.iter().any(|grant| grant.file.volume()==&private[2]));
    assert_eq!(seed.inherited_through_sequence,0);
    assert!(seed.inherited_context.is_empty());
    assert!(seed.reference_grants.is_empty());
    Ok(())
}
