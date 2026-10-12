use super::*;
use crate::{AgentId, Capabilities, core::{AggregateKind, Authority}, conversation::{VolumeClass, VolumeOwner}};
use wasm_bindgen_test::wasm_bindgen_test;

fn field(value: &JsValue, name: &str) -> JsValue { js_sys::Reflect::get(value, &name.into()).unwrap() }

fn wire(value: &Value) -> std::result::Result<JsValue, JsValue> {
    js_sys::JSON::parse(&serde_json::to_string(value).map_err(|error| JsValue::from_str(&error.to_string()))?)
}

#[wasm_bindgen_test]
async fn public_browser_selected_inspection_and_custom_loop_use_real_owner_and_providers() -> std::result::Result<(), JsValue> {
    let id = OperationId::new().to_string();
    let owner = WasmReducer::new(to_js(&Authority { kind: AggregateKind::Task, id: id.clone() })?,
        "headless-browser".into(), vec![41; 32], to_js(&Vec::<Value>::new())?)?;
    let agent = AgentId::from_bytes([42; 16]);
    let provider = ProviderRef::new("headless-browser", "filesystem", "2").map_err(js_error)?;
    let volume = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).map_err(js_error)?;
    let scope = owner.issuer.root_for_agent(agent, "owner", Capabilities::new([
        "operation:declare".into(), "operation:observe".into(), "operation:cancel".into(),
        "task:spawn:test.headless@1".into(), "tool:call:test.headless".into(), "model:generate".into(),
        volume.capability(VolumeOperation::Read).map_err(js_error)?,
        volume.capability(VolumeOperation::Write).map_err(js_error)?,
    ]));
    let mut registry = WasmTaskRegistry::new();
    registry.register_machine(wire(&serde_json::json!({
        "name":"test.headless", "version":"1", "digest":vec![43;32],
        "state_schema":{"type":"object"}, "input_schema":{"type":"object"}, "output_schema":{"type":"string"}, "requirements":[],
    }))?, Function::new_with_args("input", "return input;"),
        Function::new_with_args("state,input", "return {state,commands:[],status:{kind:'suspended'}};"))?;
    let execute = Function::new_no_args("return {value:'retained tool'};");
    registry.register_tool(to_js(&ToolDefinition {
        name:"test.headless".into(), revision:"1".into(), description:"Fixture tool".into(),
        input_schema:serde_json::json!({"type":"null"}), output_schema:serde_json::json!({"type":"string"}),
        projection_schema:crate::tool::json_projection_schema(serde_json::json!({"type":"string"})),
    })?, execute, Function::new_no_args("return null;"), Function::new_with_args("call,result", "return {kind:'json',value:result.value};"))?;
    let options = serde_json::json!({
        "filesystem_database":format!("headless-fs-{id}"), "stream_database":format!("headless-stream-{id}"),
        "maximum_object_bytes":1048576, "maximum_payload_bytes":65536, "filesystem_provider":provider, "volume":volume,
        "stream_limits":{"commands":10000,"journal_bytes":16777216,"paths":1000,"path_bytes":262144,
            "records":10000,"payload_bytes":16777216,"commits":10000,"idempotency_results":10000},
        "limits":{"file_bytes":1048576,"path_bytes":4096,"attachments":32,"render_bytes":1048576,
            "model_steps":16,"model_events_per_step":64,"tool_calls_per_step":16,"context_messages":128},
        "run_limits":{"concurrency":1,"max_steps":16,"deadline_epoch_ms":null},
        "session_limits":{"active_tasks":4,"total_tasks":8,"depth":4,"model_steps":16},"concurrency":1,
    });
    let mut runtime = WasmTaskRuntime::open(wire(&options)?, &owner, &registry, to_js(&scope)?).await?;
    runtime.initialize_volume().await?;
    let write = ContentGrant::verify(&runtime.verifier, &runtime.scope, &volume, VolumeOperation::Write).map_err(js_error)?;
    let file = runtime.filesystem.put_content(&volume, &write, "selected/read.txt", b"selected bytes", "text/plain", "read.txt",
        65536, &crate::IdempotencyKey::new("headless-file").map_err(js_error)?).await.map_err(js_error)?;
    let generation = runtime.filesystem.file_generation(&file).map_err(js_error)?;
    let operation = OperationId::new();
    let admitted = runtime.admit(operation.to_string(), "test.headless".into(), "1".into(), to_js(&serde_json::json!({}))?, None).await?;
    let task = field(&admitted, "task_id").as_string().unwrap();
    let selected = serde_json::json!({"caller":task,"source":task,"volume":volume,"granted_prefix":"selected","path":"selected","generation":generation});
    let page = runtime.list_selected_directory(wire(&selected)?, None, 1).await?;
    assert_eq!(js_sys::Array::from(&field(&page,"entries")).length(), 1);
    let mut read = selected.clone();
    read["path"] = "selected/read.txt".into();
    let read_value = runtime.read_selected_path(wire(&read)?).await?;
    assert_eq!(js_sys::Uint8Array::new(&field(&read_value, "bytes")).to_vec(), b"selected bytes");
    let metadata = runtime.inspect_source(task.clone(), task.clone()).await?;
    assert_eq!(field(&metadata, "operation_id").as_string().as_deref(), Some(operation.to_string().as_str()));
    assert!(!runtime.diff_selected_files(task.clone(), task.clone(), super::super::super::to_js_admitted(&file)?, super::super::super::to_js_admitted(&file)?).await?);
    assert!(runtime.list_selected_directory(wire(&selected)?, None, 0).await.is_err());
    read["path"] = ".system/hidden".into();
    assert!(runtime.read_selected_path(wire(&read)?).await.is_err());
    let mut wrong = selected.clone();
    wrong["source"] = OperationId::new().to_string().into();
    assert!(runtime.list_selected_directory(wire(&wrong)?, None, 1).await.is_err());
    let page = runtime.children(task.clone(), None, None, 1).await?;
    assert_eq!(js_sys::Array::from(&field(&page,"entries")).length(), 0);
    assert!(runtime.children(task.clone(), None, None, 0).await.is_err());
    let generate: Function = Function::new_no_args("let calls=0; const f=()=>{calls++; let i=0; const events=[{kind:'tool_call',call_id:'exact-call',name:'test.headless',arguments:null},{kind:'completed',metadata:{}}]; return {next:()=>Promise.resolve(i<events.length?{done:false,value:events[i++]}:{done:true}),return:()=>({done:true})};}; f.calls=()=>calls; return f;").call0(&JsValue::UNDEFINED)?.unchecked_into();
    runtime.configure_model(to_js(&Model::new("fixture", "headless", "1", serde_json::json!({})).map_err(js_error)?)?,
        generate.clone(), Function::new_no_args("return null;"),
        Some(Function::new_no_args("return {context_tokens:131072,output_tokens:8192};")),
        Some(Function::new_with_args("bytes,request_digest", "return {request_digest,fixed_tokens:bytes.byteLength,message_tokens:JSON.parse(new TextDecoder().decode(bytes)).messages.map(()=>0)};")),
        to_js(&crate::context::CompactionPolicy::Disabled)?)?;
    let lease = match runtime.runtime.task_host().pull_work(&Worker { id:"headless".into(), available:Default::default(), labels:Default::default() }).await.map_err(js_error)? {
        WorkPull::Claimed(lease) => lease,
        _ => return Err(JsValue::from_str("headless lease unavailable")),
    };
    runtime.runtime.task_host().start_task(&lease, None).await.map_err(js_error)?;
    let execution = runtime.headless_execution(task, to_js(&crate::scheduler::LeaseFence::from(&lease.reservation))?, OperationId::new().to_string()).await?;
    let input = TurnInput { operation_id:OperationId::parse(&execution.operation_id()).map_err(js_error)?, input:crate::model::ModelContent::Text("headless".into()), selected_context:None, max_steps:1 };
    let first = execution.model_step(to_js(&input)?, 0, to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    let second = execution.model_step(to_js(&input)?, 0, to_js(&Vec::<crate::model::ModelMessage>::new())?).await?;
    assert_eq!(js_sys::Array::from(&first).length(), js_sys::Array::from(&second).length());
    let count: Function = field(&generate, "calls").unchecked_into();
    assert_eq!(count.call0(&JsValue::UNDEFINED)?.as_f64(), Some(1.0));
    let settled = execution.resolve_model_tools(to_js(&input)?, 0).await?;
    assert_eq!(field(&settled,"kind").as_string().as_deref(), Some("ready"));
    assert_eq!(js_sys::Array::from(&field(&settled,"value")).length(), 2);
    assert!(execution.model_step(to_js(&input)?, 1, to_js(&Vec::<crate::model::ModelMessage>::new())?).await.is_err());
    Ok(())
}
