//! Actual JS callback boundary checks for portable provider accounting.

use super::*;
use crate::{
    context::{ModelContextCapacity, ModelTokenCount},
    conversation::Limits,
    model::{
        ModelContent, ModelMessage, ModelProvider, ModelRequest, ModelRole, PreparedModelRequest,
    },
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn provider() -> HostModel {
    HostModel {
        generate: Function::new_no_args("throw new Error('accounting invoked model generation')"),
        reconcile: Function::new_no_args("throw new Error('accounting invoked reconciliation')"),
        capacity: None,
        count_tokens: None,
        content: None,
    }
}

fn prepared(revision: &str) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("accounting".into(), "model".into(), revision.into())?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("canonical input".into()),
            }],
            tools: Vec::new(),
            sampling: Default::default(),
        },
        Limits::default(),
        None,
    )
}

#[wasm_bindgen_test]
async fn accounting_callbacks_bind_exact_selection_bytes_and_request() -> Result<()> {
    let request = prepared("selected")?;
    let mut provider = provider();
    provider.capacity = Some(Function::new_with_args(
        "model",
        "if (model.revision !== 'selected') throw new Error('wrong selection'); return {context_tokens: 32, output_tokens: 8};",
    ));
    assert_eq!(
        provider.context_capacity(&request.request().model)?,
        ModelContextCapacity {
            context_tokens: 32,
            output_tokens: Some(8),
            output_shares_context: false,
        }
    );
    provider.count_tokens = Some(Function::new_with_args(
        "bytes,digest",
        "const request = JSON.parse(new TextDecoder().decode(bytes)); if(request.model.revision !== 'selected' || request.messages[0].content !== 'canonical input') throw new Error('wrong canonical request'); return (async function*(){ yield {kind:'counted',count:{request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [3]}}; })();",
    ));
    let count = counted(&provider, &request).await?;
    assert_eq!(count.validate(&request, &mut None)?, 5);
    assert!(count.validate(&prepared("changed")?, &mut None).is_err());
    Ok(())
}

#[wasm_bindgen_test]
async fn asynchronous_count_callbacks_preserve_verified_totals() -> Result<()> {
    let request = prepared("selected")?;
    let mut provider = provider();
    provider.count_tokens = Some(Function::new_with_args(
        "bytes,digest",
        "return (async function*(){ const count = await Promise.resolve({request_digest:digest, input_tokens:512n, fixed_tokens:1, message_tokens:[1]}); yield {kind:'counted',count}; })();",
    ));
    let count = counted(&provider, &request).await?;
    assert_eq!(count.validate(&request, &mut None)?, 512);
    assert_eq!(count.fixed_tokens + count.message_tokens[0], 2);
    provider.count_tokens = Some(Function::new_no_args(
        "return (async function*(){ throw new Error('private counter detail'); })();",
    ));
    let error = counted(&provider, &request).await.unwrap_err();
    assert!(!error.to_string().contains("private counter detail"));
    Ok(())
}

#[wasm_bindgen_test]
async fn accounting_rejects_unbound_malformed_results() -> Result<()> {
    let request = prepared("selected")?;
    let mut provider = provider();
    for response in [
        "return {request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [3]};",
        "return (async function*(){ yield {kind:'counted',count:{request_digest: new Array(32).fill(0), input_tokens: 5, fixed_tokens: 2, message_tokens: [3]}}; })();",
        "return (async function*(){ yield {kind:'counted',count:{request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: []}}; })();",
        "return (async function*(){ yield {kind:'counted',count:{request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [3, 4]}}; })();",
        "return (async function*(){ yield {kind:'counted',count:{request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [-1]}}; })();",
        "throw new Error('unavailable tokenizer');",
    ] {
        provider.count_tokens = Some(Function::new_with_args("bytes,digest", response));
        assert!(matches!(
            counted(&provider, &request).await,
            Err(Error::Invalid(_))
        ));
    }
    for response in [
        "return {context_tokens: 0, output_tokens: 1};",
        "return {context_tokens: 1, output_tokens: 2};",
        "return {context_tokens: 32, output_tokens: 8, guessed: true};",
        "return Promise.resolve({context_tokens: 32, output_tokens: 8});",
    ] {
        provider.capacity = Some(Function::new_no_args(response));
        assert!(provider.context_capacity(&request.request().model).is_err());
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn host_count_wait_is_visible_before_completion_and_owns_cancellation() -> Result<()> {
    use futures::{FutureExt as _, StreamExt as _};
    let state = Function::new_no_args(
        "const state = {aborted:false}; state.count = (bytes,digest,signal) => (async function*(){ signal.addEventListener('abort', () => {state.aborted=true}, {once:true}); yield {kind:'wait',state:'admission_started',cause:{kind:'credit'}}; await new Promise(() => {}); })(); return state;",
    ).call0(&JsValue::UNDEFINED).map_err(|_| Error::Invalid("count fixture failed".into()))?;
    let mut provider = provider();
    provider.count_tokens = Some(
        js_sys::Reflect::get(&state, &"count".into())
            .map_err(|_| Error::Invalid("count fixture callback missing".into()))?
            .dyn_into::<Function>()
            .map_err(|_| Error::Invalid("count fixture callback is invalid".into()))?,
    );
    let request = prepared("selected")?;
    let mut events = provider.count_tokens(&request, 0);
    assert!(matches!(
        events.next().await.transpose()?,
        Some(ModelEvent::Wait(
            crate::model::ModelWait::AdmissionStarted {
                cause: crate::model::ModelAdmissionCause::Credit
            }
        ))
    ));
    assert!(events.next().now_or_never().is_none());
    drop(events);
    assert_eq!(
        js_sys::Reflect::get(&state, &"aborted".into())
            .map_err(|_| Error::Invalid("count fixture cancellation missing".into()))?
            .as_bool(),
        Some(true)
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn missing_accounting_remains_explicitly_unsupported() -> Result<()> {
    let request = prepared("selected")?;
    let provider = provider();
    assert!(matches!(
        provider.context_capacity(&request.request().model),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        counted(&provider, &request).await,
        Err(Error::Unsupported(_))
    ));
    Ok(())
}

async fn counted(provider: &HostModel, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
    use futures::StreamExt as _;
    let mut events = provider.count_tokens(request, 0);
    while let Some(event) = events.next().await {
        match event? {
            ModelEvent::Wait(_) | ModelEvent::Trace { .. } => {}
            ModelEvent::Counted { count } => return Ok(count),
            _ => return Err(Error::Invalid("counter emitted generation output".into())),
        }
    }
    Err(Error::Invalid("counter ended without its count".into()))
}
