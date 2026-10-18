//! Actual JS callback boundary checks for portable provider accounting.

use super::*;
use crate::{
    context::ModelContextCapacity,
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
    }
}

fn prepared(revision: &str) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new(
                "accounting".into(),
                "model".into(),
                revision.into(),
                serde_json::json!({}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("canonical input".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(4),
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
        }
    );
    provider.count_tokens = Some(Function::new_with_args(
        "bytes,digest",
        "const request = JSON.parse(new TextDecoder().decode(bytes)); if(request.model.revision !== 'selected' || request.messages[0].content !== 'canonical input') throw new Error('wrong canonical request'); return {request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [3]};",
    ));
    let count = provider.count_tokens(&request).await?;
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
        "return Promise.resolve({request_digest:digest, input_tokens:512n, fixed_tokens:1, message_tokens:[1]});",
    ));
    let count = provider.count_tokens(&request).await?;
    assert_eq!(count.validate(&request, &mut None)?, 512);
    assert_eq!(count.fixed_tokens + count.message_tokens[0], 2);
    provider.count_tokens = Some(Function::new_no_args(
        "return Promise.reject(new Error('private counter detail'));",
    ));
    let error = provider.count_tokens(&request).await.unwrap_err();
    assert!(!error.to_string().contains("private counter detail"));
    Ok(())
}

#[wasm_bindgen_test]
async fn accounting_rejects_unbound_malformed_results() -> Result<()> {
    let request = prepared("selected")?;
    let mut provider = provider();
    for response in [
        "return {request_digest: new Array(32).fill(0), input_tokens: 5, fixed_tokens: 2, message_tokens: [3]};",
        "return {request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: []};",
        "return {request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [3, 4]};",
        "return {request_digest: digest, input_tokens: 5, fixed_tokens: 2, message_tokens: [-1]};",
        "throw new Error('unavailable tokenizer');",
    ] {
        provider.count_tokens = Some(Function::new_with_args("bytes,digest", response));
        assert!(provider.count_tokens(&request).await.is_err());
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
async fn missing_accounting_remains_explicitly_unsupported() -> Result<()> {
    let request = prepared("selected")?;
    let provider = provider();
    assert!(matches!(
        provider.context_capacity(&request.request().model),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        provider.count_tokens(&request).await,
        Err(Error::Unsupported(_))
    ));
    Ok(())
}
