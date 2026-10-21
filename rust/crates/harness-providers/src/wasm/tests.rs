use super::*;
use crate::{Adapter, fixtures::fixtures};
use acyclic_harness_core::model::{ModelContent, ModelMessage, ModelRequest, ModelRole};
use js_sys::Promise;
use serde_json::{Value, json};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function modelFixture(data, mode = 'complete') {
  let ready;
  const state = { counts: 0, calls: 0, aborted: false, ready: new Promise(resolve => ready = resolve) };
  state.fetch = (url, init) => {
    if (String(url).endsWith('/count_tokens')) {
      state.counts++;
      if (mode === 'count_pending') {
        init.signal.addEventListener('abort', () => { state.aborted = true; }, {once: true});
        ready();
        return new Promise((_, reject) => init.signal.addEventListener('abort', () => reject(new Error('aborted')), {once: true}));
      }
      return Promise.resolve(Response.json({input_tokens:10}));
    }
    state.calls++;
    state.url = url;
    state.headers = new Headers(init.headers);
    state.body = JSON.parse(new TextDecoder().decode(init.body));
    init.signal.addEventListener('abort', () => { state.aborted = true; }, {once: true});
    ready();
    if (mode === 'pending') return new Promise((_, reject) => {
      init.signal.addEventListener('abort', () => reject(new Error('aborted')), {once: true});
    });
    const bytes = new TextEncoder().encode(data);
    return Promise.resolve(new Response(new ReadableStream({start(controller) {
      for (const byte of bytes) controller.enqueue(new Uint8Array([byte]));
      controller.close();
    }})));
  };
  return state;
}
export function modelContentFixture(data) {
  const state = modelFixture('data: [DONE]\n\n');
  state.reads = 0;
  state.reader = (file, maximum) => {
    state.reads++;
    state.file = file;
    state.maximum = maximum;
    return Promise.resolve(new Uint8Array(data));
  };
  return state;
}
export function watchModelInput(state, request) {
  const bytes = request.serializedInput;
  state.inputReads = 0;
  Object.defineProperty(request, 'serializedInput', {get() { state.inputReads++; return bytes; }});
}
export async function cancelCount(promise, controller, ready) {
  await ready;
  controller.abort();
  try { await promise; throw new Error('count was not cancelled'); }
  catch (error) { if (error.name !== 'AbortError') throw error; }
}
export async function collectModel(iterator) {
  const events = [];
  for await (const event of iterator) events.push(event);
  return events;
}
export async function cancelModel(iterator, signal, ready, useReturn) {
  const pending = iterator.next();
  await ready;
  if (useReturn) await iterator.return(); else signal.abort();
  try {
    const result = await pending;
    if (!useReturn || !result.done) throw new Error('cancel did not terminate iterator');
  } catch (error) { if (useReturn || error.name !== 'AbortError') throw error; }
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = cancelCount)]
    fn cancel_count(
        promise: Promise,
        controller: &web_sys::AbortController,
        ready: JsValue,
    ) -> Promise;
    #[wasm_bindgen(js_name = modelFixture)]
    fn model_fixture(data: &str, mode: &str) -> JsValue;
    #[wasm_bindgen(js_name = modelContentFixture)]
    fn content_fixture(data: &Uint8Array) -> JsValue;
    #[wasm_bindgen(js_name = watchModelInput)]
    fn watch_input(state: &JsValue, request: &JsValue);
    #[wasm_bindgen(js_name = collectModel)]
    fn collect_model(iterator: JsValue) -> Promise;
    #[wasm_bindgen(js_name = cancelModel)]
    fn cancel_model(
        iterator: JsValue,
        signal: &web_sys::AbortController,
        ready: JsValue,
        use_return: bool,
    ) -> Promise;
}

fn field(value: &JsValue, name: &str) -> Result<JsValue, JsValue> {
    Reflect::get(value, &name.into())
}

fn provider(adapter: Adapter, fixture: &JsValue) -> Result<WasmModelProvider, JsValue> {
    let mut registry = WasmProviderRegistry::new(
        to_js(&json!({"fixture":"key"}))?,
        to_js(&Limits::default())?,
        Some(field(fixture, "fetch")?.dyn_into::<Function>()?),
        None,
    )?;
    registry.register(
        "fixture".into(),
        to_js(&json!({
            "adapter": adapter, "authentication":"api_key", "base_url":"https://example.test/v1",
            "models":{"model":{"capacity":{"context_tokens":4096,"output_tokens":512},"features":{"tools":true,"images":true}}},
            "token_counter":{"path":"count_tokens","adapter":adapter,"response_field":"input_tokens","framing_tokens":0},
            "max_event_bytes":4096,
        }))?,
    )?;
    registry.resolve("fixture/model", None)
}

fn request(
    provider: &WasmModelProvider,
    signal: Option<&web_sys::AbortSignal>,
) -> Result<JsValue, JsValue> {
    let model = provider.provider.model().clone();
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            model,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: vec![],
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )
    .map_err(error)?;
    let request = js_sys::Object::new();
    Reflect::set(&request, &"model".into(), &provider.model()?)?;
    Reflect::set(
        &request,
        &"serializedInput".into(),
        &Uint8Array::from(prepared.bytes().as_ref()),
    )?;
    if let Some(signal) = signal {
        Reflect::set(&request, &"signal".into(), signal)?;
    }
    Ok(request.into())
}

fn dispatch() -> Result<JsValue, JsValue> {
    to_js(&json!({"operationId":"00000000-0000-4000-8000-000000000001","step":2}))
}

#[wasm_bindgen_test]
async fn registry_drives_every_adapter_through_fetch_with_accounting_and_public_events()
-> Result<(), JsValue> {
    for (adapter, data) in fixtures() {
        let fixture = model_fixture(data, "complete");
        let provider = provider(adapter, &fixture)?;
        let request = request(&provider, None)?;
        let capacity: Value = from_js(provider.context_capacity(provider.model()?)?)?;
        assert_eq!(capacity, json!({"contextTokens":4096,"outputTokens":512}));
        let prepared = provider.prepare(request).await?;
        let count = prepared.count_tokens().await?;
        assert_eq!(
            field(&count, "requestDigest")?
                .dyn_into::<Uint8Array>()?
                .length(),
            32
        );
        let tokens: Vec<u32> = from_js(field(&count, "messageTokens")?)?;
        assert_eq!(tokens.len(), 1);
        assert!(tokens[0] >= 5);
        assert_eq!(field(&fixture, "calls")?.as_f64(), Some(0.0));
        let iterator = prepared.generate(dispatch()?)?;
        let events: Vec<Value> = from_js(JsFuture::from(collect_model(iterator)).await?)?;
        assert_eq!(events.first().unwrap()["kind"], "content");
        assert_eq!(events.first().unwrap()["delta"], "hé");
        let call = events
            .iter()
            .find(|event| event["kind"] == "tool_call")
            .unwrap();
        assert!(
            call["callId"]
                .as_str()
                .unwrap()
                .starts_with("00000000-0000-4000-8000-000000000001:")
        );
        assert!(call.get("call_id").is_none());
        assert_eq!(call["name"], "echo");
        assert_eq!(call["arguments"], json!({"value":1}));
        assert_eq!(events.last().unwrap()["kind"], "completed");
        assert!(events.last().unwrap()["metadata"]["usage"].is_object());
        assert_eq!(field(&fixture, "calls")?.as_f64(), Some(1.0));
        let headers = field(&fixture, "headers")?.dyn_into::<web_sys::Headers>()?;
        assert_eq!(
            headers
                .get("anthropic-dangerous-direct-browser-access")?
                .as_deref(),
            (adapter == Adapter::Anthropic).then_some("true"),
        );
        assert!(
            JsFuture::from(provider.reconcile(JsValue::UNDEFINED))
                .await?
                .is_undefined()
        );
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn abort_and_iterator_return_cancel_pending_fetch_headers() -> Result<(), JsValue> {
    for use_return in [false, true] {
        let fixture = model_fixture("", "pending");
        let provider = provider(Adapter::Chat, &fixture)?;
        let controller = web_sys::AbortController::new()?;
        let iterator = provider
            .prepare(request(&provider, Some(&controller.signal()))?)
            .await?
            .generate(dispatch()?)?;
        JsFuture::from(cancel_model(
            iterator,
            &controller,
            field(&fixture, "ready")?,
            use_return,
        ))
        .await?;
        assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(true));
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn already_aborted_requests_never_dispatch() -> Result<(), JsValue> {
    let fixture = model_fixture("", "pending");
    let provider = provider(Adapter::Chat, &fixture)?;
    let controller = web_sys::AbortController::new()?;
    controller.abort();
    assert!(
        provider
            .prepare(request(&provider, Some(&controller.signal()))?)
            .await
            .is_err()
    );
    assert_eq!(field(&fixture, "calls")?.as_f64(), Some(0.0));
    Ok(())
}

#[wasm_bindgen_test]
async fn accounting_and_dispatch_reject_tampered_requests_without_effects() -> Result<(), JsValue> {
    let fixture = model_fixture("", "pending");
    let provider = provider(Adapter::Chat, &fixture)?;
    let request = request(&provider, None)?;
    Reflect::set(
        &request,
        &"serializedInput".into(),
        &Uint8Array::from(b"invalid".as_slice()),
    )?;
    assert!(provider.prepare(request).await.is_err());
    assert_eq!(field(&fixture, "calls")?.as_f64(), Some(0.0));
    Ok(())
}

#[wasm_bindgen_test]
async fn browser_reader_is_verified_for_counting_and_generation() -> Result<(), JsValue> {
    use acyclic_harness_core::{
        conversation::{FileDescriptor, FileRef},
        model::{FileProjectionPolicy, ModelContentPart},
    };
    let data = "hé\nfile".as_bytes();
    let fixture = content_fixture(&Uint8Array::from(data));
    let mut registry = WasmProviderRegistry::new(
        to_js(&json!({}))?,
        to_js(&Limits::default())?,
        Some(field(&fixture, "fetch")?.dyn_into::<Function>()?),
        Some(field(&fixture, "reader")?.dyn_into::<Function>()?),
    )?;
    registry.register(
        "local".into(),
        to_js(&json!({
            "adapter":"chat","authentication":"optional","base_url":"http://localhost:1234/v1",
            "models":{"fixture":{"capacity":{"context_tokens":4096,"output_tokens":512}}},"max_event_bytes":4096,
            "token_counter":{"path":"count_tokens","adapter":"chat","response_field":"input_tokens","framing_tokens":0},
        }))?,
    )?;
    let provider = registry.resolve("local/fixture", Some(to_js(&json!({"temperature":0.1}))?))?;
    let file = FileRef::new(
        from_js(to_js(&json!({"provider":{"namespace":"test","family":"filesystem","version":"2"},"id":"project","class":"project","owner":{"kind":"project","id":"project"}}))?)?,
        "file.txt".into(), "1".into(), FileDescriptor::from_bytes(data,"text/plain",&mut None).map_err(error)?, "file.txt".into(), &mut None,
    ).map_err(error)?;
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Part(ModelContentPart::File {
                    file,
                    policy: FileProjectionPolicy::BoundedFull,
                }),
            }],
            tools: vec![],
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )
    .map_err(error)?;
    let request = js_sys::Object::new();
    Reflect::set(&request, &"model".into(), &provider.model()?)?;
    Reflect::set(
        &request,
        &"serializedInput".into(),
        &Uint8Array::from(prepared.bytes().as_ref()),
    )?;
    watch_input(&fixture, &request);
    let request: JsValue = request.into();
    let prepared = provider.prepare(request.clone()).await?;
    prepared.count_tokens().await?;
    assert_eq!(field(&fixture, "reads")?.as_f64(), Some(1.0));
    JsFuture::from(collect_model(prepared.generate(dispatch()?)?)).await?;
    let body: Value = from_js(field(&fixture, "body")?)?;
    assert_eq!(body["messages"][0]["content"], "hé\nfile");
    assert_eq!(body["temperature"], 0.1);
    assert_eq!(field(&fixture, "reads")?.as_f64(), Some(1.0));
    assert_eq!(field(&fixture, "calls")?.as_f64(), Some(1.0));
    assert_eq!(field(&fixture, "inputReads")?.as_f64(), Some(1.0));
    // Old independent preparation is the negative control: both canonical
    // copying/decoding and verified file reads occur twice for count + generate.
    provider
        .prepare(request.clone())
        .await?
        .count_tokens()
        .await?;
    let independent = provider.prepare(request).await?;
    JsFuture::from(collect_model(independent.generate(dispatch()?)?)).await?;
    assert_eq!(field(&fixture, "reads")?.as_f64(), Some(3.0));
    assert_eq!(field(&fixture, "inputReads")?.as_f64(), Some(3.0));
    Ok(())
}

#[wasm_bindgen_test]
async fn counting_aborts_pending_headers_and_releases_the_fetch() -> Result<(), JsValue> {
    let fixture = model_fixture("", "count_pending");
    let provider = provider(Adapter::Chat, &fixture)?;
    let controller = web_sys::AbortController::new()?;
    let input = request(&provider, Some(&controller.signal()))?;
    let promise = wasm_bindgen_futures::future_to_promise(async move {
        provider.prepare(input).await?.count_tokens().await
    });
    JsFuture::from(cancel_count(
        promise,
        &controller,
        field(&fixture, "ready")?,
    ))
    .await?;
    assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(true));
    assert_eq!(field(&fixture, "counts")?.as_f64(), Some(1.0));
    Ok(())
}

#[wasm_bindgen_test]
async fn model_arguments_keep_the_exact_portable_boundary_and_reject_larger_values()
-> Result<(), JsValue> {
    let maximum = acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER;
    for value in [maximum, maximum + 1, maximum + 2] {
        let data = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{
            "delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"echo",
                "arguments":format!("{{\"value\":{value}}}")}}]},"finish_reason":"tool_calls"}]})
        );
        let fixture = model_fixture(&data, "complete");
        let provider = provider(Adapter::Chat, &fixture)?;
        let result = JsFuture::from(collect_model(
            provider
                .prepare(request(&provider, None)?)
                .await?
                .generate(dispatch()?)?,
        ))
        .await;
        if value > maximum {
            assert!(
                result.is_err(),
                "unsafe tool integers must be rejected before publication"
            );
        } else {
            let events = js_sys::Array::from(&result?);
            let value = field(&field(&events.get(0), "arguments")?, "value")?;
            assert!(value.is_bigint());
            assert_eq!(
                js_sys::BigInt::from(value)
                    .to_string(10)?
                    .as_string()
                    .as_deref(),
                Some("9007199254740991")
            );
        }
    }
    Ok(())
}

#[wasm_bindgen_test]
fn typed_numbers_and_arbitrary_model_json_have_distinct_wire_encodings() -> Result<(), JsValue> {
    let dispatch = json!({"operationId":"00000000-0000-4000-8000-000000000001", "step":2});
    assert_eq!(from_js::<Dispatch>(to_js(&dispatch)?)?.step, 2);
    // Negative control: applying the arbitrary-JSON policy to typed fields fails.
    assert!(from_js::<Dispatch>(to_js_json(&dispatch)?).is_err());
    let data = json!({"value":u64::MAX});
    assert!(
        to_js(&data).is_err(),
        "typed Number conversion must never round JSON integers"
    );
    let exact = field(&to_js_json(&data)?, "value")?;
    assert_eq!(
        js_sys::BigInt::from(exact)
            .to_string(10)?
            .as_string()
            .as_deref(),
        Some("18446744073709551615")
    );
    Ok(())
}

#[wasm_bindgen_test]
fn model_json_keeps_prototype_and_private_marker_keys_literal() -> Result<(), JsValue> {
    let data = json!({"__proto__":{"polluted":true}, "$serde_json::private::RawValue":"null"});
    let encoded = to_js_json(&data)?;
    assert!(
        js_sys::Object::keys(&js_sys::Object::from(encoded.clone()))
            .includes(&JsValue::from_str("__proto__"), 0)
    );
    let decoded = acyclic_harness_core::deserialize_json_value(
        serde_wasm_bindgen::Deserializer::from(encoded),
    )
    .map_err(error)?;
    assert_eq!(decoded, data);
    // Negative control: the default object setter consumes __proto__ as a prototype.
    let legacy = to_js(&data)?;
    assert!(
        !js_sys::Object::keys(&js_sys::Object::from(legacy))
            .includes(&JsValue::from_str("__proto__"), 0)
    );
    Ok(())
}
