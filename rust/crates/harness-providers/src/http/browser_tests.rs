use super::*;
use futures::{FutureExt as _, StreamExt as _};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function httpFixture(mode) {
  let ready;
  let controller;
  const state = { calls: 0, aborted: false, ready: new Promise(resolve => { ready = resolve; }) };
  state.fetch = (url, init) => {
    state.calls++;
    state.request = { url, body: Array.from(init.body), headers: Object.fromEntries(init.headers),
      credentials: init.credentials, redirect: init.redirect };
    init.signal.addEventListener('abort', () => {
      state.aborted = true;
      if (controller) { controller.close(); controller = undefined; }
    }, { once: true });
    ready();
    if (mode === 'pending_head') {
      return new Promise((_, reject) => {
        init.signal.addEventListener('abort', () => reject(new Error('cancelled')), { once: true });
      });
    }
    const stream = new ReadableStream({ start(value) {
      controller = value;
      controller.enqueue(new TextEncoder().encode('first'));
      if (mode === 'complete') { controller.close(); controller = undefined; }
    } });
    return Promise.resolve(new Response(stream, { status: mode === 'error' ? 503 : 200 }));
  };
  return state;
}
export function timerFixture() {
  const originalSet = globalThis.setTimeout, originalClear = globalThis.clearTimeout;
  const active = new Map(), waits = [];
  globalThis.setTimeout = (callback, wait) => {
    const handle = {};
    waits.push(wait);
    active.set(handle, callback);
    return handle;
  };
  globalThis.clearTimeout = handle => active.delete(handle);
  return {
    active: () => active.size,
    waits: () => waits,
    fire: () => { const [handle, callback] = active.entries().next().value;
      active.delete(handle); callback(); },
    leak: () => globalThis.setTimeout(() => {}, 3600000),
    restore: () => { globalThis.setTimeout = originalSet; globalThis.clearTimeout = originalClear; }
  };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = httpFixture)]
    fn http_fixture(mode: &str) -> JsValue;
    #[wasm_bindgen(js_name = timerFixture)]
    fn timer_fixture() -> JsValue;
}

struct TimerFixture(JsValue);
impl TimerFixture {
    fn call(&self, name: &str) -> JsValue {
        field(&self.0, name)
            .unwrap()
            .dyn_into::<Function>()
            .unwrap()
            .call0(&self.0)
            .unwrap()
    }
}
impl Drop for TimerFixture {
    fn drop(&mut self) {
        self.call("restore");
    }
}

#[wasm_bindgen_test]
async fn deadline_timers_are_cancelled_and_long_waits_never_overflow() {
    for millis in [1, i32::MAX as u64, i32::MAX as u64 + 1, u32::MAX as u64] {
        let fixture = TimerFixture(timer_fixture());
        let mut delay = Box::pin(timer::delay(std::time::Duration::from_millis(millis)));
        let mut elapsed = false;
        while !elapsed {
            assert!(futures::poll!(&mut delay).is_pending());
            assert_eq!(fixture.call("active").as_f64(), Some(1.0));
            fixture.call("fire");
            // Flush the Promise reaction before polling the Rust waiter.
            JsFuture::from(Promise::resolve(&JsValue::UNDEFINED))
                .await
                .unwrap();
            elapsed = futures::poll!(&mut delay).is_ready();
        }
        let waits = js_sys::Array::from(&fixture.call("waits"));
        let mut total = 0u64;
        for wait in waits.iter() {
            let wait = wait.as_f64().unwrap() as u64;
            assert!((1..=i32::MAX as u64).contains(&wait));
            total += wait;
        }
        assert_eq!(total, millis);
        assert_eq!(fixture.call("active").as_f64(), Some(0.0));
    }
    let fixture = TimerFixture(timer_fixture());
    let mut delay = Box::pin(timer::delay(std::time::Duration::from_secs(3600)));
    assert!(futures::poll!(&mut delay).is_pending());
    drop(delay);
    assert_eq!(fixture.call("active").as_f64(), Some(0.0));
    // Negative control: the former unowned setTimeout leaves its callback registered.
    fixture.call("leak");
    assert_eq!(fixture.call("active").as_f64(), Some(1.0));
    fixture.call("fire");
}

fn field(object: &JsValue, name: &str) -> std::result::Result<JsValue, JsValue> {
    Reflect::get(object, &JsValue::from_str(name))
}

fn transport(fixture: &JsValue) -> std::result::Result<BrowserTransport, JsValue> {
    Ok(BrowserTransport::new(
        field(fixture, "fetch")?.dyn_into::<Function>()?,
    ))
}

fn request() -> HttpRequest {
    HttpRequest {
        url: "https://example.test/v1/responses".into(),
        headers: BTreeMap::from([
            ("authorization".into(), "Bearer fixture".into()),
            ("x-api-key".into(), "anthropic-fixture".into()),
            ("x-goog-api-key".into(), "gemini-fixture".into()),
        ]),
        body: Bytes::from_static(b"{}"),
    }
}

fn js_error(error: acyclic_harness::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[wasm_bindgen_test]
async fn fetch_streams_bytes_and_preserves_request_security_options()
-> std::result::Result<(), JsValue> {
    let fixture = http_fixture("complete");
    let transport = transport(&fixture)?;
    let mut response = transport.send(request()).await.map_err(js_error)?;
    assert_eq!(response.status, 200);
    assert_eq!(
        response.body.next().await.transpose().map_err(js_error)?,
        Some(Bytes::from_static(b"first"))
    );
    assert!(response.body.next().await.is_none());
    let submitted = field(&fixture, "request")?;
    assert_eq!(
        field(&submitted, "url")?.as_string().as_deref(),
        Some("https://example.test/v1/responses")
    );
    assert_eq!(
        field(&submitted, "redirect")?.as_string().as_deref(),
        Some("error")
    );
    assert_eq!(
        field(&submitted, "credentials")?.as_string().as_deref(),
        Some("omit")
    );
    assert_eq!(
        field(&field(&submitted, "headers")?, "authorization")?
            .as_string()
            .as_deref(),
        Some("Bearer fixture")
    );
    for (header, expected) in [
        ("x-api-key", "anthropic-fixture"),
        ("x-goog-api-key", "gemini-fixture"),
    ] {
        assert_eq!(
            field(&field(&submitted, "headers")?, header)?
                .as_string()
                .as_deref(),
            Some(expected)
        );
    }
    assert_eq!(Uint8Array::new(&field(&submitted, "body")?).to_vec(), b"{}");
    Ok(())
}

#[wasm_bindgen_test]
async fn dropping_response_body_aborts_fetch_between_reads_and_during_a_pending_read()
-> std::result::Result<(), JsValue> {
    for pending_read in [false, true] {
        let fixture = http_fixture("pending_body");
        let transport = transport(&fixture)?;
        let mut response = transport.send(request()).await.map_err(js_error)?;
        assert!(
            response
                .body
                .next()
                .await
                .transpose()
                .map_err(js_error)?
                .is_some()
        );
        assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(false));
        if pending_read {
            let mut pending = response.body.next();
            assert!(futures::poll!(&mut pending).is_pending());
            drop(pending);
            assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(false));
        }
        drop(response);
        assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(true));
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn dropping_pending_headers_aborts_the_owned_exchange() -> std::result::Result<(), JsValue> {
    let fixture = http_fixture("pending_head");
    let transport = transport(&fixture)?;
    let mut exchange = transport.send(request()).fuse();
    let mut ready = JsFuture::from(Promise::resolve(&field(&fixture, "ready")?)).fuse();
    futures::select_biased! {
        _ = ready => (),
        _ = exchange => return Err(JsValue::from_str("fixture exchange unexpectedly completed")),
    }
    assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(false));
    drop(exchange);
    assert_eq!(field(&fixture, "aborted")?.as_bool(), Some(true));
    Ok(())
}

#[wasm_bindgen_test]
async fn http_error_status_is_returned_without_retrying() -> std::result::Result<(), JsValue> {
    let fixture = http_fixture("error");
    let transport = transport(&fixture)?;
    let response = transport.send(request()).await.map_err(js_error)?;
    assert_eq!(response.status, 503);
    assert_eq!(field(&fixture, "calls")?.as_f64(), Some(1.0));
    Ok(())
}
