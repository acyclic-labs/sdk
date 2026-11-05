//! Shared selector conformance over the real injected WASM provider port.
#![cfg(target_arch = "wasm32")]

use wasm_bindgen::prelude::*;

// Only the host port's method names and result envelope are adapted.
// The shipping WASM provider produces every frame and owns cancellation.
#[wasm_bindgen(inline_js = r#"
export function selectorPort(stream) {
  return {
async dispatch(operation, input) {
  return {value: await stream.dispatch(operation, input)};
},
async open(operation, input, preceding) {
  const cursor = await stream.openProviderCursor(operation, input, preceding);
  return {value: {
    async next() { return {value: await cursor.next()}; },
    close() { cursor.close(); }
  }};
}
  };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = selectorPort)]
    fn port(stream: JsValue) -> JsValue;
}

#[wasm_bindgen_test::wasm_bindgen_test]
async fn selectors_survive_the_paired_injected_provider() {
    acyclic_stream::conformance::verify_read_selectors(&acyclic_stream::MemoryStream::default())
        .await
        .expect("memory selector contract");
    let provider = acyclic_stream::wasm::WasmStream::connect_provider(port(
        acyclic_stream::wasm::WasmStream::new().into(),
    ))
    .unwrap();
    acyclic_stream::conformance::verify_read_selectors(provider.provider().as_ref())
        .await
        .expect("paired injected selector contract");
}
