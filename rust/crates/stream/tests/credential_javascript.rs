//! The browser adapter retains the real JavaScript Auth cache as custodian.
#![cfg(all(target_arch = "wasm32", feature = "wasm"))]
use acyclic_auth::credential::{CredentialRevision, javascript};
use futures::FutureExt;
use js_sys::{Function, Reflect};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export async function credentialSource(module) {
  const { pathToFileURL } = await import('node:url');
  const { CredentialCache, CredentialSnapshot, credentialProvider } = await import(pathToFileURL(module).href);
  let revision = '\ud800'.repeat(257), now = 1, calls = 0, closed = 0, block = false;
  let entered, cancelled;
  const started = new Promise(resolve => { entered = resolve; });
  const stopped = new Promise(resolve => { cancelled = resolve; });
  const cache = new CredentialCache(async (_, signal) => {
    calls++;
    if (block) {
      signal.addEventListener('abort', cancelled, { once: true });
      entered();
      await new Promise(() => {});
    }
    return new CredentialSnapshot(`synthetic-${calls}`, revision, 100);
  }, () => ({ nowUnixSeconds: now, revision }));
  const watch = cache.provider.watch;
  cache.provider.watch = () => {
    const registration = watch();
    return { ...registration, close() { closed++; registration.close(); } };
  };
  return {
    provider: cache.provider,
    raw: credentialProvider({ credential: () => 'synthetic-raw' }),
    calls: () => calls, closed: () => closed,
    expire: () => { now = 100; },
    replace: () => { revision = 'other-account/audience'; cache.invalidate(); },
    block: () => { block = true; cache.invalidate(); },
    started: () => started, stopped: () => stopped,
  };
}
"#)]
extern "C" {
    #[wasm_bindgen(catch, js_name = credentialSource)]
    async fn source(module: &str) -> Result<JsValue, JsValue>;
}

fn function(owner: &JsValue, name: &str) -> Function {
    Reflect::get(owner, &name.into())
        .unwrap()
        .dyn_into()
        .unwrap()
}
fn call(owner: &JsValue, name: &str) -> JsValue {
    function(owner, name).call0(owner).unwrap()
}

#[wasm_bindgen_test]
async fn javascript_owner_keeps_exact_rejection_revision_watch_and_cancellation() {
    let source = source(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../typescript/packages/auth/dist/credential.js"
    ))
    .await
    .unwrap();
    let provider = javascript::provider(function(&source, "provider"));
    let watch = provider.watch().await.unwrap().unwrap();
    let old = provider.resolve().await.unwrap();
    let revision = CredentialRevision::from(vec![0xd800; 257]);
    assert_eq!(old.binding().unwrap().revision, revision);
    assert_eq!(watch.validity().revision, Some(revision));
    old.check().await.unwrap();
    assert!(old.reject().await.unwrap());
    watch.await.unwrap();
    assert_eq!(call(&source, "closed").as_f64(), Some(1.0));
    let current = provider.resolve().await.unwrap();
    let watch = provider.watch().await.unwrap().unwrap();
    assert!(!old.reject().await.unwrap());
    current.check().await.unwrap();
    assert_eq!(call(&source, "calls").as_f64(), Some(2.0));
    call(&source, "replace");
    watch.await.unwrap();
    assert!(current.check().await.is_err());
    let replacement = provider.resolve().await.unwrap();
    assert_ne!(replacement.binding(), current.binding());
    drop(provider.watch().await.unwrap());
    assert_eq!(call(&source, "closed").as_f64(), Some(3.0));
    call(&source, "expire");
    assert!(replacement.check().await.is_err());

    let raw = javascript::provider(function(&source, "raw"));
    assert!(raw.watch().await.unwrap().is_none());
    let request = raw.resolve().await.unwrap();
    request.check().await.unwrap();
    assert!(request.binding().is_none());
    assert!(!request.reject().await.unwrap());

    call(&source, "block");
    let mut pending = Box::pin(provider.resolve());
    let started =
        wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&call(&source, "started")));
    futures::select! {
        result = pending.as_mut().fuse() => panic!("blocked source settled: {result:?}"),
        _ = started.fuse() => {},
    }
    drop(pending);
    wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&call(&source, "stopped")))
        .await
        .unwrap();
}
