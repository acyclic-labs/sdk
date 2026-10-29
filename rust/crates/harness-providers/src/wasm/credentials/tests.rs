use super::*;
use acyclic_harness_core::wire::ErrorCode;
use futures::TryStreamExt as _;
use js_sys::Promise;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function credentialOwner(data, mode, unavailable, denied, indeterminate) {
  let ready, rejectHead, body;
  const state = { calls: 0, binds: 0, aborted: false,
    ready: new Promise(resolve => ready = resolve) };
  state.bind = (origin, reference, scheme) => {
    state.binds++;
    if (origin !== 'https://example.test' || reference !== 'secrets:/models/primary')
      throw Object.assign(new Error('private diagnostic'), {code:denied});
    return {credentialScope:new Uint8Array(mode === 'malformed' ? 31 : 32).fill(7),
      fetch(url, init) {
        if (this.credentialScope.length !== 32) throw new Error('binding receiver lost');
        state.calls++;
        const headers = new Headers(init.headers);
        if (headers.has('authorization') || headers.has('x-api-key'))
          throw new Error('secret entered guest request');
        if (scheme !== 'bearer' || init.redirect !== 'error') throw new Error('binding changed');
        init.signal.addEventListener('abort', () => { state.aborted = true; }, {once:true});
        if (mode === 'indeterminate')
          return Promise.reject(Object.assign(new Error('private diagnostic'), {code:indeterminate}));
        if (mode === 'head') {
          ready();
          return new Promise((_, reject) => rejectHead = reject);
        }
        return Promise.resolve(new Response(new ReadableStream({start(controller) {
          body = controller;
          if (mode === 'body') { ready(); return; }
          controller.enqueue(new TextEncoder().encode(data));
          controller.close();
        }})));
      }};
  };
  state.close = () => {
    const cause = Object.assign(new Error('private diagnostic'), {code:unavailable});
    rejectHead?.(cause);
    if (mode === 'body') body.error(cause);
  };
  return state;
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = credentialOwner)]
    fn credential_owner(
        data: &str,
        mode: &str,
        unavailable: i32,
        denied: i32,
        indeterminate: i32,
    ) -> JsValue;
}

fn field(owner: &JsValue, key: &str) -> JsValue {
    Reflect::get(owner, &key.into()).unwrap()
}

fn request() -> crate::HttpRequest {
    crate::HttpRequest {
        url: "https://example.test/v1/chat/completions".into(),
        headers: Default::default(),
        body: bytes::Bytes::from_static(b"{}"),
    }
}

#[wasm_bindgen_test]
async fn protected_fetch_retains_authority_and_settles_when_its_producer_closes() {
    let data = include_str!("../../../../conformance/fixtures/inference/llamacpp/text.body");
    for mode in ["complete", "malformed", "head", "body", "indeterminate"] {
        let owner = credential_owner(
            data,
            mode,
            ErrorCode::Unavailable as i32,
            ErrorCode::Unauthorized as i32,
            ErrorCode::Indeterminate as i32,
        );
        let resolver = Resolver(field(&owner, "bind").dyn_into().unwrap());
        let transport: Arc<dyn HttpTransport> = Arc::new(BrowserTransport::global().unwrap());
        assert!(
            resolver
                .bind(
                    CredentialSource::ApiKey("native-key".into()),
                    "https://example.test",
                    AuthorizationScheme::Bearer,
                    transport.clone()
                )
                .is_ok()
        );
        assert_eq!(field(&owner, "binds").as_f64(), Some(0.0));
        let refused = resolver
            .bind(
                CredentialSource::SecretReference("secrets:/missing"),
                "https://example.test",
                AuthorizationScheme::Bearer,
                transport.clone(),
            )
            .err()
            .unwrap();
        assert_eq!(refused.code(), ErrorCode::Unauthorized);
        assert!(!refused.to_string().contains("private diagnostic"));
        assert_eq!(field(&owner, "calls").as_f64(), Some(0.0));
        let binding = resolver.bind(
            CredentialSource::SecretReference("secrets:/models/primary"),
            "https://example.test",
            AuthorizationScheme::Bearer,
            transport,
        );
        if mode == "malformed" {
            assert!(matches!(
                binding,
                Err(acyclic_harness_core::Error::Invalid(_))
            ));
            assert_eq!(field(&owner, "calls").as_f64(), Some(0.0));
            continue;
        }
        let binding = binding.unwrap();
        assert!(binding.key.is_none());
        let operation = async {
            let response = binding.transport.send(request()).await?;
            response
                .body
                .map_ok(|bytes| bytes.to_vec())
                .try_concat()
                .await
        };
        if mode == "complete" {
            assert_eq!(operation.await.unwrap(), data.as_bytes());
            continue;
        }
        if mode == "indeterminate" {
            let cause = operation.await.unwrap_err();
            assert_eq!(cause.code(), ErrorCode::Indeterminate);
            assert!(
                crate::retry::Redelivery::for_request(crate::RetrySpec::DEFAULT, b"{}")
                    .next(&cause, None, false)
                    .is_none()
            );
            continue;
        }
        let ready = JsFuture::from(Promise::resolve(&field(&owner, "ready")));
        futures::pin_mut!(operation, ready);
        match futures::future::select(operation, ready).await {
            futures::future::Either::Left(_) => panic!("exchange completed before producer loss"),
            futures::future::Either::Right((ready, operation)) => {
                ready.unwrap();
                field(&owner, "close")
                    .dyn_into::<Function>()
                    .unwrap()
                    .call0(&owner)
                    .unwrap();
                let cause = operation.await.unwrap_err();
                assert_eq!(cause.code(), ErrorCode::Unavailable);
                assert!(!cause.to_string().contains("private diagnostic"));
                assert_eq!(field(&owner, "aborted").as_bool(), Some(true));
                let js = super::super::model_error(cause);
                assert_eq!(
                    field(&js, "code").as_f64(),
                    Some(f64::from(ErrorCode::Unavailable as i32))
                );
            }
        }
    }
}
