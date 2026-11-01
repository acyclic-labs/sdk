use super::super::*;
use crate::{
    AcceptedCommitOutcome, CommitCondition, CommitMutation, CommitRequest, IdempotencyKey,
};
use std::cell::Cell;

// Only the transport framing is supplied here. Every protobuf response comes
// from the original MemoryStream through the shipping WASM dispatch function.
#[wasm_bindgen(inline_js = r#"
export function acceptedCommitPeer(dispatch) {
  const original = globalThis.fetch;
  globalThis.fetch = async request => {
    const frame = new Uint8Array(await request.arrayBuffer());
    if (frame[0] !== 0 || new DataView(frame.buffer).getUint32(1) !== frame.length - 5)
      throw new Error('invalid original request frame');
    const reply = await dispatch('commit', frame.subarray(5));
    if (reply.code) throw new Error(reply.code);
    const trailers = new TextEncoder().encode('grpc-status: 0\r\n');
    const body = new Uint8Array(10 + reply.value.length + trailers.length);
    const view = new DataView(body.buffer);
    view.setUint32(1, reply.value.length);
    body.set(reply.value, 5);
    const offset = 5 + reply.value.length;
    body[offset] = 128;
    view.setUint32(offset + 1, trailers.length);
    body.set(trailers, offset + 5);
    return new Response(body, {headers: {'content-type': 'application/grpc-web+proto'}});
  };
  return {dispatch, restore() { globalThis.fetch = original; }};
}
export function acceptedCommitCredential() { return () => 'fixture'; }
export function restoreAcceptedCommitPeer(peer) { peer.restore(); }
"#)]
extern "C" {
    #[wasm_bindgen(js_name = acceptedCommitPeer)]
    fn peer(dispatch: &js_sys::Function) -> JsValue;
    #[wasm_bindgen(js_name = acceptedCommitCredential)]
    fn credential() -> js_sys::Function;
    #[wasm_bindgen(js_name = restoreAcceptedCommitPeer)]
    fn restore(peer: &JsValue);
}

struct Peer(JsValue);
impl Drop for Peer {
    fn drop(&mut self) {
        restore(&self.0);
    }
}

#[wasm_bindgen_test::wasm_bindgen_test]
async fn accepted_reply_metadata_fits_both_browser_adapters_and_exact_replay() {
    let provider = Rc::new(MemoryStream::default());
    let calls = Rc::new(Cell::new(0));
    let reply_bytes = Rc::new(Cell::new(0));
    let dispatch = Closure::<dyn Fn(String, Uint8Array) -> js_sys::Promise>::new({
        let (provider, calls, reply_bytes) = (provider.clone(), calls.clone(), reply_bytes.clone());
        move |operation: String, input: Uint8Array| {
            let (provider, calls, reply_bytes) =
                (provider.clone(), calls.clone(), reply_bytes.clone());
            let input = input.to_vec();
            wasm_bindgen_futures::future_to_promise(async move {
                assert_eq!(operation, "commit");
                calls.set(calls.get() + 1);
                let bytes = dispatch_commit(provider.as_ref(), &input).await?;
                reply_bytes.set(bytes.len());
                let reply = Object::new();
                Reflect::set(&reply, &"value".into(), &Uint8Array::from(bytes.as_slice()))?;
                Ok(reply.into())
            })
        }
    });
    let peer = Peer(peer(dispatch.as_ref().unchecked_ref()));
    let injected = provider_rpc::InjectedStream::new(peer.0.clone()).unwrap();
    let remote = remote::Remote::new("https://localhost", credential()).unwrap();
    for (case, (mutations, value_bytes)) in [(1, 1000), (32, 0)].into_iter().enumerate() {
        let paths = (0..mutations)
            .map(|index| crate::StreamPath::new(format!("accepted/{case}/{index}")).unwrap())
            .collect::<Vec<_>>();
        let request = CommitRequest {
            conditions: paths
                .iter()
                .cloned()
                .map(|path| CommitCondition::Absent { path })
                .collect(),
            mutations: paths
                .iter()
                .cloned()
                .map(|path| CommitMutation::Append {
                    path,
                    records: vec![
                        crate::AppendRecord::from(crate::OwnedBytes::from(
                            vec![7; value_bytes]
                        ));
                        crate::MAX_ITEMS
                    ],
                })
                .collect(),
            idempotency_key: IdempotencyKey::unique(),
            deadline_unix_millis: None,
        };
        let mut wire = wire_codec::commit_to_wire(&request);
        wire.include_envelope = true;
        assert!(wire.encoded_len() <= MAX_COMMAND_BYTES);
        let maximum = wire_codec::requests::accepted_response_limit(&wire).unwrap();
        let accepted = injected.commit_accepted(request.clone()).await.unwrap();
        // The former command-sized receive policy rejects this real response.
        assert!(reply_bytes.get() > MAX_COMMAND_BYTES);
        assert!(reply_bytes.get() <= maximum);
        assert_eq!(
            remote.commit_accepted(request.clone()).await.unwrap(),
            accepted
        );
        let mut replay = request;
        replay.deadline_unix_millis = Some(0);
        assert_eq!(
            injected.commit_accepted(replay.clone()).await.unwrap(),
            accepted
        );
        assert_eq!(remote.commit_accepted(replay).await.unwrap(), accepted);
        let AcceptedCommitOutcome::Committed { receipt, envelope } = accepted else {
            panic!("original accepted publication");
        };
        assert!(receipt.matches_envelope(&envelope));
        for path in paths {
            assert_eq!(provider.tail(path).await.unwrap(), crate::MAX_ITEMS as u64);
        }
    }
    assert_eq!(calls.get(), 8);
}
