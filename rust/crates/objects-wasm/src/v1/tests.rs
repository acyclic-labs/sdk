use super::*;
use acyclic_objects::{
    ByteRange, RootId,
    binary::{BinaryObjects, Transport as _, javascript::JavaScriptTransport},
};
use acyclic_stream::OwnedBytes;

// Only fault selection and ownership counters wrap the actual public memory
// binding. Every reply and mutation is produced by the original Rust account.
#[wasm_bindgen(inline_js = "
export function accountPort(memory) {
    return {
        opened: 0, closed: 0, truncate: false, badRevision: false, last: Promise.resolve(),
        open(route, input) {
            ++this.opened;
            const request = memory.open(route, input);
            let pulled = false, closed = false;
            const close = () => {
                if (closed) return;
                closed = true;
                ++this.closed;
                request.close();
                if (!pulled) request.free();
            };
            const port = this;
            const owner = {
                next() {
                    if (closed || pulled) return Promise.resolve(null);
                    pulled = true;
                    return port.last = request.next().then(value => {
                        if (value !== null && port.truncate) value.body = value.body.slice(0, value.body.length >>> 1);
                        if (value !== null && port.badRevision) {
                            port.badRevision = false;
                            value.revision = new Uint8Array([255]);
                        }
                        return value;
                    }).finally(() => { close(); request.free(); });
                },
                close,
            };
            this.request = owner;
            return owner;
        }
    };
}
export function closeLast(port) { port.request.close(); }
export function settled(port) { return port.last.catch(() => {}); }
export function truncateReply(port) { port.truncate = true; }
export function malformedRevision(port) { port.badRevision = true; }
export function ownersBalanced(port) { return port.opened === port.closed; }
export function opened(port) { return port.opened; }
")]
extern "C" {
    #[wasm_bindgen(js_name = accountPort)]
    fn account_port(memory: JsValue) -> JsValue;
    #[wasm_bindgen(js_name = closeLast)]
    fn close_last(port: &JsValue);
    fn settled(port: &JsValue) -> js_sys::Promise;
    #[wasm_bindgen(js_name = truncateReply)]
    fn truncate_reply(port: &JsValue);
    #[wasm_bindgen(js_name = malformedRevision)]
    fn malformed_revision(port: &JsValue);
    #[wasm_bindgen(js_name = ownersBalanced)]
    fn owners_balanced(port: &JsValue) -> bool;
    fn opened(port: &JsValue) -> u32;
}

#[wasm_bindgen_test::wasm_bindgen_test]
async fn original_javascript_account_preserves_results_and_cancellation() {
    let account = MemoryObjects::memory();
    let port = account_port(
        ObjectsV1Memory {
            inner: Arc::new(account.clone()),
        }
        .into(),
    );
    let transport = JavaScriptTransport::new(port.clone()).unwrap();
    let client = BinaryObjects::new(transport.clone());
    let body = OwnedBytes::from_static(b"selected original account bytes");
    let digest = Digest::of(&body);
    let put = client.put(body.clone(), Vec::new()).await.unwrap();
    assert_eq!(put.verified().unwrap().digest, digest);
    assert_eq!(
        put.verified().unwrap().revision,
        account.head(digest).await.unwrap().revision
    );
    assert!(put.verified().unwrap().revision.is_some());
    assert_eq!(
        client
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap()
            .body(),
        &body
    );
    // A batch of puts is one call, answered member by member in order.
    let calls = opened(&port);
    let batch = client
        .put_batch(vec![
            (OwnedBytes::from_static(b"batch member"), vec![digest]),
            (
                OwnedBytes::from_static(b"batch dangling"),
                vec![Digest::of(b"never stored")],
            ),
            (body.clone(), Vec::new()),
        ])
        .await;
    assert_eq!(opened(&port), calls + 1);
    assert!(matches!(batch[0], Ok(acyclic_objects::Put::Created { .. })));
    assert_eq!(
        batch[1].as_ref().unwrap_err().code,
        wire::ErrorCode::MissingReference
    );
    assert_eq!(
        batch[2].as_ref().unwrap().verified().unwrap().revision,
        account.head(digest).await.unwrap().revision
    );
    let root = RootId::new(b"original-js-entry".to_vec()).unwrap();
    client.retain(&root, vec![digest]).await.unwrap();
    client.collect().await.unwrap();
    assert_eq!(
        account
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap()
            .body(),
        &body
    );

    let other = account_port(ObjectsV1Memory::new().into());
    let other = BinaryObjects::new(JavaScriptTransport::new(other).unwrap());
    assert_eq!(
        other.head(digest).await.unwrap_err().code,
        wire::ErrorCode::NotFound
    );
    assert_eq!(
        client.availability(digest).await.err().unwrap().code,
        wire::ErrorCode::Unsupported
    );
    assert_eq!(
        transport
            .invoke("not-an-objects-route", Vec::new().into())
            .await
            .unwrap_err()
            .code,
        wire::ErrorCode::InvalidArgument
    );

    // First poll opens the original request; dropping it cancels before its
    // scheduled account operation runs. Wait on that original completion.
    let cancelled = OwnedBytes::from_static(b"cancelled before account publication");
    let cancelled_digest = Digest::of(&cancelled);
    {
        let operation = client.put(cancelled, Vec::new());
        futures::pin_mut!(operation);
        assert!(futures::poll!(operation.as_mut()).is_pending());
    }
    wasm_bindgen_futures::JsFuture::from(settled(&port))
        .await
        .unwrap();
    assert_eq!(
        account.head(cancelled_digest).await.unwrap_err().code,
        wire::ErrorCode::NotFound
    );

    let operation = transport.invoke("blobs/usage", Vec::new().into());
    futures::pin_mut!(operation);
    assert!(futures::poll!(operation.as_mut()).is_pending());
    close_last(&port);
    assert_eq!(
        operation.await.unwrap_err().code,
        wire::ErrorCode::Unavailable
    );
    assert!(owners_balanced(&port));

    malformed_revision(&port);
    assert_eq!(
        client.head(digest).await.unwrap_err().code,
        wire::ErrorCode::Unavailable
    );
    truncate_reply(&port);
    assert_eq!(
        client
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap_err()
            .code,
        wire::ErrorCode::Unavailable
    );
    assert_eq!(
        account
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap()
            .body(),
        &body
    );
    assert!(owners_balanced(&port));
}
