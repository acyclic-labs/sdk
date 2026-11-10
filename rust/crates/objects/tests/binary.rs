//! Shared real-account conformance through the portable binary client.
#![cfg(all(feature = "binary", feature = "json"))]

use acyclic_objects::{
    Availability, Blobs, ByteRange, Digest, Error, MemoryMedium, MemoryObjects, RootId, Store,
    binary::{BinaryObjects, Reply, Transport},
    conformance, route, wire,
};
use acyclic_stream::{OwnedBytes, Revision};
use bytes::Bytes;
use futures::FutureExt as _;
use prost::Message;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[cfg(target_arch = "wasm32")]
use std::rc::Rc as Owner;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc as Owner;

#[derive(Default)]
struct Observed {
    calls: usize,
    allocation: Range<usize>,
    revision: Option<Revision>,
    substitute: Option<Digest>,
    truncate: bool,
}

struct Original {
    account: Owner<MemoryObjects>,
    observed: Arc<Mutex<Observed>>,
}

impl Transport for Original {
    async fn invoke(&self, route: &str, mut input: Bytes) -> Result<Reply, Error> {
        let (substitute, truncate) = {
            let mut observed = self.observed.lock().unwrap();
            observed.calls += 1;
            (
                observed.substitute.take(),
                std::mem::take(&mut observed.truncate),
            )
        };
        // Substitute a real successful reply for another selected object.
        if let Some(digest) = substitute {
            assert_eq!(route, "blobs/get");
            let mut request = wire::GetBlobRequest::decode(input).unwrap();
            request.digest = Some(digest.into());
            input = request.encode_to_vec().into();
        }
        let (mut body, revision) = route::invoke_at(self.account.as_ref(), route, input).await?;
        if truncate {
            body.truncate(body.len() / 2);
        }
        let mut observed = self.observed.lock().unwrap();
        observed.allocation = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
        observed.revision.clone_from(&revision);
        Ok((body.into(), revision))
    }

    async fn availability(&self, digest: Digest) -> Result<Availability, Error> {
        self.account.availability(digest).await
    }
}

fn client(account: MemoryObjects) -> (BinaryObjects<Original>, Arc<Mutex<Observed>>) {
    let observed = Arc::new(Mutex::new(Observed::default()));
    (
        BinaryObjects::new(Original {
            account: Owner::new(account),
            observed: Arc::clone(&observed),
        }),
        observed,
    )
}

fn revision(observed: &Mutex<Observed>) -> Option<Revision> {
    observed.lock().unwrap().revision.clone()
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
async fn binary_accounts_preserve_original_results_and_observation_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(MemoryMedium::default());
    let (first, observed) = client(store.account(b"binary-first")?);
    let (second, _) = client(store.account(b"binary-second")?);
    conformance::verify(&first, &second, "binary-original").await?;

    let body = OwnedBytes::from_static(b"the original selected body with an owned response");
    let digest = Digest::of(&body);
    let put = first.put(body.clone(), Vec::new()).await?;
    assert_eq!(put.verified().unwrap().revision, revision(&observed));
    assert!(revision(&observed).is_some());

    let object = first.get(digest, ByteRange::FULL, u64::MAX).await?;
    assert_eq!(object.body(), &body);
    assert_eq!(object.revision(), revision(&observed).as_ref());
    assert!(
        observed
            .lock()
            .unwrap()
            .allocation
            .contains(&(object.body().as_ptr() as usize))
    );
    assert_eq!(first.head(digest).await?.revision, revision(&observed));
    assert_eq!(
        first.list(None, u32::MAX).await?.revision,
        revision(&observed)
    );
    let root = RootId::new(b"binary-original-intent".to_vec())?;
    assert_eq!(
        first.retain(&root, vec![digest]).await?.revision,
        revision(&observed)
    );
    assert_eq!(first.release(&root).await?.revision, revision(&observed));
    assert_eq!(first.delete(digest).await?.revision, revision(&observed));
    assert_eq!(first.collect().await?.revision, revision(&observed));

    let original = first.put(body.clone(), Vec::new()).await?;
    let other = first
        .put(
            OwnedBytes::from_static(b"a different real object"),
            Vec::new(),
        )
        .await?;
    observed.lock().unwrap().substitute = Some(other.verified().unwrap().digest);
    assert_eq!(
        first
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap_err()
            .code,
        wire::ErrorCode::Unavailable,
    );
    observed.lock().unwrap().truncate = true;
    assert_eq!(
        first
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .unwrap_err()
            .code,
        wire::ErrorCode::Unavailable,
    );
    // Neither malformed response changes the original account's selected bytes.
    assert_eq!(
        first.get(digest, ByteRange::FULL, u64::MAX).await?.body(),
        &body
    );
    assert_eq!(original.verified().unwrap().digest, digest);

    // A barrier over a durable binary producer needs no second request.
    let before = observed.lock().unwrap().calls;
    first.flush(Some(&[digest])).await?;
    assert_eq!(observed.lock().unwrap().calls, before);

    // The cursor retains its exact-key registration, not its producer. Dropping
    // a parked pull keeps that registration; dropping the source ends the wait.
    let mut observation = first.availability(digest).await?;
    assert!(observation.changed().now_or_never().is_none());
    drop(store);
    drop(first);
    let stopped = observation
        .changed()
        .now_or_never()
        .expect("original producer dropped");
    assert_eq!(stopped.unwrap_err().code, wire::ErrorCode::Unavailable);
    Ok(())
}
