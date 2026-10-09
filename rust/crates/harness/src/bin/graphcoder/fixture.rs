//! Explicit read-only playback of the existing native/WASM wire fixture.
//! It supplies no authority, admission, cancellation or durable session host.

use acyclic_harness::{
    Error, Result, wire,
    wire_api::{HarnessWireApi, OperationControlRequest, negotiate, validate_resume_protocol},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::{FutureExt as _, stream};
use prost::Message as _;

pub struct WireFixture(wire::Delivery);

impl WireFixture {
    pub fn load() -> Result<Self> {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/native-wasm-event-v2.json"
        ))
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let hex = fixture
            .get("event_wire_hex")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("wire fixture is missing".into()))?;
        let bytes = hex::decode(hex).map_err(|error| Error::Invalid(error.to_string()))?;
        let event = wire::EventEnvelope::decode(bytes.as_slice())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let predecessor = event
            .revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("fixture event has no predecessor".into()))?;
        Ok(Self(wire::Delivery {
            authority: event.authority.clone(),
            generation: "offline-wire-fixture".into(),
            from_revision: predecessor,
            through_revision: event.revision,
            events: vec![event],
            live: false,
        }))
    }

    pub fn start_cursor(&self) -> wire::ReplayCursor {
        wire::ReplayCursor {
            authority: self.0.authority.clone(),
            generation: self.0.generation.clone(),
            revision: self.0.from_revision,
        }
    }
}

fn unavailable<T: Send>() -> BoxProviderFuture<'static, Result<T>> {
    async {
        Err(Error::Unsupported(
            "wire fixture has no durable product host".into(),
        ))
    }
    .boxed()
}

impl HarnessWireApi for WireFixture {
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> BoxProviderFuture<'a, Result<wire::HandshakeResponse>> {
        async move { negotiate(&request, &wire::CapabilitySet::default()) }.boxed()
    }

    fn submit<'a>(
        &'a self,
        _: wire::CommandEnvelope,
    ) -> BoxProviderFuture<'a, Result<wire::Admission>> {
        unavailable()
    }

    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        async move {
            validate_resume_protocol(&request)?;
            let finished = match request.cursors.as_slice() {
                [cursor]
                    if cursor.authority == self.0.authority
                        && cursor.generation == self.0.generation
                        && (cursor.revision == self.0.from_revision
                            || cursor.revision == self.0.through_revision) =>
                {
                    cursor.revision == self.0.through_revision
                }
                _ => {
                    return Err(Error::Conflict(
                        "fixture cursor does not match playback".into(),
                    ));
                }
            };
            let delivery = (!finished).then(|| Ok(self.0.clone()));
            Ok(Box::pin(stream::iter(delivery))
                as BoxProviderStream<'static, Result<wire::Delivery>>)
        }
        .boxed()
    }

    fn authorize_operation_control<'a>(
        &'a self,
        _: &'a OperationControlRequest,
    ) -> BoxProviderFuture<'a, Result<()>> {
        unavailable()
    }

    fn observe<'a>(
        &'a self,
        _: wire::ObserveRequest,
    ) -> BoxProviderFuture<'a, Result<wire::OperationStatus>> {
        unavailable()
    }

    fn cancel<'a>(
        &'a self,
        _: wire::CancelRequest,
    ) -> BoxProviderFuture<'a, Result<wire::CancelResponse>> {
        unavailable()
    }
}
