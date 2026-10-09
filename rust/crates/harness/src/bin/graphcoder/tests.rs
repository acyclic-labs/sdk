use super::*;
use acyclic_harness::{OperationId, wire_api::current_protocol};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::{FutureExt as _, executor::block_on, stream};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Probe {
    submit: AtomicUsize,
    cancel: AtomicUsize,
    authorize: AtomicUsize,
    mismatch: bool,
    events: usize,
}

impl HarnessWireApi for Probe {
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> BoxProviderFuture<'a, Result<wire::HandshakeResponse>> {
        async move { negotiate(&request, &wire::CapabilitySet::default()) }.boxed()
    }

    fn submit<'a>(
        &'a self,
        command: wire::CommandEnvelope,
    ) -> BoxProviderFuture<'a, Result<wire::Admission>> {
        async move {
            self.submit.fetch_add(1, Ordering::SeqCst);
            if self.mismatch {
                Ok(wire::Admission::default())
            } else {
                Err(Error::Indeterminate(OperationId::parse(
                    &command
                        .operation
                        .ok_or_else(|| Error::Invalid("operation".into()))?
                        .operation_id,
                )?))
            }
        }
        .boxed()
    }

    fn replay<'a>(
        &'a self,
        _: wire::ResumeRequest,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        async move {
            let delivery = wire::Delivery {
                events: vec![wire::EventEnvelope::default(); self.events],
                ..wire::Delivery::default()
            };
            // A page request must return without waiting for stream completion.
            Ok(
                Box::pin(stream::once(async { Ok(delivery) }).chain(stream::pending()))
                    as BoxProviderStream<'static, Result<wire::Delivery>>,
            )
        }
        .boxed()
    }

    fn authorize_operation_control<'a>(
        &'a self,
        _: &'a acyclic_harness::wire_api::OperationControlRequest,
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.authorize.fetch_add(1, Ordering::SeqCst);
        async { Err(Error::Unauthorized("probe denied".into())) }.boxed()
    }

    fn observe<'a>(
        &'a self,
        _: wire::ObserveRequest,
    ) -> BoxProviderFuture<'a, Result<wire::OperationStatus>> {
        async { Err(Error::Unsupported("unused".into())) }.boxed()
    }

    fn cancel<'a>(
        &'a self,
        _: wire::CancelRequest,
    ) -> BoxProviderFuture<'a, Result<wire::CancelResponse>> {
        self.cancel.fetch_add(1, Ordering::SeqCst);
        async { Err(Error::Unsupported("must not dispatch".into())) }.boxed()
    }
}

fn command() -> wire::client_frame::Frame {
    wire::client_frame::Frame::Command(wire::CommandEnvelope {
        protocol: Some(current_protocol()),
        operation: Some(wire::OperationIdentity {
            operation_id: OperationId::from_bytes([7; 16]).to_string(),
            idempotency_key: "unchanged-admission".into(),
        }),
        ..wire::CommandEnvelope::default()
    })
}

#[test]
fn uncertain_submit_is_not_replayed_and_mismatched_admission_is_rejected() {
    let api = Probe::default();
    assert!(matches!(
        block_on(dispatch(&api, command())),
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(api.submit.load(Ordering::SeqCst), 1);
    let api = Probe {
        mismatch: true,
        ..Probe::default()
    };
    assert!(matches!(
        block_on(dispatch(&api, command())),
        Err(Error::Conflict(_))
    ));
    assert_eq!(api.submit.load(Ordering::SeqCst), 1);
}

#[test]
fn denied_control_does_not_call_cancel() {
    let api = Probe::default();
    let request = wire::CancelRequest {
        protocol: Some(current_protocol()),
        operation_id: OperationId::from_bytes([7; 16]).to_string(),
        idempotency_key: "cancel-once".into(),
        owner: Some(wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "probe".into(),
        }),
        scope: Some(wire::Scope {
            id: "test-only-untrusted-scope".into(),
            capabilities: vec!["operation:cancel".into()],
            issuer: "probe".into(),
            proof: vec![0; 32],
            ..wire::Scope::default()
        }),
        ..wire::CancelRequest::default()
    };
    assert!(matches!(
        block_on(dispatch(&api, wire::client_frame::Frame::Cancel(request))),
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(api.cancel.load(Ordering::SeqCst), 0);
    assert_eq!(api.authorize.load(Ordering::SeqCst), 1);
}

#[test]
fn replay_reads_one_delivery_and_renderer_rejects_oversized_pages() -> Result<()> {
    let request = || {
        wire::client_frame::Frame::Resume(wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: Vec::new(),
        })
    };
    assert!(matches!(
        block_on(dispatch(&Probe::default(), request())),
        Ok(Some(wire::server_frame::Frame::Delivery(_)))
    ));
    let api = Probe {
        events: MAX_PAGE_EVENTS + 1,
        ..Probe::default()
    };
    let response = wire::ServerFrame {
        frame: block_on(dispatch(&api, request()))?,
    };
    assert!(matches!(render(&response), Err(Error::Invalid(_))));
    Ok(())
}

#[test]
fn display_escapes_terminal_controls_and_preserves_unknown_outcomes() -> Result<()> {
    assert_eq!(
        text("é\n\u{1b}]52;c;secret\u{7}\r"),
        "é\n\\u{1b}]52;c;secret\\u{7}\\r"
    );
    let status = wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Status(wire::OperationStatus {
            state: 999,
            error: Some(wire::Error {
                message: "\u{1b}[2J".into(),
                ..wire::Error::default()
            }),
            ..wire::OperationStatus::default()
        })),
    };
    let output = render(&status)?;
    assert!(output.contains("999"));
    assert!(!output.contains('\u{1b}'));
    let oversized = wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Error(wire::Error {
            message: "x".repeat(MAX_FRAME_BYTES + 1),
            ..wire::Error::default()
        })),
    };
    assert!(matches!(render(&oversized), Err(Error::Invalid(_))));
    Ok(())
}

#[test]
fn fixture_cursor_is_explicit_and_mutations_stay_unavailable() -> Result<()> {
    let api = crate::fixture::WireFixture::load()?;
    assert!(matches!(
        block_on(dispatch(
            &api,
            wire::client_frame::Frame::Resume(wire::ResumeRequest {
                protocol: Some(current_protocol()),
                cursors: Vec::new(),
            })
        )),
        Err(Error::Conflict(_))
    ));
    let response = block_on(dispatch(
        &api,
        wire::client_frame::Frame::Resume(wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: vec![api.start_cursor()],
        }),
    ))?;
    let Some(wire::server_frame::Frame::Delivery(delivery)) = response else {
        return Err(Error::Invalid("fixture delivery missing".into()));
    };
    assert_eq!(delivery.from_revision, api.start_cursor().revision);
    assert_eq!(
        delivery.from_revision.checked_add(1),
        Some(delivery.through_revision)
    );
    assert!(
        delivery
            .events
            .iter()
            .all(|event| event.revision == delivery.through_revision)
    );
    let resume = wire::ResumeRequest {
        protocol: Some(current_protocol()),
        cursors: vec![wire::ReplayCursor {
            authority: delivery.authority,
            generation: delivery.generation,
            revision: delivery.through_revision,
        }],
    };
    assert!(block_on(dispatch(&api, wire::client_frame::Frame::Resume(resume)))?.is_none());
    assert!(matches!(
        block_on(dispatch(&api, command())),
        Err(Error::Unsupported(_))
    ));
    Ok(())
}
