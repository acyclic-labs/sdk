//! Async transport facade over the shared durable client reducer.

use super::{ClientSession, ClientStateStore};
use crate::{
    Error, Result, wire,
    wire_api::{HarnessWireApi, current_protocol, negotiate, validate_operation_status},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use futures::StreamExt;
use std::sync::Arc;

/// Native service discovery or an injected in-process/browser endpoint.
pub trait ServiceConnector: ProviderPlatform {
    /// Connects to the user's service; only `NotFound` requests autospawn.
    fn connect(&self) -> BoxProviderFuture<'_, Result<Arc<dyn HarnessWireApi>>>;
    /// Starts the per-user service and waits for readiness. A concurrent spawn is success.
    fn spawn(&self) -> BoxProviderFuture<'_, Result<()>>;
}

/// Connects to the per-user service, starting it only when its endpoint is absent.
pub async fn connect_or_spawn<S: ClientStateStore>(
    connector: &impl ServiceConnector,
    session: ClientSession<S>,
) -> Result<HarnessClient<S>> {
    let api = match connector.connect().await {
        Ok(api) => api,
        Err(Error::NotFound(_)) => {
            connector.spawn().await?;
            connector.connect().await?
        }
        Err(error) => return Err(error),
    };
    HarnessClient::connect(api, session).await
}

/// Thin transport runner; Rust owns retry identities, replay and cursor recovery.
pub struct HarnessClient<S> {
    api: Arc<dyn HarnessWireApi>,
    handshake: wire::HandshakeResponse,
    session: ClientSession<S>,
    deliveries: Option<BoxProviderStream<'static, Result<wire::Delivery>>>,
    buffered_delivery: Option<wire::Delivery>,
    files: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
}

impl<S: ClientStateStore> HarnessClient<S> {
    /// Negotiates before sending commands. Opening a client starts no task.
    pub async fn connect(api: Arc<dyn HarnessWireApi>, session: ClientSession<S>) -> Result<Self> {
        let request = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: None,
        };
        let response = api.handshake(request.clone()).await?;
        let expected = negotiate(
            &request,
            response
                .supported
                .as_ref()
                .ok_or_else(|| Error::Conflict("handshake capabilities missing".into()))?,
        )?;
        if response != expected {
            return Err(Error::Conflict("handshake identity mismatch".into()));
        }
        Ok(Self {
            api,
            handshake: response,
            session,
            deliveries: None,
            buffered_delivery: None,
            files: None,
        })
    }

    /// Binds the filesystem reader used to validate live and replayed message references.
    /// Without read authority, content-bearing deliveries fail before cursor commit.
    pub fn with_files(
        mut self,
        files: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    ) -> Self {
        self.files = Some(files);
        self
    }

    /// Queues intent durably and publishes every admission in outbox order,
    /// including a predecessor that stops draining with an indeterminate outcome.
    pub async fn submit(
        &mut self,
        command: wire::CommandEnvelope,
        admitted: impl FnMut(wire::Admission) -> Result<()>,
    ) -> Result<()> {
        let operation_id = command
            .operation
            .as_ref()
            .ok_or_else(|| Error::Invalid("command identity missing".into()))?
            .operation_id
            .clone();
        self.queue(command)?;
        self.drain_outbox(Some(&operation_id), admitted).await
    }

    /// Queues a command without starting I/O, for offline use or ordered flushing.
    pub fn queue(&mut self, command: wire::CommandEnvelope) -> Result<()> {
        self.session.queue(command)
    }

    /// Resubmits pending envelopes in order, stopping after an indeterminate admission.
    /// The callback receives every admission, including the one that stops draining.
    pub async fn flush_outbox(
        &mut self,
        admitted: impl FnMut(wire::Admission) -> Result<()>,
    ) -> Result<()> {
        self.drain_outbox(None, admitted).await
    }

    async fn drain_outbox(
        &mut self,
        through: Option<&str>,
        mut admitted: impl FnMut(wire::Admission) -> Result<()>,
    ) -> Result<()> {
        let mut pending = Vec::new();
        for command in self.session.pending() {
            pending.push(command.clone());
            if through.is_some_and(|id| {
                command
                    .operation
                    .as_ref()
                    .is_some_and(|op| op.operation_id == id)
            }) {
                break;
            }
        }
        for command in pending {
            let admission = self.api.submit(command).await?;
            let blocked = admission.state == wire::AdmissionState::Indeterminate as i32;
            self.session
                .receive_admission(admission, |admission| admitted(admission.clone()))?;
            if blocked {
                break;
            }
        }
        Ok(())
    }

    /// Opens replay/follow at the disposable view cut, retaining the durable cursor.
    pub async fn replay(&mut self) -> Result<()> {
        self.deliveries = Some(
            self.api
                .replay(
                    self.session.replay_request(),
                    crate::wire_api::ReplayMode::Follow,
                )
                .await?,
        );
        Ok(())
    }

    /// Validates and durably publishes the next complete delivery.
    pub async fn next_delivery(&mut self) -> Result<Option<wire::Delivery>> {
        match self.read_delivery().await? {
            Some(delivery) => self.publish_delivery(delivery, |_| Ok(())).await.map(Some),
            None => Ok(None),
        }
    }

    pub(super) fn has_buffered_delivery(&self) -> bool {
        self.buffered_delivery.is_some()
    }

    pub(super) async fn read_delivery(&mut self) -> Result<Option<wire::Delivery>> {
        if let Some(delivery) = self.buffered_delivery.take() {
            return Ok(Some(delivery));
        }
        let stream = self
            .deliveries
            .as_mut()
            .ok_or_else(|| Error::Invalid("open replay before reading deliveries".into()))?;
        stream.next().await.transpose()
    }

    pub(super) async fn publish_delivery(
        &mut self,
        delivery: wire::Delivery,
        before_commit: impl FnOnce(&wire::ServerFrame) -> Result<()>,
    ) -> Result<wire::Delivery> {
        self.buffered_delivery = Some(
            self.session
                .receive_delivery(delivery, self.files.as_deref(), before_commit)
                .await?,
        );
        // Maintenance can fail after append; retain the committed frame before it runs.
        self.session.compact_if_needed()?;
        if let Some(delivery) = self.buffered_delivery.as_ref()
            && delivery
                .authority
                .as_ref()
                .and_then(|authority| self.session.cursor(authority))
                .is_some_and(|cursor| cursor.revision == delivery.through_revision)
        {
            self.api
                .acknowledge(wire::Acknowledge {
                    authority: delivery.authority.clone(),
                    generation: delivery.generation.clone(),
                    through_revision: delivery.through_revision,
                })
                .await?;
        }
        self.buffered_delivery
            .take()
            .ok_or_else(|| Error::Conflict("committed delivery disappeared".into()))
    }

    /// Subscribes to status changes independently of owner-history replay.
    pub async fn operation_updates(
        &self,
        request: wire::ObserveRequest,
    ) -> Result<BoxProviderStream<'static, Result<wire::OperationStatus>>> {
        crate::wire_api::validate_observe_request(&request)?;
        let stream = self.api.watch_operation(request.clone()).await?;
        Ok(Box::pin(stream.map(move |status| {
            let status = status?;
            validate_operation_status(&request.operation_id, request.owner.as_ref(), &status)?;
            Ok(status)
        })))
    }

    /// Observes an identity-bound operation projection.
    pub async fn observe(&self, request: wire::ObserveRequest) -> Result<wire::OperationStatus> {
        crate::wire_api::validate_observe_request(&request)?;
        let status = self.api.observe(request.clone()).await?;
        validate_operation_status(&request.operation_id, request.owner.as_ref(), &status)?;
        Ok(status)
    }

    /// Cancels with the wire protocol's durable cancellation identity.
    pub async fn cancel(&self, request: wire::CancelRequest) -> Result<wire::CancelResponse> {
        crate::wire_api::validate_cancel_request(&request)?;
        let response = self.api.cancel(request.clone()).await?;
        crate::wire_api::validate_cancel_response(&request, &response)?;
        Ok(response)
    }

    /// Shared cursor and outbox state and outbox.
    pub fn session(&self) -> &ClientSession<S> {
        &self.session
    }

    /// Negotiated service response, also emitted by JSONL runners.
    pub fn handshake(&self) -> &wire::HandshakeResponse {
        &self.handshake
    }

    /// Tracks a new aggregate without changing an existing durable replay cut.
    pub fn track(&mut self, cursor: wire::ReplayCursor) -> Result<()> {
        self.session.track(cursor)
    }

    /// Consumes the transport while retaining durable state for reconnect.
    pub fn into_session(self) -> ClientSession<S> {
        self.session
    }
}
