//! One in-memory session transport and status wait shared by the client's own
//! tests, so each test module drives the same original provider path.

use crate::{AgentClient, AgentClientError, AgentEvent, StatusRecord};
use acyclic_stream::{
    BoxProviderFuture, MemoryStream,
    session::{self, transport},
};
use futures::{StreamExt as _, future::poll_fn};
use std::sync::Arc;

pub(super) struct Endpoint(pub(super) Arc<MemoryStream>);

impl transport::Transport for Endpoint {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses<transport::Failure>, transport::Failure>>
    {
        Box::pin(async move {
            let response = session::serve(
                self.0.clone(),
                session::testing::authority(),
                "node".into(),
                session::Limits::default(),
                source.map(Ok).boxed(),
            )?;
            Ok(Box::pin(response.map(|frame| frame.map_err(Into::into)))
                as transport::Responses<transport::Failure>)
        })
    }
}

pub(super) async fn status(client: &mut AgentClient) -> Result<StatusRecord, AgentClientError> {
    loop {
        if let AgentEvent::Status { status, .. } = poll_fn(|cx| client.poll_next(cx)).await? {
            return Ok(status);
        }
    }
}
