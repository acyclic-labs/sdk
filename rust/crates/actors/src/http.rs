//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
use prost::Message;
use prost_reflect::DescriptorPool;
use reqwest::{
    Client as Transport, Url,
    header::{AUTHORIZATION, HeaderValue},
};

/// HTTP configuration, encoding, or remote service failure.
pub type Error = acyclic_workers::http::Error<wire::Error>;

/// Typed HTTP operations with bearer authentication and bounded responses.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    authorization: HeaderValue,
    maximum: usize,
    descriptors: DescriptorPool,
}

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        let (transport, endpoint, authorization) = acyclic_workers::http::configure::<wire::Error>(endpoint, token, maximum_response_bytes)?;
        Ok(Self {
            transport,
            endpoint,
            authorization,
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    #[tracing::instrument(
            name = "acyclic.actors.http.call",
            level = "info",
            skip_all,
            fields(
                route = route,
                http.status = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error.kind = tracing::field::Empty,
            )
    )]
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &'static str,
        input: &str,
        output: &str,
        request: &I,
    ) -> Result<O, Error> {
        let result = async {
            let body = acyclic_workers::http::encode::<wire::Error>(&self.descriptors, input, request)?;
            let mut response = self
                .transport
                .post(
                    self.endpoint
                        .join(route)
                        .map_err(|_| Error::InvalidArgument)?,
                )
                .header(AUTHORIZATION, self.authorization.clone())
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await?;
            let status = response.status();
            tracing::Span::current().record("http.status", status.as_u16());
            let bytes = acyclic_workers::http::response_body(&mut response, self.maximum)
                .await?.ok_or(Error::ResponseTooLarge)?;
            if !status.is_success() {
                let detail = acyclic_workers::http::decode::<wire::Error, wire::Error>(&self.descriptors, "acyclic.actors.v1.Error", &bytes)
                    .ok()
                    .filter(|detail| {
                        wire::ErrorCode::try_from(detail.code)
                            .is_ok_and(|code| code != wire::ErrorCode::Unspecified)
                    });
                return Err(Error::Service {
                    status: status.as_u16(),
                    detail,
                    raw_details: bytes,
                });
            }
            acyclic_workers::http::decode(&self.descriptors, output, &bytes)
        }.await;
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span.record("outcome", "err").record("error.kind", error.kind()),
        };
        result
    }

    /// Execute the canonical `CreateActor` operation.
    pub async fn create_actor(
        &self,
        request: &wire::CreateActorRequest,
    ) -> Result<wire::CreateActorResponse, Error> {
        self.call(
            HTTP_ROUTES.first().ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.CreateActorRequest",
            "acyclic.actors.v1.CreateActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `UpdateActor` operation.
    pub async fn update_actor(
        &self,
        request: &wire::UpdateActorRequest,
    ) -> Result<wire::UpdateActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(1).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.UpdateActorRequest",
            "acyclic.actors.v1.UpdateActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InspectActor` operation.
    pub async fn inspect_actor(
        &self,
        request: &wire::InspectActorRequest,
    ) -> Result<wire::InspectActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(2).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.InspectActorRequest",
            "acyclic.actors.v1.InspectActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `AddSubscription` operation.
    pub async fn add_subscription(
        &self,
        request: &wire::AddSubscriptionRequest,
    ) -> Result<wire::AddSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(3).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.AddSubscriptionRequest",
            "acyclic.actors.v1.AddSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `RemoveSubscription` operation.
    pub async fn remove_subscription(
        &self,
        request: &wire::RemoveSubscriptionRequest,
    ) -> Result<wire::RemoveSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(4).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.RemoveSubscriptionRequest",
            "acyclic.actors.v1.RemoveSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `ResumeSubscription` operation.
    pub async fn resume_subscription(
        &self,
        request: &wire::ResumeSubscriptionRequest,
    ) -> Result<wire::ResumeSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(5).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.ResumeSubscriptionRequest",
            "acyclic.actors.v1.ResumeSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `CheckpointActor` operation.
    pub async fn checkpoint_actor(
        &self,
        request: &wire::CheckpointActorRequest,
    ) -> Result<wire::CheckpointActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(6).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.CheckpointActorRequest",
            "acyclic.actors.v1.CheckpointActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeActor` operation.
    pub async fn invoke_actor(
        &self,
        request: &wire::InvokeActorRequest,
    ) -> Result<wire::InvokeActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(7).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.InvokeActorRequest",
            "acyclic.actors.v1.InvokeActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `DeleteActor` operation.
    pub async fn delete_actor(
        &self,
        request: &wire::DeleteActorRequest,
    ) -> Result<wire::DeleteActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(8).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.DeleteActorRequest",
            "acyclic.actors.v1.DeleteActorResponse",
            request,
        )
        .await
    }
}
