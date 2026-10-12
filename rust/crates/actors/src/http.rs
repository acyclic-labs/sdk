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

macro_rules! operations {
    ($(($method:ident, $index:literal, $input:ident, $output:ident)),+ $(,)?) => {
        $(/// Executes the canonical HTTP operation.
        pub async fn $method(&self, request: &wire::$input) -> Result<wire::$output, Error> {
            let route = HTTP_ROUTES.get($index).ok_or(Error::InvalidArgument)?.1;
            self.call(
                route,
                concat!("acyclic.actors.v1.", stringify!($input)),
                concat!("acyclic.actors.v1.", stringify!($output)),
                request,
            ).await
        })+
    };
}

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        let (transport, endpoint, authorization) = acyclic_workers::http::configure::<wire::Error>(
            endpoint,
            token,
            maximum_response_bytes,
        )?;
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
            let body =
                acyclic_workers::http::encode::<wire::Error>(&self.descriptors, input, request)?;
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
                .await?
                .ok_or(Error::ResponseTooLarge)?;
            if !status.is_success() {
                let detail = acyclic_workers::http::decode::<wire::Error, wire::Error>(
                    &self.descriptors,
                    "acyclic.actors.v1.Error",
                    &bytes,
                )
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
        }
        .await;
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span
                .record("outcome", "err")
                .record("error.kind", error.kind()),
        };
        result
    }

    operations! {
        (create_actor, 0, CreateActorRequest, CreateActorResponse),
        (update_actor, 1, UpdateActorRequest, UpdateActorResponse),
        (inspect_actor, 2, InspectActorRequest, InspectActorResponse),
        (add_subscription, 3, AddSubscriptionRequest, AddSubscriptionResponse),
        (remove_subscription, 4, RemoveSubscriptionRequest, RemoveSubscriptionResponse),
        (resume_subscription, 5, ResumeSubscriptionRequest, ResumeSubscriptionResponse),
        (checkpoint_actor, 6, CheckpointActorRequest, CheckpointActorResponse),
        (invoke_actor, 7, InvokeActorRequest, InvokeActorResponse),
        (delete_actor, 8, DeleteActorRequest, DeleteActorResponse),
    }
}
