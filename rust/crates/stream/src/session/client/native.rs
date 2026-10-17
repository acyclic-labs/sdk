//! gRPC I/O for the same pool used by browser Streams sessions.
use super::*;
use crate::grpc;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::Channel as TransportChannel,
};

pub(super) type Connect = std::sync::Arc<
    dyn Fn(
            wire::SessionNode,
        ) -> futures::future::BoxFuture<'static, Result<TransportChannel, StreamError>>
        + Send
        + Sync,
>;

struct Grpc {
    channel: TransportChannel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
}

// A concrete input type avoids tonic's higher-ranked bound on an erased stream.
struct Input(transport::Requests);
impl futures::Stream for Input {
    type Item = wire::SessionFrame;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

pub(super) fn endpoint(
    channel: TransportChannel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
) -> Endpoint {
    std::sync::Arc::new(Grpc {
        channel,
        authorization,
        credential,
    })
}

impl transport::Transport for Grpc {
    fn session(
        &self,
        source: transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<Response, StreamError>> {
        Box::pin(async move {
            let authorization =
                grpc::request_authorization(self.authorization.as_ref(), self.credential.as_ref())
                    .await
                    .map_err(|error| grpc::status(&error))?;
            let mut request = Request::new(Input(source));
            request
                .metadata_mut()
                .insert("authorization", authorization);
            let response = wire::stream_session_service_client::StreamSessionServiceClient::new(
                self.channel.clone(),
            )
            .session(request)
            .await
            .map_err(|error| grpc::status(&error))?
            .into_inner();
            Ok(Box::pin(response.map(|result| {
                result.map_err(|error| {
                    if grpc::retryable(&error) {
                        StreamError::Unavailable
                    } else {
                        grpc::status(&error)
                    }
                })
            })) as Response)
        })
    }
}

impl Client {
    pub(crate) fn open(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        ca: Option<std::sync::Arc<[u8]>>,
        verifier: Option<std::sync::Arc<dyn rustls::client::danger::ServerCertVerifier>>,
    ) -> Self {
        let connect: Connect = std::sync::Arc::new(move |node| {
            let ca = ca.clone();
            let verifier = verifier.clone();
            Box::pin(async move {
                match verifier {
                    Some(verifier) => grpc::channel_with_verifier(&node.endpoint, verifier).await,
                    None => grpc::channel_tls(&node.endpoint, ca.as_deref()).await,
                }
                .map_err(|_| StreamError::Unavailable)
            })
        });
        Self::with_connector(channel, authorization, credential, connect)
    }

    pub(super) fn with_connector(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        connect: Connect,
    ) -> Self {
        let home = endpoint(channel, authorization.clone(), credential.clone());
        let connect: std::sync::Arc<transport::Connect> = std::sync::Arc::new(move |node| {
            let connect = connect.clone();
            let authorization = authorization.clone();
            let credential = credential.clone();
            Box::pin(async move { Ok(endpoint(connect(node).await?, authorization, credential)) })
        });
        Self::with_transport(
            home,
            connect,
            std::sync::Arc::new(super::super::runtime::PlatformExecutor),
        )
    }
}
