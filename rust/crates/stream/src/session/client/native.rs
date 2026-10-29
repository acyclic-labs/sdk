//! gRPC I/O for the same pool used by browser Streams sessions.
use super::*;
use crate::grpc::{self, ChannelConnector as Connect};
use acyclic_auth::credential::{CredentialAction, ResolvedCredential};
use futures::{
    channel::oneshot,
    future::{Either, select},
};
use std::future::Future as _;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::Channel as TransportChannel,
};

struct Grpc {
    channel: TransportChannel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
}

// A concrete input type avoids tonic's higher-ranked bound on an erased stream.
struct Input {
    source: transport::Requests,
    lease: Option<ResolvedCredential>,
    admission: Option<(wire::SessionFrame, CredentialAction<()>)>,
    failed: Option<oneshot::Sender<StreamError>>,
    ended: bool,
}

// One local admission-failure event bridges the infallible gRPC request body to
// its response. Normal body teardown leaves the maintained response operation
// as the event producer; it must still report its real status/trailers.
async fn input_result<T>(
    operation: impl std::future::Future<Output = T>,
    failed: &mut Option<oneshot::Receiver<StreamError>>,
) -> Result<T, StreamError> {
    futures::pin_mut!(operation);
    if let Some(receive) = failed {
        match select(receive, operation.as_mut()).await {
            Either::Left((Ok(error), _)) => return Err(error),
            Either::Left((Err(_), _)) => *failed = None,
            Either::Right((result, _)) => return Ok(result),
        }
    }
    Ok(operation.await)
}

async fn session_error(
    error: tonic::Status,
    lease: Option<&acyclic_auth::credential::ResolvedCredential>,
) -> StreamError {
    if error.code() == tonic::Code::Unauthenticated && std::error::Error::source(&error).is_none() {
        if let Some(lease) = lease {
            if lease.reject().await.is_err() {
                return StreamError::AccessDenied;
            }
        }
    }
    if grpc::retryable(&error) {
        StreamError::Unavailable
    } else {
        grpc::status(&error)
    }
}
impl futures::Stream for Input {
    type Item = wire::SessionFrame;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.ended {
            return std::task::Poll::Ready(None);
        }
        loop {
            if let Some((_, check)) = &mut self.admission {
                let result = futures::ready!(std::pin::Pin::new(check).poll(cx));
                let (frame, _) = self.admission.take().expect("admission owns frame");
                if result.is_ok() {
                    return std::task::Poll::Ready(Some(frame));
                }
                self.ended = true;
                if let Some(failed) = self.failed.take() {
                    let _ = failed.send(StreamError::AccessDenied);
                }
                return std::task::Poll::Ready(None);
            }
            let Some(frame) = futures::ready!(self.source.as_mut().poll_next(cx)) else {
                self.ended = true;
                self.failed.take();
                return std::task::Poll::Ready(None);
            };
            if let Some(lease) = &self.lease {
                self.admission = Some((frame, lease.check()));
            } else {
                return std::task::Poll::Ready(Some(frame));
            }
        }
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
            let mut changed = match &self.credential {
                Some(provider) => provider
                    .watch()
                    .await
                    .map_err(|_| StreamError::AccessDenied)?,
                None => None,
            };
            let (authorization, lease) = transport::credential_event(
                &mut changed,
                grpc::request_authorization(self.authorization.as_ref(), self.credential.as_ref()),
            )
            .await?
            .map_err(|error| grpc::status(&error))?;
            let (failed, mut failure) = if lease.is_some() {
                let (send, receive) = oneshot::channel();
                (Some(send), Some(receive))
            } else {
                (None, None)
            };
            let mut request = Request::new(Input {
                source,
                lease: lease.clone(),
                admission: None,
                failed,
                ended: false,
            });
            if let Some(lease) = &lease {
                request.extensions_mut().insert(lease.clone());
            }
            request
                .metadata_mut()
                .insert("authorization", authorization);
            let mut service = wire::stream_session_service_client::StreamSessionServiceClient::new(
                acyclic_auth::channel::guard(self.channel.clone()),
            );
            let response = transport::credential_event(&mut changed, async {
                match input_result(service.session(request), &mut failure).await? {
                    Ok(response) => Ok(response.into_inner()),
                    Err(error) => Err(session_error(error, lease.as_ref()).await),
                }
            })
            .await??;
            Ok(Box::pin(futures::stream::try_unfold(
                (response, lease, failure, changed),
                |(mut response, lease, mut failure, mut changed)| async move {
                    let value = transport::credential_event(&mut changed, async {
                        let value = match input_result(response.message(), &mut failure).await? {
                            Ok(value) => value,
                            Err(error) => return Err(session_error(error, lease.as_ref()).await),
                        };
                        if let Some(lease) = &lease {
                            lease.check().await.map_err(|_| StreamError::AccessDenied)?;
                        }
                        Ok(value)
                    })
                    .await??;
                    Ok(value.map(|value| (value, (response, lease, failure, changed))))
                },
            )) as Response)
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
        revisions: crate::query_sync::Arc<crate::RevisionTracker>,
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
        Self::connector(channel, authorization, credential, connect, revisions)
    }

    pub(super) fn with_connector(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        connect: Connect,
    ) -> Self {
        Self::connector(
            channel,
            authorization,
            credential,
            connect,
            Default::default(),
        )
    }

    pub(crate) fn connector(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        connect: Connect,
        revisions: crate::query_sync::Arc<crate::RevisionTracker>,
    ) -> Self {
        let home = endpoint(channel, authorization.clone(), credential.clone());
        let connect: std::sync::Arc<transport::Connect> = std::sync::Arc::new(move |node| {
            let connect = connect.clone();
            let authorization = authorization.clone();
            let credential = credential.clone();
            Box::pin(async move { Ok(endpoint(connect(node).await?, authorization, credential)) })
        });
        Self::tracking(
            home,
            connect,
            std::sync::Arc::new(crate::runtime::PlatformExecutor),
            revisions,
        )
    }
}
