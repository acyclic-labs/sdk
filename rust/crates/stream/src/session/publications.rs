use super::*;

/// Filesystem publication source.
pub type PublicationStream =
    crate::BoxProviderStream<'static, Result<wire::SessionData, StreamError>>;

/// Current authorization query for one validated publication scope.
#[cfg(not(target_arch = "wasm32"))]
pub type PublicationRevalidation = Arc<dyn Fn() -> Result<(), StreamError> + Send + Sync>;
/// Current browser authorization query for one validated publication scope.
#[cfg(target_arch = "wasm32")]
pub type PublicationRevalidation = Arc<dyn Fn() -> Result<(), StreamError>>;

/// Prepared filesystem publication subscription.
pub struct PublicationSubscription {
    /// Session delivery mode.
    pub delivery: wire::SessionDelivery,
    /// Required authorization query at the delivery boundary.
    pub revalidate: PublicationRevalidation,
    /// Independent terminal observation; polled even with no data credit.
    pub terminal: crate::BoxProviderFuture<'static, StreamError>,
    /// Asynchronous provider open.
    pub open: crate::BoxProviderFuture<'static, Result<PublicationStream, StreamError>>,
}

/// Filesystem-owned adapter for the opaque Session payload.
pub trait PublicationProvider: crate::ProviderPlatform + 'static {
    /// Prepares a subscription and continuation.
    fn subscribe(
        &self,
        encoded: Bytes,
        resume: wire::SessionCursor,
    ) -> Result<PublicationSubscription, StreamError>;
}

pub(super) fn source(
    open: crate::BoxProviderFuture<'static, Result<PublicationStream, StreamError>>,
    identity: wire::SessionChannel,
) -> Source {
    Box::pin(
        stream::try_unfold(
            (Some(open), None::<PublicationStream>),
            |(open, mut publications)| async move {
                if let Some(open) = open {
                    publications = Some(open.await?);
                    return Ok(Some((Event::Opened(None), (None, publications))));
                }
                let data = publications
                    .as_mut()
                    .ok_or(StreamError::Unavailable)?
                    .next()
                    .await
                    .ok_or(StreamError::Unavailable)??;
                Ok(Some((Event::Publication(data), (None, publications))))
            },
        )
        .map(move |event| (identity, event)),
    )
}

pub(super) fn terminal(
    terminal: crate::BoxProviderFuture<'static, StreamError>,
    identity: wire::SessionChannel,
) -> Source {
    Box::pin(stream::once(async move { (identity, Err(terminal.await)) }))
}
