use super::*;

/// Filesystem publication source.
pub type PublicationStream =
    crate::BoxProviderStream<'static, Result<wire::SessionData, StreamError>>;

/// Prepared filesystem publication subscription.
pub struct PublicationSubscription {
    /// Session delivery mode.
    pub delivery: wire::SessionDelivery,
    /// Asynchronous provider open.
    pub open: crate::BoxProviderFuture<'static, Result<PublicationStream, StreamError>>,
}

/// Filesystem-owned adapter for the opaque Session payload.
pub trait PublicationProvider: crate::ProviderPlatform + 'static {
    /// Revalidates current authority immediately before delivery.
    fn revalidate(&self, encoded: &Bytes) -> Result<(), StreamError>;
    /// Prepares a subscription and continuation.
    fn subscribe(
        &self,
        encoded: Bytes,
        resume: wire::SessionCursor,
    ) -> Result<PublicationSubscription, StreamError>;
}

pub(super) fn source(
    subscription: PublicationSubscription,
    identity: wire::SessionChannel,
) -> Source {
    Box::pin(
        stream::try_unfold(
            (Some(subscription.open), None::<PublicationStream>),
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
