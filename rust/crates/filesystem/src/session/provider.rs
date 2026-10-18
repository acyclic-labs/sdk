use super::*;
use acyclic_stream::session::{PublicationProvider, PublicationStream, PublicationSubscription};
use acyclic_stream::{StreamProvider, wire as session};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

/// Session publication adapter for a filesystem authority provider.
pub struct Provider<P, A> {
    stream: Arc<P>,
    authorize: A,
}
impl<P, A> Provider<P, A> {
    /// Binds a provider and the host's current authorization query.
    pub fn new(stream: Arc<P>, authorize: A) -> Self {
        Self { stream, authorize }
    }
}

impl<P: StreamProvider, A> PublicationProvider for Provider<P, A>
where
    A: Fn(&Subscription) -> Result<(), StreamError> + Send + Sync + 'static,
{
    fn revalidate(&self, encoded: &Bytes) -> Result<(), StreamError> {
        (self.authorize)(&Subscription::decode(encoded)?)
    }
    fn subscribe(
        &self,
        encoded: Bytes,
        resume: session::SessionCursor,
    ) -> Result<PublicationSubscription, StreamError> {
        let subscription = Subscription::decode(&encoded)?;
        let authorized = (self.authorize)(&subscription);
        let anchor = if resume.opaque.is_empty() {
            if resume.next != 0 {
                return Err(StreamError::InvalidArgument);
            }
            None
        } else {
            let cursor = Cursor::decode(&resume.opaque)?;
            if cursor.subscription != subscription || cursor.next != resume.next {
                return Err(StreamError::InvalidArgument);
            }
            Some(cursor)
        };
        let delivery = match subscription.delivery {
            wire::SessionFsDelivery::All => session::SessionDelivery::All,
            wire::SessionFsDelivery::Latest => session::SessionDelivery::Latest,
            _ => return Err(StreamError::InvalidArgument),
        };
        let provider = self.stream.clone();
        Ok(PublicationSubscription {
            delivery,
            open: Box::pin(async move {
                authorized?;
                let path = crate::distributed::authority_records_path(subscription.volume_id)
                    .map_err(|_| StreamError::InvalidArgument)?;
                let checked = anchor.is_some();
                let records = if let Some(anchor) = anchor {
                    provider
                        .follow_checked(path, anchor.next, anchor.preceding_commit_id)
                        .await
                        .map_err(history_error)?
                } else {
                    let records = provider
                        .follow(path.clone(), subscription.start_next.max(1))
                        .await?;
                    let tail = provider.tail(path.clone()).await?;
                    let mut head = provider
                        .read(acyclic_stream::ReadRequest {
                            path,
                            from: tail.checked_sub(1).ok_or(StreamError::PrefixNotRetained)?,
                            limit: 1,
                        })
                        .await?;
                    let record = head.next().await.ok_or(StreamError::PrefixNotRetained)??;
                    let durable = crate::streams_record::StreamsDurableRecord::decode_borrowed(
                        &record.value,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                    )
                    .map_err(|_| StreamError::InvalidArgument)?;
                    check_deletion(&subscription, durable.payload)?;
                    records
                };
                let output = records
                    .map_err(move |error| if checked { history_error(error) } else { error })
                    .try_filter_map(move |record| {
                        futures::future::ready(publication(&subscription, record))
                    });
                Ok(Box::pin(output) as PublicationStream)
            }),
        })
    }
}

fn publication(
    subscription: &Subscription,
    record: acyclic_stream::Record,
) -> Result<Option<session::SessionData>, StreamError> {
    let durable = crate::streams_record::StreamsDurableRecord::decode_borrowed(
        &record.value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .map_err(|_| StreamError::InvalidArgument)?;
    if durable.sequence.get() != record.sequence {
        return Err(StreamError::InvalidArgument);
    }
    check_deletion(subscription, durable.payload)?;
    let generation = if record.sequence == 1 {
        let created = crate::kernel::decode_volume_created::<crate::FsError>(
            durable.payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut |_| Ok(()),
        )
        .map_err(|_| StreamError::InvalidArgument)?;
        PublishedGeneration {
            volume_id: created.volume_id,
            generation_root: created.initial_generation_root,
        }
    } else {
        if !durable
            .payload
            .starts_with(b"acyclic-fs-publish-generation-v1\0")
        {
            return Ok(None);
        }
        crate::kernel::decode_published_generation::<crate::FsError>(
            durable.payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            |_| Ok(()),
        )
        .map_err(|_| StreamError::InvalidArgument)?
    };
    if generation.volume_id != subscription.volume_id {
        return Err(StreamError::InvalidArgument);
    }
    let next = record
        .sequence
        .checked_add(1)
        .ok_or(StreamError::OutOfRange)?;
    let cursor = Cursor {
        subscription: subscription.clone(),
        next,
        preceding_commit_id: record.commit_id,
    };
    let publication = Publication {
        generation,
        workspace_name: subscription.workspace_name.clone(),
        sequence: Sequence::new(record.sequence),
        commit_id: record.commit_id,
    };
    Ok(Some(session::SessionData {
        after: Some(session::SessionCursor {
            next,
            opaque: cursor.encode()?,
        }),
        accepted: next,
        durable: 0,
        item: Some(session::session_data::Item::FsPublication(
            publication.encode()?,
        )),
    }))
}

fn history_error(error: StreamError) -> StreamError {
    match error {
        StreamError::NotFound | StreamError::OutOfRange => StreamError::PrefixNotRetained,
        other => other,
    }
}

fn check_deletion(subscription: &Subscription, payload: &[u8]) -> Result<(), StreamError> {
    if payload.starts_with(b"acyclic-fs-workspace-deleted-v1\0") {
        let deleted = crate::kernel::decode_workspace_deleted(
            payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
        )
        .map_err(|_| StreamError::InvalidArgument)?;
        return Err(if deleted == subscription.volume_id {
            StreamError::NotFound
        } else {
            StreamError::InvalidArgument
        });
    }
    Ok(())
}
