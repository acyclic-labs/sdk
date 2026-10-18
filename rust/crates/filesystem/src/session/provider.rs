use super::*;
use acyclic_stream::session::{PublicationProvider, PublicationStream, PublicationSubscription};
use acyclic_stream::{StreamProvider, wire as session};
use futures::TryStreamExt;
use std::sync::Arc;

/// Session publication adapter for a filesystem authority provider.
pub struct Provider<P>(pub Arc<P>);

impl<P: StreamProvider> PublicationProvider for Provider<P> {
    fn subscribe(
        &self,
        encoded: Bytes,
        resume: session::SessionCursor,
    ) -> Result<PublicationSubscription, StreamError> {
        let subscription = Subscription::decode(&encoded)?;
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
        let provider = self.0.clone();
        Ok(PublicationSubscription {
            delivery,
            open: Box::pin(async move {
                let path = crate::distributed::authority_records_path(subscription.volume_id)
                    .map_err(|_| StreamError::InvalidArgument)?;
                let records = if let Some(anchor) = anchor {
                    provider
                        .follow_checked(path, anchor.next, anchor.preceding_commit_id)
                        .await?
                } else {
                    provider
                        .follow(path, subscription.start_next.max(1))
                        .await?
                };
                let output = records.try_filter_map(move |record| {
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
