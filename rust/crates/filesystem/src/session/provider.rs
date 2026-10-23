use super::*;
use acyclic_stream::session::{PublicationProvider, PublicationStream, PublicationSubscription};
use acyclic_stream::{StreamProvider, wire as session};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

#[cfg(test)]
#[cfg(all(feature = "memory", not(acyclic_loom)))]
mod tests;
#[cfg(test)]
thread_local! {
    static COPY_SUBSCRIPTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Session publication adapter for a filesystem authority provider.
pub struct Provider<P, A> {
    stream: Arc<P>,
    authorize: Arc<A>,
}
impl<P, A> Provider<P, A> {
    /// Binds a provider and the host's current authorization query.
    pub fn new(stream: Arc<P>, authorize: A) -> Self {
        Self {
            stream,
            authorize: Arc::new(authorize),
        }
    }
}

impl<P: StreamProvider, A> PublicationProvider for Provider<P, A>
where
    A: Fn(&Subscription) -> Result<(), StreamError> + Send + Sync + 'static,
{
    fn subscribe(
        &self,
        encoded: Bytes,
        resume: session::SessionCursor,
    ) -> Result<PublicationSubscription, StreamError> {
        let subscription = Arc::new(Subscription::decode(&encoded)?);
        let authorize = self.authorize.clone();
        let anchor = if resume.opaque.is_empty() {
            if resume.next != 0 {
                return Err(StreamError::InvalidArgument);
            }
            None
        } else {
            let cursor = Cursor::decode(&resume.opaque)?;
            if cursor.subscription != *subscription || cursor.next != resume.next {
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
        let scope = subscription.clone();
        let policy = authorize.clone();
        let terminal_provider = provider.clone();
        let terminal_scope = subscription.clone();
        let terminal_policy = authorize.clone();
        let resumed = anchor.is_some();
        Ok(PublicationSubscription {
            delivery,
            terminal: Box::pin(async move {
                match watch_terminal(terminal_provider, terminal_scope, terminal_policy, resumed)
                    .await
                {
                    Err(error) => error,
                    Ok(()) => StreamError::Unavailable,
                }
            }),
            revalidate: Arc::new(move || policy(&scope)),
            open: Box::pin(async move {
                authorize(&subscription)?;
                let path = crate::distributed::authority_records_path(subscription.volume_id)
                    .map_err(|_| StreamError::InvalidArgument)?;
                let records = if let Some(anchor) = anchor {
                    #[cfg(test)]
                    acyclic_test_support::differential::record(("checked_follow", anchor.next));
                    provider
                        .follow_checked(
                            path,
                            anchor.next,
                            anchor.preceding_commit_id,
                            Default::default(),
                        )
                        .await
                        .map_err(history_error)?
                } else {
                    let tail = provider.tail(path.clone()).await?;
                    let mut head = provider
                        .read(acyclic_stream::ReadRequest {
                            path: path.clone(),
                            from: tail.checked_sub(1).ok_or(StreamError::PrefixNotRetained)?,
                            limit: 1,
                        })
                        .await
                        .map_err(history_error)?;
                    let record = head
                        .next()
                        .await
                        .ok_or(StreamError::PrefixNotRetained)?
                        .map_err(history_error)?;
                    check_record_deletion(&subscription, &record)?;
                    let from = subscription.start_next.max(1);
                    if from > tail {
                        return Err(StreamError::OutOfRange);
                    }
                    let mut preceding = provider
                        .read(acyclic_stream::ReadRequest {
                            path: path.clone(),
                            from: from - 1,
                            limit: 1,
                        })
                        .await
                        .map_err(history_error)?;
                    let preceding = preceding
                        .next()
                        .await
                        .ok_or(StreamError::PrefixNotRetained)?
                        .map_err(history_error)?;
                    check_record_deletion(&subscription, &preceding)?;
                    provider
                        .follow_checked(path, from, preceding.commit_id, Default::default())
                        .await
                        .map_err(history_error)?
                };
                let output = records
                    .map_err(history_error)
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
    #[cfg(test)]
    if COPY_SUBSCRIPTION.get() {
        std::hint::black_box(subscription.clone());
    }
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
    #[cfg(test)]
    acyclic_test_support::differential::record(("publication", record.sequence));
    let next = record
        .sequence
        .checked_add(1)
        .ok_or(StreamError::OutOfRange)?;
    let publication = Publication {
        generation,
        workspace_name: subscription.workspace_name.clone(),
        sequence: Sequence::new(record.sequence),
        commit_id: record.commit_id,
    };
    Ok(Some(session::SessionData {
        after: Some(session::SessionCursor {
            next,
            opaque: Cursor::encode_parts(subscription, next, record.commit_id)?,
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

fn check_record_deletion(
    subscription: &Subscription,
    record: &acyclic_stream::Record,
) -> Result<(), StreamError> {
    // Offset zero is the authority genesis header, not a durable fs fact.
    if record.sequence == 0 {
        return Ok(());
    }
    let durable = crate::StreamsDurableRecord::decode_borrowed(
        &record.value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .map_err(|_| StreamError::InvalidArgument)?;
    if durable.sequence.get() != record.sequence {
        return Err(StreamError::InvalidArgument);
    }
    check_deletion(subscription, durable.payload)
}

async fn watch_terminal<P: StreamProvider, A>(
    provider: Arc<P>,
    subscription: Arc<Subscription>,
    authorize: Arc<A>,
    resumed: bool,
) -> Result<(), StreamError>
where
    A: Fn(&Subscription) -> Result<(), StreamError> + Send + Sync + 'static,
{
    authorize(&subscription)?;
    let path = crate::distributed::authority_records_path(subscription.volume_id)
        .map_err(|_| StreamError::InvalidArgument)?;
    let tail = provider
        .tail(path.clone())
        .await
        .map_err(|error| if resumed { history_error(error) } else { error })?;
    let mut head = provider
        .read(acyclic_stream::ReadRequest {
            path: path.clone(),
            from: tail.checked_sub(1).ok_or(StreamError::PrefixNotRetained)?,
            limit: 1,
        })
        .await
        .map_err(history_error)?;
    let head = head
        .next()
        .await
        .ok_or(StreamError::PrefixNotRetained)?
        .map_err(history_error)?;
    check_record_deletion(&subscription, &head)?;
    let mut records = provider
        .follow_checked(path, tail, head.commit_id, Default::default())
        .await
        .map_err(history_error)?;
    while let Some(record) = records.next().await {
        check_record_deletion(&subscription, &record.map_err(history_error)?)?;
    }
    Err(StreamError::Unavailable)
}
