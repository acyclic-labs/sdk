//! Application decisions use the canonical session cursor and credit owner.
use super::*;

/// A follow whose replay cursor advances only after an application decision.
/// At most one delivery is outstanding. Its credit remains in custody until
/// completion or abandonment, independently of the application checkpoint.
pub struct ApplicationSubscription {
    cursor: Cursor,
    subscription: wire::SessionSubscription,
    completion: Option<oneshot::Receiver<Result<(), StreamError>>>,
}

/// Custody of one application delivery. Persist the consumer's effects and
/// checkpoint before calling [`Self::commit`], or explicitly [`Self::discard`]
/// the item. Neither receiving a frame nor returning its credit completes it.
/// Dropping an undecided delivery ends the interest without advancing its cut.
#[must_use = "commit or discard after the durable application decision"]
pub struct ApplicationDelivery {
    frame: wire::SessionFrame,
    checkpoint: wire::SessionSubscription,
    before: Option<wire::SessionFrame>,
    sequence: u64,
    generation: u64,
    progress: Arc<Progress>,
    removed: Arc<signal::Signal<u64>>,
    id: u64,
    credit: Option<Credit>,
    completion: Option<oneshot::Sender<Result<(), StreamError>>>,
}

impl Client {
    /// Opens application-owned ALL or LATEST delivery on the existing pool.
    /// The checkpoint is a v1 subscription, containing only a credential slot,
    /// never credential bytes. Resume it after recovering the durable consumer
    /// decision; a crash before that decision replays the uncommitted item.
    pub async fn deliveries(
        &self,
        subscription: wire::SessionSubscription,
    ) -> Result<ApplicationSubscription, StreamError> {
        let (kind, resume, filter, credential) = subscription_parts(subscription.clone())?;
        let cursor = self
            .open_cursor(kind, resume, true, true, filter, credential)
            .await?;
        if matches!(&subscription.kind, Some(Kind::Follow(follow)) if follow.delivery == wire::SessionDelivery::All as i32 && follow.key.is_none())
        {
            cursor.progress.track_records()?;
        }
        Ok(ApplicationSubscription {
            cursor,
            subscription,
            completion: None,
        })
    }
}

impl ApplicationSubscription {
    /// Returns the next delivery, waiting on the preceding application's
    /// decision if it is still outstanding. This wait does not poll the peer.
    pub async fn next(&mut self) -> Result<Option<ApplicationDelivery>, StreamError> {
        if let Some(completion) = self.completion.as_mut() {
            let result = completion.await.map_err(|_| StreamError::Unavailable);
            self.completion = None;
            result??;
        }
        let Some(delivery) = self.cursor.receive().await? else {
            return Ok(None);
        };
        let Delivery {
            before,
            item,
            sequence,
            generation,
            credit,
        } = delivery;
        let Received::Frame(frame) = item else {
            return Err(StreamError::InvalidResponse);
        };
        let (after, preceding) = match &frame.body {
            Some(Body::Data(data)) => (
                data.after.clone(),
                match &data.item {
                    Some(Item::Record(record)) => record.commit_id.clone(),
                    _ => Bytes::new(),
                },
            ),
            Some(Body::Watermark(mark)) => (mark.after.clone(), mark.preceding_commit_id.clone()),
            _ => return Err(StreamError::InvalidResponse),
        };
        let mut checkpoint = self.checkpoint()?;
        if let Some(after) = after {
            checkpoint.resume = Some(after);
            if let Some(Kind::Follow(follow)) = &mut checkpoint.kind {
                follow.preceding_commit_id = if follow.delivery == wire::SessionDelivery::All as i32
                    && follow.key.is_none()
                {
                    preceding
                } else {
                    Bytes::new()
                };
            }
        }
        let (complete, completion) = oneshot::channel();
        self.completion = Some(completion);
        Ok(Some(ApplicationDelivery {
            frame,
            checkpoint,
            before,
            sequence,
            generation,
            credit: Some(credit),
            progress: Arc::clone(&self.cursor.progress),
            removed: Arc::clone(&self.cursor.client.pool.removed),
            id: self.cursor.id,
            completion: Some(complete),
        }))
    }

    /// Last explicitly decided cut. An outstanding or abandoned delivery is
    /// absent from it. Store the proposed frame's cursor with the consumer's
    /// effects atomically, then commit its handle; this accessor alone supplies
    /// no durability. Contiguous ALL checkpoints retain their preceding commit
    /// fence; indexed reads retain their sparse sequence and exact key instead.
    pub fn checkpoint(&self) -> Result<wire::SessionSubscription, StreamError> {
        let position = self.cursor.progress.subscription()?;
        let mut subscription = self.subscription.clone();
        subscription.resume = Some(wire::SessionCursor {
            next: position.next,
            opaque: position.opaque.into(),
        });
        if let Some(Kind::Follow(follow)) = &mut subscription.kind {
            follow.preceding_commit_id = position
                .preceding
                .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(&id));
        }
        Ok(subscription)
    }

    /// An owned stream with the same explicit-decision backpressure as `next`.
    pub fn into_stream(
        self,
    ) -> BoxProviderStream<'static, Result<ApplicationDelivery, StreamError>> {
        super::super::runtime::boxed(stream::try_unfold(self, |mut source| async move {
            Ok(source.next().await?.map(|delivery| (delivery, source)))
        }))
    }
}

impl ApplicationDelivery {
    /// The canonical received frame, including its proposed cursor.
    #[must_use]
    pub fn frame(&self) -> &wire::SessionFrame {
        &self.frame
    }

    /// A filtered prefix that precedes this frame at the same delivery cut.
    #[must_use]
    pub fn before(&self) -> Option<&wire::SessionFrame> {
        self.before.as_ref()
    }

    /// Proposed v1 checkpoint to persist atomically with the consumer's durable
    /// effects or discard decision, before completing this handle.
    #[must_use]
    pub fn checkpoint(&self) -> &wire::SessionSubscription {
        &self.checkpoint
    }

    /// Records the already durable application decision and releases custody.
    /// False means a reconnect fenced this delivery: its idempotent effects may
    /// already exist, but the current generation must replay and decide it.
    pub fn commit(mut self) -> Result<bool, StreamError> {
        let result = (|| {
            if self.progress.cancelled() {
                return Ok(false);
            }
            if let Some(before) = &self.before {
                consume_frame(&self.progress, self.generation, self.sequence, before)?;
            }
            consume_frame(&self.progress, self.generation, self.sequence, &self.frame)
        })();
        if result.is_ok() {
            drop(self.credit.take());
            if let Some(completion) = self.completion.take() {
                let _ = completion.send(Ok(()));
            }
        }
        result
    }

    /// Records an explicit terminal decision to skip the item. It advances the
    /// same replay cut as commit, without asserting that effects were applied.
    pub fn discard(self) -> Result<bool, StreamError> {
        self.commit()
    }
}

impl Drop for ApplicationDelivery {
    fn drop(&mut self) {
        if let Some(completion) = self.completion.take() {
            self.progress.cancel();
            let _ = self.removed.send(self.id);
            let _ = completion.send(Err(StreamError::Unavailable));
        }
    }
}
