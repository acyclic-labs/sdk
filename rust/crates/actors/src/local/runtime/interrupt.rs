//! The actor's subscription follow observes interrupts while its handler runs.
//! Lookahead never commits or buffers inputs: the next delivery re-reads its
//! original committed cursor, including records passed while finding a control.
use super::*;
use crate::guest::CancellationToken;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) async fn interrupt_follows(
        &self,
        inner: &mut Inner,
        state: &State,
        trigger: &Trigger,
    ) -> Result<(), Error> {
        for (id, subscription) in state.subscriptions() {
            if !self
                .handler
                .manifest()
                .interrupts
                .iter()
                .any(|filter| &filter.subscription == id)
            {
                continue;
            }
            let from = match trigger {
                Trigger::Record {
                    subscription: delivered,
                    sequence,
                } if delivered == id => sequence
                    .checked_add(1)
                    .ok_or(Error::Invalid("interrupt cursor overflow"))?,
                _ => subscription.cursor,
            };
            self.open(inner, id, &subscription.path, from).await?;
        }
        Ok(())
    }

    pub(super) async fn interrupt(
        &self,
        inner: &mut Inner,
        cancellation: CancellationToken,
    ) -> Error {
        let mut cancelled = false;
        // This active delivery owns a bounded, stable set of follows. Rotate
        // after an exhausted poll so a hot first input cannot hide a later
        // subscription's control record. No lookahead advances a durable cut.
        let ids: Vec<_> = inner.follows.keys().cloned().collect();
        let mut next = 0;
        poll_fn(|context| {
            if cancelled {
                return Poll::Pending;
            }
            // Bound lookahead per poll so a busy inbox cannot starve the
            // handler or its original wall-clock timeout.
            let mut remaining = 256;
            for _ in 0..ids.len() {
                let id = &ids[next];
                next = (next + 1) % ids.len();
                let Some(follow) = inner.follows.get_mut(id) else {
                    return Poll::Ready(Error::Invalid("interrupt follow disappeared"));
                };
                loop {
                    if remaining == 0 {
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    match follow.poll_next_unpin(context) {
                        Poll::Ready(Some(Ok(record))) => {
                            remaining -= 1;
                            if self.handler.manifest().interrupts.iter().any(|filter| {
                                &filter.subscription == id
                                    && !filter.value_prefix.is_empty()
                                    && record.value.starts_with(&filter.value_prefix)
                            }) {
                                cancellation.cancel();
                                cancelled = true;
                                // No unpolled provider future may obstruct the
                                // handler's cleanup after cancellation.
                                inner.follows.clear();
                                return Poll::Pending;
                            }
                        }
                        Poll::Ready(Some(Err(error))) => return Poll::Ready(error.into()),
                        Poll::Ready(None) => return Poll::Ready(StreamError::Unavailable.into()),
                        Poll::Pending => break,
                    }
                }
            }
            // A provider can yield before our own lookahead is exhausted
            // (including cooperative executor admission). Rotate complete
            // scans too, so the same first input cannot take every budget.
            if !ids.is_empty() {
                next = (next + 1) % ids.len();
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest::{Checkpoint, GuestFuture, Manifest, Transition};
    use acyclic_stream::{MemoryStream, StreamProvider};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MultipleInputs {
        manifest: Manifest,
        package: crate::conformance::Package,
    }

    impl Handler for MultipleInputs {
        fn manifest(&self) -> &Manifest {
            &self.manifest
        }
        fn on_record<'a>(
            &'a self,
            delivery: Delivery,
            checkpoint: Option<&'a Checkpoint>,
            cancellation: CancellationToken,
        ) -> GuestFuture<'a, Transition> {
            self.package.on_record(delivery, checkpoint, cancellation)
        }
    }

    #[tokio::test]
    async fn last_subscription_interrupts_after_a_hot_first_input_exhausts_lookahead()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = Arc::new(MemoryStream::default());
        let package = crate::conformance::Package::new()?;
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(package.manifest()),
        )?;
        manifest.subscriptions.push("a".into());
        manifest.interrupts.push(crate::guest::Interrupt {
            subscription: "a".into(),
            value_prefix: Bytes::from_static(b"stop"),
        });
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), StreamPath::new("interrupt/state")?),
            Arc::new(MultipleInputs { manifest, package }),
            Arc::new(SystemClock),
        );
        let mut inner = Inner::default();
        let hot_records = Arc::new(AtomicUsize::new(0));
        for (id, value, count) in [
            ("a", b"ordinary".as_slice(), 512),
            ("inbox", b"cancel".as_slice(), 1),
        ] {
            let path = StreamPath::new(format!("interrupt/{id}"))?;
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![Bytes::copy_from_slice(value).into(); count],
                    if_tail: Some(0),
                    idempotency_key: acyclic_stream::IdempotencyKey::new(
                        format!("interrupt/{id}").into_bytes(),
                    )?,
                })
                .await?;
            let observed = Arc::clone(&hot_records);
            let hot = id == "a";
            inner.follows.insert(
                id.into(),
                acyclic_stream::records_only(
                    provider
                        .follow(acyclic_stream::FollowRequest::new(path, 0))
                        .await?,
                )
                .inspect(move |record| {
                    if hot && record.is_ok() {
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .boxed(),
            );
        }
        let cancellation = CancellationToken::new();
        tokio::select! {
            () = cancellation.cancelled() => Ok(()),
            error = runtime.interrupt(&mut inner, cancellation.clone()) =>
                Err(format!("interrupt observation failed: {error}")),
        }?;
        assert!(
            hot_records.load(Ordering::SeqCst) <= 256,
            "a later subscription must interrupt before the hot input gets another lookahead budget"
        );
        assert!(
            inner.follows.is_empty(),
            "interruption releases provider I/O for cleanup"
        );
        assert!(inner.pending.is_none(), "lookahead creates no publication");
        Ok(())
    }
}
