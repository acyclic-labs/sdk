//! Per-delivery state uses the runtime's original cut. Successful flushes
//! advance that cut before the final transition, without running the guest again.
use super::*;
use crate::guest::{ActorMetadataWrite, ActorStateSeed, GuestError, StateCommand};

struct StateOwner<P: StreamProvider + ?Sized> {
    store: Store<P>,
    snapshot: Arc<Snapshot>,
    epoch: u64,
    receiver: tokio::sync::mpsc::Receiver<StateCommand>,
    conditions: Vec<acyclic_stream::CommitCondition>,
}

impl<P: StreamProvider + ?Sized> StateOwner<P> {
    async fn run(&mut self, stopped: crate::guest::CancellationToken) -> Result<(), Error> {
        loop {
            // Stop accepting commands at return/timeout. An already dispatched
            // publication keeps its owner until it returns its original result.
            let command = tokio::select! {
                biased;
                () = stopped.cancelled() => return Ok(()),
                command = self.receiver.recv() => match command {
                    Some(command) => command,
                    None => return Ok(()),
                },
            };
            match command {
                StateCommand::Seed(reply) => {
                    let state = self.snapshot.state().ok_or(Error::Conflict)?;
                    let seed = ActorStateSeed {
                        metadata: state
                            .metadata
                            .iter()
                            .map(|(key, value)| ActorMetadataWrite {
                                key: key.clone(),
                                value: value.clone(),
                            })
                            .collect(),
                        journal_tail: state.journal_tail.to_string(),
                    };
                    let _ = reply.send(Ok(seed));
                }
                StateCommand::Read(from, limit, reply) => {
                    let binding = self
                        .store
                        .bind_state((*self.snapshot).clone(), self.epoch)?;
                    let records = tokio::select! {
                        biased;
                        () = stopped.cancelled() => return Ok(()),
                        records = binding.read_journal(from, limit) => records,
                    };
                    match records {
                        Ok(records) => {
                            let _ = reply.send(Ok(records
                                .into_iter()
                                .map(|record| bytes::Bytes::copy_from_slice(&record))
                                .collect()));
                        }
                        Err(error) => {
                            let _ = reply.send(Err(GuestError::Pending(error.to_string())));
                            return Err(error);
                        }
                    }
                }
                StateCommand::Commit(packet, reply) => {
                    let result = async {
                        let mut binding = self
                            .store
                            .bind_state((*self.snapshot).clone(), self.epoch)?;
                        binding.apply(packet)?;
                        let mut publication =
                            binding.prepare_flush(key(&self.snapshot, "state")?)?;
                        publication.require(self.conditions.clone())?;
                        self.store
                            .submit_retained(publication)
                            .await?
                            .ok_or(Error::Invalid("state flush omitted its original cut"))
                    }
                    .await;
                    match result {
                        Ok(snapshot) => {
                            self.snapshot = Arc::new(snapshot);
                            let _ = reply.send(Ok(()));
                        }
                        Err(error) => {
                            let _ = reply.send(Err(GuestError::Pending(error.to_string())));
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

impl<P: StreamProvider + ?Sized> Runtime<P> {
    /// Every handler entry uses the same manifest deadline and control fence,
    /// including recovery from a definitive publication refusal. Each
    /// invocation gets its own capability binding, revoked when it returns,
    /// times out or is cancelled, so a task it leaves behind holds nothing.
    pub(super) async fn handled<'f, T>(
        &self,
        inner: &mut Inner,
        actor: &ActorReference,
        epoch: u64,
        cancellation: crate::guest::CancellationToken,
        delivery: impl FnOnce(crate::guest::RequestId) -> Delivery,
        checkpoint: Option<&crate::guest::Checkpoint>,
        cut: &mut Arc<Snapshot>,
        invoke: impl FnOnce(Delivery) -> crate::guest::GuestFuture<'f, T>,
    ) -> Result<T, Error> {
        let request = self.requests.open(actor, epoch);
        let stopped = crate::guest::CancellationToken::new();
        let mut owner = self
            .handler
            .manifest()
            .bindings
            .iter()
            .any(crate::domain::Binding::is_actor_state)
            .then(|| {
                let (commands, receiver) = tokio::sync::mpsc::channel(1);
                self.requests.bind_state(
                    request.id(),
                    crate::guest::ActorStateHost {
                        commands,
                        stopped: stopped.clone(),
                    },
                );
                StateOwner {
                    store: self.store.clone(),
                    snapshot: Arc::clone(cut),
                    epoch,
                    receiver,
                    conditions: self.handler.publication_conditions(actor),
                }
            });
        let delivery = delivery(request.id());
        let mut request = Some(request);
        let execution = async {
            let handled = core::pin::pin!(acyclic_stream::trace::observe(
                acyclic_stream::trace::Request {
                    name: "actors.handler",
                    mechanism: acyclic_stream::trace::Mechanism::ActorDelivery,
                    parent: None,
                    guarantee: Some("admitted_handler_under_original_control_and_deadline"),
                },
                invoke(delivery.clone()),
                |_| None,
            ));
            let delivery = &delivery;
            let interrupted = self.interrupt(inner, cancellation, |subscription, record| {
                Box::pin(async move {
                    self.handler
                        .accepts_interrupt(delivery, checkpoint, &subscription, &record)
                        .await
                })
            });
            let interrupted = core::pin::pin!(interrupted);
            let timeout: BoxProviderFuture<'_, ()> = Box::pin(async {
                match self.handler.manifest().limits.handler_timeout_millis() {
                    None => futures::future::pending::<()>().await,
                    Some(milliseconds) => sleep(milliseconds).await,
                }
            });
            match futures::future::select(handled, futures::future::select(timeout, interrupted))
                .await
            {
                futures::future::Either::Left((transition, _)) => transition.map_err(Error::from),
                futures::future::Either::Right((futures::future::Either::Left(_), _)) => {
                    Err(Error::Invalid("handler exceeded its manifest timeout"))
                }
                futures::future::Either::Right((futures::future::Either::Right((error, _)), _)) => {
                    Err(error)
                }
            }
        };
        let result = if let Some(owner) = &mut owner {
            let serving = owner.run(stopped.clone());
            futures::pin_mut!(execution, serving);
            match futures::future::select(execution, serving).await {
                futures::future::Either::Left((result, serving)) => {
                    drop(request.take());
                    stopped.cancel();
                    serving.await.and(result)
                }
                futures::future::Either::Right((result, _)) => {
                    drop(request.take());
                    stopped.cancel();
                    result.and(Err(Error::Invalid("actor state producer stopped")))
                }
            }
        } else {
            execution.await
        };
        drop(request);
        if let Some(owner) = owner {
            *cut = owner.snapshot;
        }
        match result {
            Err(Error::Unknown {
                publication,
                source,
            }) => {
                inner.pending = Some((Arc::clone(cut), *publication, None));
                Err(Error::Stream(source))
            }
            result => result,
        }
    }
}
