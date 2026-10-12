use super::*;

pub(super) struct CommitCompletion<T> {
    pub(super) value: T,
    pub(super) error: Option<std::io::Error>,
    pub(super) _input: Option<NativeStreamOutput<Command>>,
    pub(super) visibility: Option<OwnedRwLockWriteGuard<()>>,
    // A rejected original guard did not publish and must not write its frame.
    pub(super) persist: bool,
    pub(super) primary_failure: bool,
}

/// The actual journal job packet exists before submission.
pub(super) struct JournalPersistWork {
    pub(super) frame: NativeStreamOutput<Vec<u8>>,
    pub(super) required: bool,
    pub(super) admission: Option<AdmittedFrameWrite>,
    pub(super) inner: Arc<LocalInner>,
}

pub(super) fn persist(
    work: NativeStreamOutput<JournalPersistWork>,
) -> (Result<(), LocalStreamError>, std::io::Result<()>) {
    let mut job = work.value;
    match job.inner.journal.lock() {
        Ok(mut journal) => journal.append(&job.frame.value, job.required, job.admission.as_mut()),
        Err(_) => (Err(LocalStreamError::Corrupt), Ok(())),
    }
}

impl LocalStream {
    pub(super) fn prepare(
        &self,
        command: &Command,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> MemoryResult<(NativeStreamOutput<Vec<u8>>, u64)> {
        let committed_at_millis = self.inner.provider.clock.now_unix_millis();
        let frame = encode_frame(command, committed_at_millis, meter).map_err(publication_error)?;
        self.inner
            .journal
            .lock()
            .map_err(|_| MemoryFailure::Provider(StreamError::Unavailable))?
            .admit_bytes(frame.value.len() as u64)
            .map_err(publication_error)?;
        Ok((frame, committed_at_millis))
    }

    /// Flushes every frame written without waiting for the storage device (see [`deferring_durability`]), blocking the calling thread.
    pub fn flush(&self) -> Result<(), StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.flush",
            batch_len = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.in_scope(|| obs::finish(&span, self.flush_all(&span)))
    }

    pub(super) fn flush_all(&self, span: &tracing::Span) -> Result<(), StreamError> {
        let (target, durability) = {
            let journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
            if journal.synced >= journal.appended {
                return Ok(());
            }
            span.record("batch_len", journal.appended - journal.synced);
            (journal.appended, journal.limits.durability)
        };
        sync_file_data(&self.inner.sync, durability).map_err(|_| self.inner.poison())?;
        let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
        self.inner.publish_synced(&mut journal, target);
        Ok(())
    }

    pub(super) fn plan_persist(
        &self,
        frame: NativeStreamOutput<Vec<u8>>,
        admission: Option<AdmittedFrameWrite>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> std::io::Result<NativeStreamOutput<JournalPersistWork>> {
        fund_task(
            JournalPersistWork {
                frame,
                required: !deferred(),
                admission,
                inner: Arc::clone(&self.inner),
            },
            meter,
        )
    }

    pub(super) async fn persist_planned(
        &self,
        plan: NativeStreamOutput<JournalPersistWork>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> MemoryResult<()> {
        let (physical, observed) = tokio::task::spawn_blocking(move || persist(plan))
            .await
            .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?;
        if let Err(error) = physical {
            self.inner.poison();
            // A previously observed original cause remains the primary cause.
            observed?;
            return Err(publication_error(error));
        }
        self.inner
            .changed
            .send_modify(|revision| *revision = revision.saturating_add(1));
        let compacted = self.compact_if_due(meter).await;
        observed?;
        compacted
    }

    /// Writes the whole state as the snapshot and starts the journal again, once the journal is half full.
    pub(super) async fn compact_if_due(
        &self,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> MemoryResult<()> {
        if !self
            .inner
            .journal
            .lock()
            .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?
            .compaction_due()
        {
            return Ok(());
        }
        let state = self.inner.provider.encode_state(meter).await?;
        let inner = Arc::clone(&self.inner);
        let observer = meter.cloned();
        let span = obs::OperationSpan::new(tracing::debug_span!(
            "acyclic.stream.compact",
            bytes = state.value.len()
        ));
        let operation = fund_task(
            move || {
                span.in_scope(|| {
                    let mut journal = inner
                        .journal
                        .lock()
                        .map_err(|_| MemoryFailure::Provider(inner.poison()))?;
                    let (physical, observed, started) =
                        journal.compact(&state.value, observer.as_ref());
                    if physical.is_ok() {
                        let appended = journal.appended;
                        inner.publish_synced(&mut journal, appended);
                    } else if started || observer.is_none() {
                        inner.poison();
                    }
                    observed?;
                    physical.map_err(publication_error)
                })
            },
            meter,
        )?;
        tokio::task::spawn_blocking(move || {
            let result = (operation.value)();
            drop(operation.allocation);
            result
        })
        .await
        .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?
    }

    pub(super) fn plan_flush(
        &self,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> MemoryResult<
        NativeStreamOutput<(
            Arc<LocalInner>,
            LocalDurability,
            Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
        )>,
    > {
        let journal = self
            .inner
            .journal
            .lock()
            .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?;
        let admission = meter
            .map(|meter| {
                NativeOperationReservation::new(
                    Arc::clone(meter),
                    NativeOperationKind::Durability,
                    1,
                )
            })
            .transpose()?;
        let allocation = meter
            .map(|meter| {
                ReadAllocation::new(
                    meter,
                    std::mem::size_of::<(
                        NativeStreamOutput<(
                            Arc<LocalInner>,
                            LocalDurability,
                            Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
                        )>,
                        u64,
                    )>(),
                )
            })
            .transpose()?;
        Ok(NativeStreamOutput {
            value: (
                Arc::clone(&self.inner),
                journal.limits.durability,
                admission,
            ),
            allocation,
        })
    }

    pub(super) async fn await_durable(
        &self,
        sequence: u64,
        meter: Option<&Arc<dyn NativeReadMeter>>,
        mut planned: Option<
            NativeStreamOutput<(
                Arc<LocalInner>,
                LocalDurability,
                Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
            )>,
        >,
    ) -> MemoryResult<()> {
        let mut synced = self.inner.synced.subscribe();
        loop {
            let drain_poison = self.inner.poisoned.load(Ordering::Acquire) && planned.is_some();
            if !drain_poison {
                self.inner.check_available()?;
                if *synced.borrow_and_update() >= sequence {
                    return Ok(());
                }
            }
            let lead = {
                let mut journal = self
                    .inner
                    .journal
                    .lock()
                    .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?;
                if journal.syncing || (!drain_poison && journal.synced >= sequence) {
                    None
                } else {
                    let plan = match planned.take() {
                        Some(plan) => plan,
                        None => {
                            drop(journal);
                            let plan = self.plan_flush(meter)?;
                            journal = self
                                .inner
                                .journal
                                .lock()
                                .map_err(|_| MemoryFailure::Provider(self.inner.poison()))?;
                            if journal.syncing || journal.synced >= sequence {
                                planned = Some(plan);
                                continue;
                            }
                            plan
                        }
                    };
                    let target = journal.appended;
                    let before = journal.synced;
                    let bytes = journal.bytes;
                    journal.syncing = true;
                    Some((plan, target, before, bytes))
                }
            };
            match lead {
                Some((plan, target, synced_before, journal_bytes)) => {
                    tokio::task::spawn_blocking(move || flush(plan, target))
                        .await
                        .map_err(|_| MemoryFailure::Provider(self.inner.poison()))??;
                    tracing::debug!(
                        fsyncs = 1_u64,
                        batch_len = target.saturating_sub(synced_before),
                        journal_bytes,
                        waiters = self.inner.synced.receiver_count(),
                        "acyclic.stream.fsync"
                    );
                    if drain_poison {
                        return Err(StreamError::Unavailable.into());
                    }
                }
                None => {
                    if let Some(meter) = meter {
                        if planned.is_none() {
                            meter.check()?;
                        }
                    }
                    synced
                        .changed()
                        .await
                        .map_err(|_| MemoryFailure::Provider(StreamError::Unavailable))?;
                }
            }
        }
    }

    pub(super) async fn visible<T, E>(
        &self,
        read: impl Future<Output = Result<T, E>>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> MemoryResult<T>
    where
        MemoryFailure: From<E>,
    {
        self.inner.check_available()?;
        if let Some(meter) = meter {
            meter.check()?;
        }
        let (result, required) = {
            let _visibility = self.inner.visibility.read().await;
            self.inner.check_available()?;
            if let Some(meter) = meter {
                meter.check()?;
            }
            (
                read.await.map_err(MemoryFailure::from),
                self.inner.required()?,
            )
        };
        let durable = self.await_durable(required, meter, None).await;
        // A produced original cause wins over a later drain observation.
        let result = result?;
        durable?;
        if let Some(meter) = meter {
            meter.check()?;
        }
        Ok(result)
    }

    pub(super) async fn mutation<T, F>(
        &self,
        input: NativeStreamOutput<Command>,
        deadline: Option<u64>,
        meter: Option<Arc<dyn NativeReadMeter>>,
        execute: impl FnOnce(
            MemoryStream,
            NativeStreamOutput<Command>,
            Option<u64>,
            u64,
            u64,
            OwnedRwLockWriteGuard<()>,
        ) -> F
        + Send
        + 'static,
    ) -> MemoryResult<T>
    where
        T: Send + 'static,
        F: Future<Output = MemoryResult<CommitCompletion<T>>> + Send + 'static,
    {
        let is_commit = matches!(&input.value, Command::Commit(_));
        let span = obs::OperationSpan::new(match &input.value {
            Command::Commit(request) => tracing::info_span!(
                "acyclic.stream.commit",
                items = request.mutations.len(),
                outcome = Empty,
                error.kind = Empty,
            ),
            Command::Fork(_) => {
                tracing::info_span!("acyclic.stream.fork", outcome = Empty, error.kind = Empty)
            }
            Command::Append(request) => tracing::info_span!(
                "acyclic.stream.append",
                items = request.records.len(),
                bytes = request
                    .records
                    .iter()
                    .map(crate::OwnedBytes::len)
                    .sum::<usize>(),
                outcome = Empty,
                error.kind = Empty,
            ),
        });
        self.inner
            .check_available()
            .inspect_err(|error| obs::failed(&span, error.code()))?;
        if let Some(meter) = &meter {
            meter.check()?;
        }
        let stream = self.clone();
        let deferred = deferred();
        let operation = span.clone();
        let observer = meter.clone();
        let task = DEFERRED.scope(deferred, async move {
            let result = operation
                .scope(async {
                    let mut accepted_flush = None;
                    let mut completion = None;
                    let (result, required) = {
                        let visibility = Arc::clone(&stream.inner.visibility).write_owned().await;
                        stream.inner.check_available()?;
                        if let Some(meter) = &observer {
                            meter.check()?;
                        }
                        stream.compact_if_due(observer.as_ref()).await?;
                        let result = async {
                            let command = &input.value;
                            let (frame, committed_at) =
                                stream.prepare(command, observer.as_ref())?;
                            let frame_bytes = frame.value.len() as u64;
                            let admission = observer
                                .as_ref()
                                .map(|meter| AdmittedFrameWrite::new(meter, frame.value.len()))
                                .transpose()?;
                            let mut flush = if observer.is_some() && !deferred {
                                Some(stream.plan_flush(observer.as_ref())?)
                            } else {
                                None
                            };
                            if is_commit {
                                accepted_flush = flush.take();
                            }
                            let persist =
                                stream.plan_persist(frame, admission, observer.as_ref())?;
                            completion = Some(
                                execute(
                                    stream.inner.provider.clone(),
                                    input,
                                    deadline,
                                    committed_at,
                                    frame_bytes,
                                    visibility,
                                )
                                .await?,
                            );
                            let completion = completion.as_mut().expect("executed mutation");
                            let persisted = if completion.persist {
                                if !is_commit {
                                    accepted_flush = flush.take();
                                }
                                stream.persist_planned(persist, observer.as_ref()).await
                            } else {
                                drop(persist);
                                Ok(())
                            };
                            if let Some(error) = completion.error.take() {
                                return Err(MemoryFailure::Observer(error));
                            }
                            if completion.primary_failure {
                                return Ok(());
                            }
                            persisted?;
                            Ok(())
                        }
                        .await;
                        let required = stream.inner.required();
                        if let Some(completion) = &mut completion {
                            drop(completion.visibility.take());
                        }
                        (result, required)
                    };
                    let durable = match required {
                        Ok(required) => {
                            stream
                                .await_durable(required, observer.as_ref(), accepted_flush)
                                .await
                        }
                        Err(error) => {
                            if let Some(plan) = accepted_flush {
                                let _ = tokio::task::spawn_blocking(move || flush(plan, 0)).await;
                            }
                            Err(MemoryFailure::Provider(error))
                        }
                    };
                    result?;
                    let completion = completion.expect("successful mutation owns its value");
                    if !completion.primary_failure {
                        durable?;
                        if let Some(meter) = &observer {
                            meter.check()?;
                        }
                    }
                    Ok(completion.value)
                })
                .await;
            match &result {
                Ok(_) => {
                    let _ = obs::finish(&operation, Ok::<(), StreamError>(()));
                }
                Err(MemoryFailure::Provider(error)) => obs::failed(&operation, error.code()),
                Err(MemoryFailure::Observer(_)) => obs::failed(
                    &operation,
                    if observer.is_some() {
                        "native_work"
                    } else {
                        StreamError::Unavailable.code()
                    },
                ),
            }
            result
        });
        span.scope(async {
            let task = fund_task(task, meter.as_ref())?;
            tokio::spawn(task).await.map_err(|_| {
                obs::failed(&span, StreamError::Unavailable.code());
                MemoryFailure::Provider(StreamError::Unavailable)
            })?
        })
        .await
    }
}
