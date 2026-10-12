use super::*;

/// The SAME persist and flush jobs are accepted on one blocking submission before issuing the late-input packet.
pub(super) struct PreparedLocalWork {
    pub(super) persist: NativeStreamOutput<JournalPersistWork>,
    pub(super) input: NativeStreamOutput<Command>,
    pub(super) prepared: crate::memory::NativeMemoryPreparedCommit,
    pub(super) record: Arc<crate::NativeCommitRecordSlot>,
    pub(super) meter: Arc<dyn NativeReadMeter>,
    pub(super) flush: NativeStreamOutput<(
        Arc<LocalInner>,
        LocalDurability,
        Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>,
    )>,
}

impl PreparedLocalWork {
    pub(super) fn run(mut self) -> NativeStreamResult<CommitOutcome> {
        let inner = &self.flush.value.0;
        let body = match self.record.receive_blocking() {
            Ok(body) => body,
            Err(error) => return native_result(Err(error.into())),
        };
        let result: MemoryResult<NativeStreamOutput<CommitOutcome>> = (|| {
            // Input was supplied before taking any provider lock.
            let Command::Commit(request) = &mut self.input.value else {
                return Err(StreamError::InvalidArgument.into());
            };
            let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice()
            else {
                return Err(StreamError::InvalidArgument.into());
            };
            let Some(record) = records.first_mut() else {
                return Err(StreamError::InvalidArgument.into());
            };
            *record = body;
            let mut late = Ok(());
            let visibility = inner.visibility.blocking_write();
            inner.check_available()?;
            let committed_at = inner.provider.clock.now_unix_millis();
            let wire = crate::wire_codec::CanonicalWire::Journal(&self.input.value);
            let encoded_length = wire.encoded_len(None, None)?;
            keep_observation(&mut late, self.meter.record_examination(1, 0));
            write_frame(
                &mut self.persist.value.frame.value,
                &wire,
                committed_at,
                encoded_length,
                Some(&self.meter),
                Some(&mut late),
            )
            .map_err(publication_error)?;
            inner
                .journal
                .lock()
                .map_err(|_| MemoryFailure::Provider(inner.poison()))?
                .admit_bytes(self.persist.value.frame.value.len() as u64)
                .map_err(publication_error)?;
            let Command::Commit(request) = &mut self.input.value else {
                unreachable!("prepared Commit input")
            };
            let completion = inner.provider.finish_commit(
                &mut *inner.provider.state.blocking_write(),
                self.prepared,
                request,
                Some(committed_at),
            );
            let (outcome, error) = completion
                .map_err(MemoryFailure::Observer)?
                .map_err(MemoryFailure::Provider)?;
            let (physical, observed) = persist(self.persist);
            if physical.is_err() {
                inner.poison();
            } else {
                inner
                    .changed
                    .send_modify(|revision| *revision = revision.saturating_add(1));
            }
            keep_observation(&mut late, observed);
            drop(visibility);
            late?;
            if let Some(error) = error {
                return Err(MemoryFailure::Observer(error));
            }
            physical.map_err(publication_error)?;
            Ok(crate::memory::native(Ok(outcome), &self.meter)?.expect("completed publication"))
        })();
        let required = inner.required().unwrap_or(0);
        let flushed = flush(self.flush, required);
        native_result(match result {
            Err(error) => Err(error),
            Ok(value) => flushed.map(|()| value),
        })
    }
}

impl LocalInner {
    /// The latest frame a caller that observed the current state must wait to flush.
    pub(super) fn required(&self) -> Result<u64, StreamError> {
        Ok(self
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .required)
    }
}

impl LocalStream {
    pub(super) fn native_commit_with_guard<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        before: Option<F>,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let observer = Arc::clone(&meter);
        let guard_stream = self.clone();
        NativeProviderFuture::new(&meter, async move {
            let input = crate::capture_commit(&request, &observer)?;
            // SDK descriptor custody is accepted before the worker detaches.
            drop(request);
            let execute_meter = Arc::clone(&observer);
            let result = self.mutation(
                NativeStreamOutput { value: Command::Commit(input.value), allocation: input.allocation }, deadline, Some(observer),
                move |provider, input, deadline, committed_at, frame_bytes, visibility| async move {
                    let NativeStreamOutput { value, allocation } = input;
                    let Command::Commit(mut request) = value else { unreachable!() };
                    let mut before = before;
                    let guarded = before.is_some();
                    let stage_visibility = if guarded { drop(visibility); None } else { Some(visibility) };
                    let mut acquire = || {
                        let mut original = before.as_mut().expect("guarded Commit has its original callback").clone();
                        let stream = guard_stream.clone();
                        async move {
                            loop {
                                let available = Arc::clone(&stream.inner.visibility).write_owned().await;
                                drop(available);
                                original().await?;
                                let Ok(visibility) = Arc::clone(&stream.inner.visibility).try_write_owned() else {
                                    // No permission survives a contended wait.
                                    continue;
                                };
                                stream.inner.check_available().map_err(|error| E::from(error))?;
                                let admitted = {
                                    match stream.inner.journal.try_lock() {
                                        Ok(journal) => Some(journal.admit_bytes(frame_bytes)),
                                        Err(std::sync::TryLockError::Poisoned(_)) =>
                                            return Err(E::from(stream.inner.poison())),
                                        Err(std::sync::TryLockError::WouldBlock) => None,
                                    }
                                };
                                let Some(admitted) = admitted else {
                                    drop(visibility);
                                    {
                                        let _available = stream.inner.journal.lock()
                                            .map_err(|_| E::from(stream.inner.poison()))?;
                                    }
                                    continue;
                                };
                                if admitted.is_err() {
                                    return Err(E::from(StreamError::Capacity));
                                }
                                return Ok(visibility);
                            }
                        }
                    };
                    let completion = provider.apply_commit(&mut request, deadline, Some(&execute_meter),
                        if guarded { Some(&mut acquire) } else { None }, Some(committed_at),
                    ).await;
                    drop(acquire);
                    let (result, visibility, persist) = match completion {
                        Ok(Ok((outcome, error, visibility))) => {
                            let result = match error {
                                Some(error) => Err(error),
                                None => crate::memory::native(Ok(outcome), &execute_meter).map(|result| result.map_err(E::from)),
                            };
                            (result, visibility, true)
                        }
                        Ok(Err(error)) => (Ok(Err(error)), None, false),
                        Err(MemoryFailure::Observer(error)) => (Err(error), None, false),
                        Err(MemoryFailure::Provider(error)) => (Ok(Err(E::from(error))), None, false),
                    };
                    let primary_failure = !matches!(&result, Ok(Ok(_)));
                    Ok(CommitCompletion {
                        value: (result, before),
                        error: None, _input: Some(NativeStreamOutput { value: Command::Commit(request), allocation }), visibility: visibility.or(stage_visibility), persist, primary_failure,
                    })
                },
            ).await;
            match result {
                Ok(completion) => completion.0,
                Err(MemoryFailure::Observer(error)) => Err(error),
                Err(MemoryFailure::Provider(error)) => Ok(Err(E::from(error))),
            }
        })
    }
}
