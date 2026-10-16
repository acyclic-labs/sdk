impl LocalStream {
    fn prepare_commit(
        &self,
        request: CommitRequest,
        maximum: usize,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        #[cfg(feature = "test-support")]
        describe_commit(&request);
        let original = Arc::clone(&meter);
        let stream = self.clone();
        crate::NativeProviderFuture::new(&Arc::clone(&original), async move {
            let work = crate::memory::Work::new(meter)?;
            let stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            let provider = stream.inner.provider.with_context(Arc::clone(&work));
            let result = async {
                let _visibility = stream.inner.visibility.read().await;
                stream.check_available()?;
                let input = provider.request_claim(&request)?;
                // Clone only before issuance, with both control vectors funded.
                let copied = usize::try_from(
                    input
                        .as_ref()
                        .map_or(0, acyclic_native_runtime::ReadAllocation::bytes),
                )
                .map_err(|_| StreamError::Capacity)?;
                let command_claim = work.record(acyclic_native_runtime::ReadAllocation::new(
                    &work.meter,
                    copied,
                ))?;
                work.record(work.meter.admit_codec(0, copied as u64, 0))?;
                let command = Command::Commit(request.clone());
                work.record(work.meter.record_codec(0, copied as u64, 0))?;
                drop(input);
                let (plan, recovery) = provider.prepare(request, maximum).await?;
                let capacity = command
                    .encoded_len()
                    .checked_add(maximum)
                    .and_then(|bytes| {
                        bytes.checked_add(4 + FRAME_TIME_BYTES + FRAME_CHECKSUM_BYTES + 20)
                    })
                    .ok_or(StreamError::Capacity)?;
                let frame = PreparedFrame::reserve(capacity, Some(Arc::clone(&work)))
                    .map_err(|error| stream.frame_error(error))?;
                work.reserve_durability(4)?;
                let slot = work.record(crate::native::NativeCommitRecordSlot::new(&work.meter))?;
                let ingress = Arc::clone(&slot);
                let writer = stream.clone();
                let writer_work = Arc::clone(&work);
                let worker = async move {
                    let _command_claim = command_claim;
                    let result = writer.settle_prepared(command, frame, plan, ingress).await;
                    writer_work.finish(result)
                };
                let allocation = work.record(acyclic_native_runtime::ReadAllocation::new(
                    &work.meter,
                    std::mem::size_of_val(&worker),
                ))?;
                let task = spawn(async move {
                    let _allocation = allocation;
                    worker.await
                });
                let future = match crate::NativeProviderFuture::try_new(&work.meter, async move {
                    task.await.map_err(std::io::Error::other)?
                }) {
                    Ok(future) => future,
                    Err(error) => {
                        slot.close();
                        return Err({
                            let _ = work.record::<()>(Err(error));
                            StreamError::Unavailable
                        });
                    }
                };
                Ok((
                    crate::PreparedNativeCommit::new(future, recovery, slot, maximum),
                    None,
                ))
            }
            .await;
            work.finish(result)
        })
    }

    async fn settle_prepared(
        &self,
        mut command: Command,
        mut frame: PreparedFrame,
        plan: crate::memory::PreparedCommit,
        ingress: Arc<crate::native::NativeCommitRecordSlot>,
    ) -> Result<
        (
            CommitOutcome,
            Option<acyclic_native_runtime::ReadAllocation>,
        ),
        StreamError,
    > {
        let body = ingress.receive().await?;
        let visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let Command::Commit(request) = &mut command else {
            unreachable!()
        };
        let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice()
        else {
            unreachable!()
        };
        *records.first_mut().ok_or(StreamError::InvalidArgument)? = body.clone();
        let time = self.inner.commit_clock.source.now_unix_millis();
        frame
            .fill(&command, time)
            .map_err(|error| self.frame_error(error))?;
        self.inner
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .admit(&frame)
            .map_err(|error| self.frame_error(error))?;
        self.inner.commit_clock.pin(time);
        let publication = self.inner.publication.write().await;
        let result = plan.settle(body).await;
        self.inner.commit_clock.unpin();
        let result = match result {
            Ok(result) => {
                self.persist(frame).await?;
                Ok(result)
            }
            Err(error) => Err(error),
        };
        let _retained = self.release_publication(publication);
        if result.is_ok() {
            self.compact_if_due().await?;
        }
        let required = self.inner.required()?;
        drop(visibility);
        self.await_durable(required).await?;
        result
    }

    fn frame_error(&self, error: LocalStreamError) -> StreamError {
        match error {
            LocalStreamError::InvalidLimits => StreamError::Capacity,
            LocalStreamError::Io(error) => match &self.work {
                Some(work) => {
                    let _ = work.record::<()>(Err(error));
                    StreamError::Unavailable
                }
                None => StreamError::Unavailable,
            },
            _ => StreamError::Unavailable,
        }
    }
}
