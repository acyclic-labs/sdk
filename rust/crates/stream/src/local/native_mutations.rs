impl LocalStream {
    fn guarded_commit<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        mut before: F,
    ) -> crate::NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        #[cfg(feature = "test-support")]
        describe_commit(&request);
        let original = Arc::clone(&meter);
        let stream = self.clone();
        crate::native::Work::start(meter, |work| async move {
            let mut stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            // The caller's handle stays with this future; the detached work
            // holds none, so it drains as the store closes.
            let _handle = stream.handle.take();
            let provider = stream.inner.provider.with_context(Arc::clone(&work));
            let input = match provider.request_claim(&request) {
                Ok(input) => input,
                Err(error) => {
                    return work
                        .finish(Err(error))
                        .map(|result| result.map_err(E::from));
                }
            };
            let worker = publication_scope(async move {
                let _input = input;
                let result = loop {
                    let revision = *stream.inner.revision.borrow();
                    let replay = match provider.has_replay(&request.idempotency_key).await {
                        Ok(replay) => replay,
                        Err(error) => {
                            return work
                                .finish(Err(error))
                                .map(|result| result.map_err(E::from));
                        }
                    };
                    if !replay && let Err(error) = before().await {
                        return Ok(Err(error));
                    }
                    let Ok(visibility) = stream.inner.visibility.try_write() else {
                        drop(stream.inner.visibility.write().await);
                        continue;
                    };
                    if revision != *stream.inner.revision.borrow() {
                        drop(visibility);
                        continue;
                    }
                    let publication = stream.inner.publication.write().await;
                    let mut result = async {
                        stream.check_available()?;
                        let execution = stream
                            .execute(WriteRequest::Commit(request), deadline)
                            .await?;
                        let crate::WriteOutcome::Commit(outcome) = execution.outcome else {
                            return Err(StreamError::InvalidResponse);
                        };
                        Ok((outcome, execution.allocation))
                    }
                    .await;
                    stream.inner.commit_clock.unpin();
                    let _retained = stream.release_publication(publication);
                    if result.is_ok()
                        && let Err(error) = stream.compact_if_due().await
                    {
                        result = Err(error);
                    }
                    let required = stream.inner.required();
                    drop(visibility);
                    let result = match required {
                        Ok(required) => stream.await_publication(required).await.and(result),
                        Err(error) => Err(error),
                    };
                    break result;
                };
                // Keep the callback's captured lease through the physical terminal.
                drop(before);
                work.finish(result).map(|result| result.map_err(E::from))
            });
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &original,
                std::mem::size_of_val(&worker),
            )?;
            let deferred = current_deferral();
            let task = spawn(DEFERRED.scope(deferred, async move {
                let _allocation = allocation;
                worker.await
            }));
            task.await.map_err(std::io::Error::other)?
        })
    }

    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, AppendOutcome> {
        #[cfg(feature = "test-support")]
        acyclic_native_runtime::testing::describe(|| {
            format!(
                "native append {} {:?}",
                request.path, request.idempotency_key
            )
        });
        let stream = self.clone();
        crate::native::Work::start(meter, |work| async move {
            let stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            let input = work.record(acyclic_native_runtime::ReadAllocation::adopt(
                &work.meter,
                request.records.capacity() * std::mem::size_of::<crate::OwnedBytes>(),
            ));
            let input = match input {
                Ok(input) => input,
                Err(error) => return work.finish(Err(error)),
            };
            let span = obs::OperationSpan::new(tracing::info_span!(
                "acyclic.stream.append",
                outcome = Empty,
                error.kind = Empty
            ));
            let result = stream
                .exclusive(&span, move |stream| async move {
                    let _input = input;
                    let execution = stream.execute(WriteRequest::Append(request), None).await?;
                    let crate::WriteOutcome::Append(outcome) = execution.outcome else {
                        return Err(StreamError::InvalidResponse);
                    };
                    Ok((outcome, execution.allocation))
                })
                .await;
            work.finish(result)
        })
    }
}
