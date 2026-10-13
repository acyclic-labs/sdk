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
        let original = Arc::clone(&meter);
        let stream = self.clone();
        crate::NativeProviderFuture::new(&Arc::clone(&original), async move {
            let work = crate::memory::Work::new(meter)?;
            let stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            let provider = stream.inner.provider.with_context(Arc::clone(&work));
            let input = match provider.request_claim(&request) {
                Ok(input) => input,
                Err(error) => {
                    return work
                        .finish(Err(error))
                        .map(|result| result.map_err(E::from));
                }
            };
            let worker = async move {
                let _input = input;
                let result = loop {
                    let revision = *stream.inner.changed.borrow();
                    let replay = provider.has_replay(&request.idempotency_key).await;
                    if !replay {
                        if let Err(error) = before().await {
                            return Ok(Err(error));
                        }
                    }
                    let visibility = match stream.inner.visibility.try_write() {
                        Ok(visibility) => visibility,
                        Err(_) => {
                            drop(stream.inner.visibility.write().await);
                            continue;
                        }
                    };
                    if revision != *stream.inner.changed.borrow() {
                        drop(visibility);
                        continue;
                    }
                    let result = async {
                        stream.check_available()?;
                        let command = Command::Commit(request);
                        let frame = if replay {
                            None
                        } else {
                            work.reserve_durability(4)?;
                            Some(stream.prepare(&command)?)
                        };
                        let Command::Commit(request) = command else {
                            unreachable!()
                        };
                        let result = provider.commit_inner(request, deadline).await?;
                        if let Some(frame) = frame {
                            stream.persist(frame).await?;
                        }
                        Ok(result)
                    }
                    .await;
                    stream.inner.commit_clock.unpin();
                    let required = stream.inner.required();
                    drop(visibility);
                    let result = match required {
                        Ok(required) => stream.await_durable(required).await.and(result),
                        Err(error) => Err(error),
                    };
                    break result;
                };
                // Keep the callback's captured lease through the physical terminal.
                drop(before);
                work.finish(result).map(|result| result.map_err(E::from))
            };
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &original,
                std::mem::size_of_val(&worker),
            )?;
            let task = tokio::spawn(async move {
                let _allocation = allocation;
                worker.await
            });
            task.await.map_err(std::io::Error::other)?
        })
    }

    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'_, AppendOutcome> {
        let original = Arc::clone(&meter);
        let stream = self.clone();
        crate::NativeProviderFuture::new(&original, async move {
            let work = crate::memory::Work::new(meter)?;
            let stream = Self {
                work: Some(Arc::clone(&work)),
                ..stream
            };
            let provider = stream.inner.provider.with_context(Arc::clone(&work));
            let input = work.record(acyclic_native_runtime::ReadAllocation::adopt(
                &work.meter,
                request.records.capacity() * std::mem::size_of::<crate::OwnedBytes>(),
            ));
            let input = match input {
                Ok(input) => input,
                Err(error) => return work.finish(Err(error)),
            };
            let span = obs::OwnedSpan::new(tracing::info_span!(
                "acyclic.stream.append",
                outcome = Empty,
                error.kind = Empty
            ));
            let result = stream
                .exclusive(&span, move |stream| async move {
                    let _input = input;
                    let replay = match &request.idempotency_key {
                        Some(key) => provider.has_replay(key).await,
                        None => false,
                    };
                    let command = Command::Append(request);
                    let frame = if replay {
                        None
                    } else {
                        stream
                            .work
                            .as_ref()
                            .expect("native context")
                            .reserve_durability(4)?;
                        Some(stream.prepare(&command)?)
                    };
                    let Command::Append(request) = command else {
                        unreachable!()
                    };
                    let retain_conflict = request.idempotency_key.is_some();
                    let result = provider.append_inner(request).await?;
                    if let Some(frame) = frame
                        && (matches!(result.0, AppendOutcome::Committed(_)) || retain_conflict)
                    {
                        stream.persist(frame).await?;
                    }
                    Ok(result)
                })
                .await;
            work.finish(result)
        })
    }
}
