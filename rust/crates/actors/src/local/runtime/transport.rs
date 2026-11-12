//! Activation follows retain their accepted cursor through transport loss.
use super::*;

pub(super) fn unavailable(error: &Error) -> bool {
    match error {
        Error::Stream(error) => error.category() == StreamError::Unavailable,
        Error::Handler(crate::guest::GuestError::Unavailable(_)) => true,
        _ => false,
    }
}

pub(super) async fn retry<T, F, Fut>(mut operation: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    loop {
        match operation().await {
            Err(error) if unavailable(&error) => sleep(10).await,
            result => return result,
        }
    }
}

pub(super) fn follow<P: StreamProvider + ?Sized + 'static>(
    provider: Arc<P>,
    path: StreamPath,
    from: u64,
) -> acyclic_stream::BoxProviderStream<'static, Result<Record, StreamError>> {
    Box::pin(futures::stream::try_unfold(
        (None::<RecordStream>, from),
        move |(mut records, mut next)| {
            let provider = Arc::clone(&provider);
            let path = path.clone();
            async move {
                loop {
                    let result = match &mut records {
                        Some(records) => records
                            .next()
                            .await
                            .unwrap_or(Err(StreamError::Unavailable)),
                        None => match provider
                            .follow(acyclic_stream::FollowRequest::new(path.clone(), next))
                            .await
                        {
                            Ok(opened) => {
                                records = Some(acyclic_stream::records_only(opened));
                                continue;
                            }
                            Err(error) => Err(error),
                        },
                    };
                    match result {
                        Ok(record) => {
                            if record.sequence != next {
                                return Err(StreamError::InvalidResponse);
                            }
                            next = next.checked_add(1).ok_or(StreamError::OutOfRange)?;
                            return Ok(Some((record, (records, next))));
                        }
                        Err(error) if error.category() == StreamError::Unavailable => {
                            records = None;
                            sleep(10).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{ExplicitStreamProvider, FollowRequest, FollowStream, MemoryStream};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct Interrupted {
        inner: MemoryStream,
        attempts: AtomicUsize,
        cursors: Mutex<Vec<u64>>,
    }

    #[async_trait::async_trait]
    impl ExplicitStreamProvider for Interrupted {
        fn context(&self) -> acyclic_stream::ProviderContext<'_> {
            self.inner.context()
        }
        async fn heads(
            &self,
            request: acyclic_stream::HeadsRequest,
        ) -> Result<acyclic_stream::Heads, StreamError> {
            self.inner.heads(request).await
        }
        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }
        fn write(
            &self,
            request: acyclic_stream::WriteRequest,
        ) -> BoxProviderFuture<'_, Result<acyclic_stream::WriteOutcome, StreamError>> {
            self.inner.write(request)
        }
        async fn read(
            &self,
            request: acyclic_stream::ReadRequest,
        ) -> Result<RecordStream, StreamError> {
            self.inner.read(request).await
        }
        async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
            self.cursors.lock().unwrap().push(request.from);
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(StreamError::Unavailable);
            }
            let opened = self.inner.follow(request).await?;
            if attempt == 1 {
                Ok(opened.map_cursor(|cursor| {
                    Box::pin(cursor.take(2).chain(futures::stream::once(async {
                        Err(StreamError::Unavailable)
                    })))
                }))
            } else {
                Ok(opened)
            }
        }
    }

    #[tokio::test]
    async fn unavailable_activation_follow_resumes_without_losing_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = Arc::new(Interrupted {
            inner: MemoryStream::default(),
            attempts: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
        });
        let path = StreamPath::new("activations")?;
        provider
            .append(acyclic_stream::AppendRequest {
                path: path.clone(),
                records: vec![
                    bytes::Bytes::from_static(b"first").into(),
                    bytes::Bytes::from_static(b"second").into(),
                ],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let mut records = follow(provider.clone(), path, 0);
        assert_eq!(records.next().await.ok_or("missing first")??.sequence, 0);
        assert_eq!(records.next().await.ok_or("missing second")??.sequence, 1);
        assert_eq!(*provider.cursors.lock().unwrap(), vec![0, 0, 1]);
        Ok(())
    }
}
