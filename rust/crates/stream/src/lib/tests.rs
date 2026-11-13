#[cfg(test)]
mod error_code_tests {
    use super::StreamError;

    #[test]
    fn local_causes_survive_cloning_without_changing_classification() {
        use std::error::Error as _;
        for category in StreamError::ALL {
            let original = category.clone().with_source(
                StreamError::InvalidArgument.with_source("invalid".parse::<u64>().unwrap_err()),
            );
            let cloned = original.clone();
            assert_eq!(original, cloned);
            drop(original);
            assert_eq!(cloned.category(), category);
            assert_eq!(cloned.code(), category.code());
            assert_eq!(cloned.uncertain(), category.uncertain());
            let mut source = cloned.source();
            let mut found = false;
            while let Some(cause) = source {
                found |= cause.downcast_ref::<std::num::ParseIntError>().is_some();
                source = cause.source();
            }
            assert!(found, "the original concrete parser error was lost");
            assert!(category.source().is_none());
            #[cfg(feature = "serde")]
            {
                let encoded = serde_json::to_value(&cloned).unwrap();
                assert_eq!(encoded, serde_json::to_value(&category).unwrap());
                let decoded: StreamError = serde_json::from_value(encoded).unwrap();
                assert_eq!(decoded, category);
                assert!(decoded.source().is_none());
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                let encoded = tonic::Status::from(&cloned);
                let canonical = tonic::Status::from(&category);
                assert_eq!(encoded.code(), canonical.code());
                assert_eq!(encoded.message(), canonical.message());
                assert_eq!(encoded.details(), canonical.details());
            }
        }
        let scheduled =
            StreamError::not_before(73).with_source("invalid".parse::<u64>().unwrap_err());
        assert_eq!(scheduled.not_before_authority(), Some(73));
        assert_eq!(scheduled.category(), StreamError::OutOfRange);
    }

    #[test]
    fn boxed_native_causes_survive_cloning_without_changing_classification() {
        use std::error::Error as _;
        for category in StreamError::ALL {
            let native: Box<dyn std::error::Error + Send + Sync + 'static> =
                Box::new(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
            let original = category.clone().with_boxed_source(native);
            let cloned = original.clone();
            assert!(std::ptr::eq(
                original.source().unwrap().source().unwrap(),
                cloned.source().unwrap().source().unwrap(),
            ));
            assert_eq!(original, cloned);
            drop(original);
            let cause = cloned
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .expect("original native error type");
            assert_eq!(cause.kind(), std::io::ErrorKind::BrokenPipe);
            assert_eq!(cloned.category(), category);
            assert_eq!(cloned.code(), category.code());
            assert_eq!(cloned.uncertain(), category.uncertain());
            #[cfg(feature = "serde")]
            assert_eq!(
                serde_json::to_value(&cloned).unwrap(),
                serde_json::to_value(&category).unwrap()
            );
        }
    }

    #[test]
    fn wire_codes_are_pinned_and_round_trip() {
        let codes = StreamError::ALL.map(|error| error.code());
        assert_eq!(
            codes,
            [
                "invalid_response",
                "invalid_path",
                "invalid_argument",
                "limit_exceeded",
                "not_found",
                "already_exists",
                "prefix_not_retained",
                "out_of_range",
                "idempotency_mismatch",
                "capacity",
                "access_denied",
                "unavailable",
                "hierarchy_changed",
                "deadline_elapsed",
                "unsupported",
                "revision_foreign"
            ]
        );
        for error in StreamError::ALL {
            assert_eq!(StreamError::from_code(error.code()), Some(error.clone()));
            #[cfg(feature = "serde")]
            {
                let encoded = serde_json::to_value(&error).unwrap();
                assert_eq!(encoded, error.code());
                // An owning envelope is not necessarily a borrowed JSON slice.
                assert_eq!(
                    serde_json::from_value::<StreamError>(encoded).unwrap(),
                    error
                );
            }
        }
        assert_eq!(StreamError::from_code("stream_not_found"), None);
        #[cfg(feature = "serde")]
        for invalid in [
            serde_json::json!("stream_not_found"),
            serde_json::json!("unknown"),
            serde_json::json!({ "code": "unavailable" }),
            serde_json::json!(12),
        ] {
            assert!(serde_json::from_value::<StreamError>(invalid).is_err());
        }
    }

    #[test]
    fn session_close_codes_keep_refusals_typed() {
        for error in StreamError::ALL {
            assert_eq!(StreamError::from_session_close(error.code()), error);
        }
        for (code, error) in [
            ("unimplemented", StreamError::Unsupported),
            ("permission_denied", StreamError::AccessDenied),
            ("forbidden", StreamError::AccessDenied),
            ("revoked", StreamError::AccessDenied),
            ("credential_expired", StreamError::AccessDenied),
            ("unauthenticated", StreamError::AccessDenied),
            ("filter_invalid", StreamError::InvalidArgument),
            ("resource_exhausted", StreamError::Capacity),
            ("invalid_argument", StreamError::InvalidArgument),
            ("not_found", StreamError::NotFound),
        ] {
            assert_eq!(StreamError::from_session_close(code), error, "{code}");
        }
        // Negative control: only an unknown code is unavailability.
        for code in ["internal", "unknown", "", "stream_not_found"] {
            assert_eq!(
                StreamError::from_session_close(code),
                StreamError::Unavailable,
                "{code}"
            );
        }
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use futures::StreamExt as _;

    /// Delegates to real memory and injects missing or extra records.
    struct Gapped {
        inner: MemoryStream,
        missing: u64,
        extra_records: u32,
        inspect_on_write: bool,
        pause_at: usize,
        pause_write: Option<Arc<std::sync::atomic::AtomicUsize>>,
        resume_write: Option<std::sync::Mutex<Option<futures::channel::oneshot::Receiver<()>>>>,
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl StreamProvider for Gapped {
        fn context(&self) -> crate::ProviderContext<'_> {
            crate::ProviderContext::new(self.inner.context().operation_claims())
        }

        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }
        async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
            self.inner.heads(request).await
        }

        fn write(
            &self,
            request: crate::WriteRequest,
        ) -> crate::BoxProviderFuture<
            '_,
            std::result::Result<crate::WriteOutcome, crate::StreamError>,
        > {
            if self.pause_write.as_ref().is_some_and(|writes| {
                writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == self.pause_at
            }) {
                if let Some(resume) = &self.resume_write {
                    let receiver = resume.lock().unwrap().take().unwrap();
                    return Box::pin(async move {
                        receiver
                            .await
                            .map_err(|error| StreamError::Unavailable.with_source(error))?;
                        self.inner.write(request).await
                    });
                }
                return Box::pin(std::future::pending());
            }
            if self.inspect_on_write {
                return Box::pin(async move {
                    Stream::new(Arc::new(self.inner.clone()), "diagnostic-probe")?
                        .head()
                        .await?;
                    self.inner.write(request).await
                });
            }
            self.inner.write(request)
        }

        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            let extra = if self.extra_records == 0 {
                None
            } else {
                let mut extra = request.clone();
                extra.from = extra
                    .from
                    .checked_add(u64::from(extra.limit))
                    .ok_or(StreamError::LimitExceeded)?;
                extra.limit = self.extra_records;
                Some(self.inner.read(extra).await?)
            };
            let missing = self.missing;
            let records = self.inner.read(request).await?;
            Ok(records.map_cursor(|records| {
                Box::pin(
                    records
                        .chain(futures::stream::iter(extra).flatten())
                        .filter(move |record| {
                            std::future::ready(
                                !matches!(record, Ok(record) if record.sequence == missing),
                            )
                        }),
                )
            }))
        }
        async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
            self.inner.follow(request).await
        }

        async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
            self.inner.read_commit(commit_id).await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl crate::NativeStreamProvider for Gapped {}

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn portable_success_cannot_substitute_for_native_admission()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering;
        let provider = Gapped {
            inner: MemoryStream::default(),
            missing: u64::MAX,
            extra_records: 0,
            inspect_on_write: false,
            pause_at: 1,
            pause_write: None,
            resume_write: None,
        };
        let path = StreamPath::new("native-refusal")?;
        let request = AppendRequest {
            path: path.clone(),
            records: vec!["opaque".into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        };
        let meter = Arc::new(crate::test_meter::Meter::default());
        assert!(matches!(
            provider
                .native_append(request.clone(), meter.original())
                .await?,
            Err(StreamError::Unsupported)
        ));
        assert_eq!(
            provider.tail(path.clone()).await,
            Err(StreamError::NotFound)
        );
        assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        // A fallback to the ordinary path would succeed and mutate the stream.
        assert!(matches!(
            provider.append(request).await?,
            AppendOutcome::Committed(_)
        ));
        assert_eq!(provider.tail(path).await?, 1);
        Ok(())
    }

    async fn filled<P: StreamProvider>(provider: P, records: u64) -> Stream<P> {
        let stream = Stream::new(Arc::new(provider), "replay").unwrap();
        let mut next = 0;
        while next < records {
            let batch = (next..records.min(next + 500))
                .map(|sequence| crate::AppendRecord::from(sequence.to_be_bytes().to_vec()))
                .collect::<Vec<_>>();
            next += batch.len() as u64;
            stream
                .append_batch(
                    batch,
                    None,
                    IdempotencyKey::new(next.to_be_bytes().to_vec()).unwrap(),
                )
                .await
                .unwrap();
        }
        stream
    }

    async fn drain<P: StreamProvider>(replay: &mut Replay<P>) -> Result<Vec<u64>, StreamError> {
        let mut sequences = Vec::new();
        while let Some(page) = replay.next_page().await? {
            sequences.extend(page.into_iter().map(|record| record.sequence));
        }
        Ok(sequences)
    }

    #[tokio::test]
    async fn missing_stream_replays_as_empty() {
        let stream = Stream::new(
            Arc::new(MemoryStream::new(MemoryLimits::default())),
            "absent",
        )
        .unwrap();
        let mut replay = stream.replay(0);
        assert!(drain(&mut replay).await.unwrap().is_empty());
        assert_eq!(replay.cursor(), 0);
        assert!(replay.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn append_helper_recovers_accepted_append_after_reopen_and_keeps_conflicts_unspent() {
        let provider = Arc::new(MemoryStream::default());
        let first_key = IdempotencyKey::new(b"first".as_slice()).unwrap();
        let path = StreamPath::new("append-helper").unwrap();
        // The response is lost after the existing provider has accepted it.
        let first = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec!["first".into()],
                if_tail: None,
                idempotency_key: first_key.clone(),
            })
            .await
            .unwrap();
        let authority = provider.revisions.current();
        let restored = Arc::new(
            MemoryStream::default().with_authority(authority.scope, authority.incarnation),
        );
        restored
            .install_state(&provider.encode_state().await)
            .await
            .unwrap();
        let provider = restored;
        let stream = Stream::new(provider.clone(), path.as_str()).unwrap();
        assert_eq!(
            stream.append("first", first_key.clone()).await.unwrap(),
            first
        );
        let observed = provider
            .inspect_idempotency(first_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.outcome, IdempotencyOutcome::Append(first));
        let key = IdempotencyKey::new(b"retry".as_slice()).unwrap();
        assert_eq!(
            stream.append_at("second", 0, key.clone()).await.unwrap(),
            AppendOutcome::TailConflict { actual_tail: 1 }
        );
        assert!(
            provider
                .inspect_idempotency(key.clone())
                .await
                .unwrap()
                .is_none()
        );
        let accepted = stream.append_at("second", 1, key.clone()).await.unwrap();
        assert_eq!(
            stream.append_at("second", 1, key.clone()).await.unwrap(),
            accepted
        );
        assert_eq!(
            stream.append_at("changed", 1, key).await,
            Err(StreamError::IdempotencyMismatch)
        );
        assert_eq!(provider.head(stream.path().clone()).await.unwrap().tail, 2);
    }

    #[tokio::test]
    async fn replay_crosses_pages_in_order_from_any_cursor() {
        let stream = filled(MemoryStream::new(MemoryLimits::default()), 2_500).await;
        let mut replay = stream.replay(0);
        assert_eq!(
            drain(&mut replay).await.unwrap(),
            (0..2_500).collect::<Vec<_>>()
        );
        assert_eq!(replay.cursor(), 2_500);
        let mut replay = stream.replay(1_500);
        assert_eq!(
            drain(&mut replay).await.unwrap(),
            (1_500..2_500).collect::<Vec<_>>()
        );
        let tail = stream.tail().await.unwrap();
        let mut replay = stream.replay(0).until(tail);
        let first = replay.next_page().await.unwrap().unwrap();
        assert!(
            replay.cursor() < tail,
            "append must occur between replay pages"
        );
        stream
            .append("later", IdempotencyKey::unique())
            .await
            .unwrap();
        let mut prefix = first
            .into_iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>();
        prefix.extend(drain(&mut replay).await.unwrap());
        assert_eq!(prefix, (0..tail).collect::<Vec<_>>());
        assert_eq!(replay.cursor(), tail);
        assert!(replay.next_page().await.unwrap().is_none());
        for end in [0, 1_499, 1_500] {
            assert!(
                drain(&mut stream.replay(1_500).until(end))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(drain(&mut stream.replay(tail)).await.unwrap(), [tail]);
        assert_eq!(
            drain(&mut stream.replay(0).until(tail + 2))
                .await
                .map_err(|error| error.category()),
            Err(StreamError::Unavailable)
        );
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn sdk_reads_inherit_owner_without_recursive_diagnostics()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let inner = MemoryStream::default();
        inner
            .append(AppendRequest {
                path: StreamPath::new("diagnostic-probe")?,
                records: vec!["original probe".into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let provider = Arc::new(Gapped {
            inner,
            missing: u64::MAX,
            extra_records: 0,
            inspect_on_write: true,
            pause_at: 1,
            pause_write: None,
            resume_write: None,
        });
        let data = Stream::new(provider.clone(), "agents/worker/data")?;
        data.append(
            AppendRecord::latest(
                bytes::Bytes::from_static(b"original"),
                bytes::Bytes::from_static(b"selected"),
                false,
            )?,
            IdempotencyKey::unique(),
        )
        .await?;
        let original = data.head().await?;
        let trace = Stream::new(provider, "agents/worker/trace")?;
        assert!(
            matches!(trace.tail().await, Err(error) if error.category() == StreamError::NotFound)
        );
        let recorder = Arc::new(Recorder::new(trace.clone(), [15; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?;
        let parent = root.id();
        let root = root.into_owned();
        assert_eq!(root.within(data.head()).await?, original);
        let mut rows = root.within(data.read(0, 1)).await?;
        let original_row = rows.next().await.ok_or("original bounded row missing")??;
        assert_eq!(original_row.value.as_ref(), b"original");
        assert!(rows.next().await.is_none());
        let mut selected = root
            .within(data.seek(
                ReadKeySeek::latest(bytes::Bytes::from_static(b"selected"), u64::MAX),
                0,
                1,
            ))
            .await?;
        assert_eq!(
            selected
                .next()
                .await
                .ok_or("original selected row missing")??,
            original_row
        );
        assert!(selected.next().await.is_none());
        let original_children = data.children(None, 1).await?;
        assert_eq!(
            root.within(data.children(None, 1)).await?,
            original_children
        );
        let fork_key = IdempotencyKey::unique();
        let forked = root
            .within(data.fork("agents/worker/copy", Some(1), vec![], fork_key.clone()))
            .await?;
        let retried = data
            .fork("agents/worker/copy", Some(1), vec![], fork_key)
            .await?;
        assert_eq!(forked.fork_receipt(), retried.fork_receipt());
        assert_eq!(
            forked
                .read(0, 1)
                .await?
                .next()
                .await
                .ok_or("fork lost its original selected prefix")??
                .value,
            original_row.value
        );
        assert_eq!(
            root.within(forked.append_at("refused", 0, IdempotencyKey::unique()))
                .await?,
            AppendOutcome::TailConflict { actual_tail: 1 }
        );
        let append_key = IdempotencyKey::unique();
        let appended = root
            .within(forked.append("next", append_key.clone()))
            .await?;
        assert!(matches!(appended, AppendOutcome::Committed(_)));
        assert_eq!(forked.append("next", append_key).await?, appended);
        assert_eq!(forked.tail().await?, 2);
        let mut live = root.within(data.follow(0, Default::default())).await?;
        assert_eq!(
            live.next()
                .await
                .ok_or("original followed row missing")??
                .value
                .as_ref(),
            b"original"
        );
        drop(live);
        let mut diagnostic_live = root.within(trace.follow(0, Default::default())).await?;
        assert!(
            diagnostic_live
                .next()
                .await
                .ok_or("original diagnostic follow missing")?
                .is_ok()
        );
        drop(diagnostic_live);
        let mut diagnostic = root.within(trace.read(0, 1)).await?;
        assert!(
            diagnostic
                .next()
                .await
                .ok_or("original diagnostic record missing")?
                .is_ok()
        );
        let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        query.next().await?;
        for (name, guarantee, bytes) in [
            ("stream.head", "atomic_history_durability_origin", None),
            ("stream.read_open", "bounded_ordered_record_cursor", None),
            ("stream.record_delivery", "ordered_cursor_delivery", Some(8)),
            ("stream.record_delivery", "ordered_cursor_delivery", Some(0)),
            ("stream.seek_open", "bounded_header_key_selection", None),
            ("stream.record_delivery", "ordered_cursor_delivery", Some(8)),
            ("stream.record_delivery", "ordered_cursor_delivery", Some(0)),
            (
                "stream.children_page",
                "bounded_direct_children_at_namespace_cut",
                None,
            ),
            ("stream.fork", "immutable_selected_prefix_and_origin", None),
            ("stream.append", "atomic_contiguous_append_decision", None),
            ("stream.append", "atomic_contiguous_append_decision", None),
            (
                "stream.follow_open",
                "ordered_history_and_live_cursor",
                None,
            ),
            ("stream.record_delivery", "ordered_cursor_delivery", Some(8)),
        ] {
            query.next().await?;
            assert!(
                matches!(query.next().await?, Some(Observation::Completed { start, bytes: observed_bytes, result: Outcome::Succeeded, .. })
                if start.name == name && start.parent == Some(parent)
                    && start.guarantee.as_deref() == Some(guarantee) && observed_bytes == bytes)
            );
        }
        assert!(
            matches!(query.next().await?, Some(Observation::Completed { id, .. }) if id == parent)
        );
        assert_eq!(
            trace.tail().await?,
            28,
            "diagnostic publication recursively traced its own head check"
        );
        assert_eq!(data.head().await?, original);
        drop(data.read(0, 1).await?);
        assert_eq!(
            trace.tail().await?,
            28,
            "completed scope leaked its owner into unrelated work"
        );
        Ok(())
    }
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn ready_sdk_work_does_not_wait_for_diagnostic_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (release, resume) = futures::channel::oneshot::channel::<()>();
        let trace = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: Some(writes),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "agents/worker/trace",
        )?;
        let data = Stream::new(Arc::new(MemoryStream::default()), "agents/worker/data")?;
        data.append(b"original".to_vec(), IdempotencyKey::unique())
            .await?;
        let original = data.head().await?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [16; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        let parent = root.id();
        let mut work = Box::pin(root.within(data.head()));
        let std::task::Poll::Ready(result) = futures::poll!(&mut work) else {
            return Err("diagnostic admission delayed an original ready result".into());
        };
        assert_eq!(result?, original);
        drop(work);
        let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        release
            .send(())
            .map_err(|_| "submitted start lost its original producer")?;
        let mut completed = 0;
        let mut child = false;
        while completed < 2 {
            if let Some(Observation::Completed { start, result, .. }) = query.next().await? {
                completed += 1;
                assert_eq!(result, Outcome::Succeeded);
                if start.name == "stream.head" {
                    assert_eq!(start.parent, Some(parent));
                    assert_eq!(
                        start.guarantee.as_deref(),
                        Some("atomic_history_durability_origin")
                    );
                    child = true;
                }
            }
        }
        assert!(child, "ready work lost its diagnostic start or terminal");
        assert_eq!(trace.tail().await?, 4);
        // The original start consumes the one configured record reservation.
        // Failed child admission must not issue a detached terminal append.
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let refused = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::new(MemoryLimits {
                    records: Some(1),
                    ..Default::default()
                }),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: usize::MAX,
                pause_write: Some(writes.clone()),
                resume_write: None,
            }),
            "trace",
        )?;
        let recorder = Arc::new(Recorder::new(refused.clone(), [27; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        assert_eq!(root.within(data.head()).await?, original);
        assert_eq!(
            writes.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "failed start attempted a detached terminal append"
        );
        assert_eq!(refused.tail().await?, 1);
        drop(root);
        Ok(())
    }
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn ready_cursor_record_is_not_held_by_diagnostic_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let (release, resume) = futures::channel::oneshot::channel::<()>();
        let trace = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 3,
                pause_write: Some(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "agents/worker/trace",
        )?;
        let data = Stream::new(Arc::new(MemoryStream::default()), "agents/worker/data")?;
        data.append(b"original".to_vec(), IdempotencyKey::unique())
            .await?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [17; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        let parent = root.id();
        let mut records = root.within(data.read(0, 1)).await?;
        let mut next = Box::pin(records.next());
        let std::task::Poll::Ready(record) = futures::poll!(&mut next) else {
            return Err("diagnostic admission retained an original ready record".into());
        };
        assert_eq!(
            record.ok_or("original record missing")??.value.as_ref(),
            b"original"
        );
        drop(next);
        drop(records);
        let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
        release
            .send(())
            .map_err(|_| "record delivery start lost its producer")?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        let mut completed = 0;
        let mut delivered = false;
        while completed < 3 {
            if let Some(Observation::Completed {
                start,
                result,
                bytes,
                ..
            }) = query.next().await?
            {
                completed += 1;
                assert_eq!(result, Outcome::Succeeded);
                if start.name == "stream.record_delivery" {
                    assert_eq!(start.parent, Some(parent));
                    assert_eq!(bytes, Some(8));
                    delivered = true;
                }
            }
        }
        assert!(delivered, "ready delivery lost its start or terminal");
        assert_eq!(trace.tail().await?, 6);
        Ok(())
    }
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn cancelled_inherited_wait_retains_pending_start_and_original_follow()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, Request, TraceQuery};
        use futures::TryStreamExt as _;
        let (release, resume) = futures::channel::oneshot::channel::<()>();
        let trace = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: Some(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "trace",
        )?;
        let data = Stream::new(Arc::new(MemoryStream::default()), "data")?;
        data.append("before", IdempotencyKey::unique()).await?;
        let mut records = data.follow(1, Default::default()).await?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [26; 16]));
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        let parent = root.id();
        let mut waiting = Box::pin(root.within(crate::trace::observe_value(
            Request {
                name: "original.follow",
                mechanism: Mechanism::Wait,
                parent: None,
                guarantee: Some("original_record_delivery"),
            },
            records.next(),
            |_| None,
        )));
        assert!(futures::poll!(&mut waiting).is_pending());
        drop(waiting);
        let _terminal = root.finish_submitted(None, Outcome::Succeeded)?;
        data.append("after", IdempotencyKey::unique()).await?;
        assert_eq!(
            records
                .try_next()
                .await?
                .ok_or("original follow lost delivery")?
                .value
                .as_ref(),
            b"after"
        );
        release
            .send(())
            .map_err(|_| "cancelled observer withdrew the original start")?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        let mut completed = 0;
        let mut unsettled = false;
        while completed < 2 {
            if let Some(Observation::Completed {
                start,
                result,
                bytes,
                ..
            }) = query.next().await?
            {
                completed += 1;
                if start.name == "original.follow" {
                    assert_eq!(start.parent, Some(parent));
                    assert_eq!(start.guarantee.as_deref(), Some("original_record_delivery"));
                    assert_eq!(result, Outcome::Unsettled);
                    assert_eq!(bytes, None);
                    unsettled = true;
                }
            }
        }
        assert!(unsettled);
        assert_eq!(query.totals()[&Mechanism::Wait].unsettled, 1);
        assert_eq!(trace.tail().await?, 4);
        Ok(())
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn stalled_trace_publication_reuses_credit_without_retaining_ready_work()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let (release, resume) = futures::channel::oneshot::channel::<()>();
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let trace = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: Some(writes.clone()),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "trace",
        )?;
        let data = Stream::new(Arc::new(MemoryStream::default()), "data")?;
        data.append("original", IdempotencyKey::unique()).await?;
        let original = data.head().await?;
        let recorder = Arc::new(
            Recorder::new(trace.clone(), [30; 16])
                .with_publication_credit(std::num::NonZeroUsize::new(1).ok_or("no credit")?)?,
        );
        let root = recorder
            .start(
                "request",
                Mechanism::ToolCall,
                None,
                Some("original_request"),
            )
            .await?
            .into_owned();
        for _ in 0..8 {
            let mut work = Box::pin(root.within(data.head()));
            let std::task::Poll::Ready(result) = futures::poll!(&mut work) else {
                return Err("publication credit delayed ready work".into());
            };
            assert_eq!(result?, original);
        }
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(recorder.suppressed_spans(), 7);
        release
            .send(())
            .map_err(|_| "credited start lost its producer")?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        loop {
            if let Some(Observation::Completed { start, result, .. }) = query.next().await? {
                assert_eq!(start.name, "stream.head");
                assert_eq!(result, Outcome::Succeeded);
                break;
            }
        }
        assert_eq!(root.within(data.head()).await?, original);
        assert_eq!(
            recorder.suppressed_spans(),
            7,
            "completed publication did not return credit"
        );
        let terminal = root.finish_submitted(None, Outcome::Succeeded)?;
        terminal.result().await?.as_ref().map_err(Clone::clone)?;
        let mut completed = 1;
        while completed < 3 {
            if matches!(query.next().await?, Some(Observation::Completed { .. })) {
                completed += 1;
            }
        }
        let tail = trace.tail().await?;
        assert_eq!(tail, 8);
        let mut replay = TraceQuery::new(trace.read(0, 8).await?, Default::default());
        while replay.next().await?.is_some() {}
        assert_eq!(
            replay
                .snapshot(0, tail, Some(tail), 0)
                .largest_reported_suppression,
            7
        );
        Ok(())
    }

    #[cfg(all(feature = "serde", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn original_append_retry_publishes_comparison_in_its_terminal_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{ComparisonKind, Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let provider = Arc::new(MemoryStream::default());
        let path = StreamPath::new("data")?;
        let request = AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"original").into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        };
        let original = provider.append(request.clone()).await?;
        let AppendOutcome::Committed(receipt) = &original else {
            return Err("original append refused".into());
        };
        let trace = Stream::new(
            Arc::new(MemoryStream::default()),
            StreamPath::new("owner/trace")?,
        )?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [35; 16]));
        let root = recorder
            .start(
                "retry_owner",
                Mechanism::ActorDelivery,
                None,
                Some("original_request_observed"),
            )
            .await?;
        let parent = root.id();
        let root = root.into_owned();
        assert_eq!(
            root.within(provider.append(request.clone())).await?,
            original
        );
        root.finish_submitted(None, Outcome::Succeeded)?
            .result()
            .await?
            .as_ref()
            .map_err(Clone::clone)?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        let mut completed = 0;
        let mut terminal = None;
        let mut compared = false;
        while completed != 2 || !compared {
            match query.next().await? {
                Some(Observation::Completed {
                    id,
                    start,
                    result,
                    finish,
                    bytes,
                    ..
                }) => {
                    assert_eq!(result, Outcome::Succeeded);
                    if id != parent {
                        assert_eq!(start.name, "stream.append_replay");
                        assert_eq!(start.parent, Some(parent));
                        assert_eq!(
                            start.guarantee.as_deref(),
                            Some("same_request_reuses_original_append_receipt")
                        );
                        assert_eq!(bytes, Some(0));
                        terminal = Some((id, finish));
                    }
                    completed += 1;
                }
                Some(Observation::Compared {
                    id,
                    evidence,
                    comparison,
                    identical,
                }) => {
                    let (span, finish) = terminal
                        .as_ref()
                        .ok_or("comparison preceded its own terminal")?;
                    assert_eq!(id, *span);
                    assert_eq!(evidence.commit_id, finish.commit_id);
                    assert_eq!(evidence.sequence, finish.sequence + 1);
                    assert_eq!(comparison.kind, ComparisonKind::Request);
                    assert!(identical);
                    assert_eq!(comparison.previous.commit_id, receipt.commit_id);
                    assert_eq!(comparison.previous.path, path);
                    assert_eq!(comparison.previous.sequence, receipt.start);
                    compared = true;
                }
                _ => {}
            }
        }
        assert_eq!(query.snapshot(0, 5, Some(5), 0).repeated_calls, 1);
        assert_eq!(trace.tail().await?, 5);
        assert_eq!(provider.append(request).await?, original);
        assert_eq!(
            trace.tail().await?,
            5,
            "unobserved retry emitted diagnostics"
        );
        let mut rows = provider
            .read(ReadRequest {
                path,
                from: receipt.start,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let row = rows
            .try_next()
            .await?
            .ok_or("original comparison evidence disappeared")?;
        assert_eq!(row.commit_id, receipt.commit_id);
        assert_eq!(row.value.as_ref(), b"original");
        Ok(())
    }

    #[cfg(all(feature = "serde", not(target_arch = "wasm32")))]
    #[test]
    fn dropped_owned_span_uses_original_executor_outside_runtime_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (release, resume) = futures::channel::oneshot::channel::<()>();
        let trace = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: Some(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "agents/worker/trace",
        )?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [19; 16]));
        let recording = runtime
            .block_on(recorder.start(
                "original.wait",
                Mechanism::Wait,
                None,
                Some("original_delivery"),
            ))?
            .into_owned();
        // The terminal really suspends; eager completion alone cannot hide a
        // missing executor after leaving its original runtime scope.
        drop(recording);
        assert!(release.send(()).is_ok());
        runtime.block_on(async {
            let mut query = TraceQuery::new(
                trace.follow(0, Default::default()).await?,
                Default::default(),
            );
            query.next().await?;
            assert!(matches!(
                query.next().await?,
                Some(Observation::Completed {
                    result: Outcome::Unsettled,
                    ..
                })
            ));
            assert_eq!(trace.tail().await?, 2);
            Ok::<_, crate::StreamError>(())
        })?;
        Ok(())
    }
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn observed_dispatch_handoff_cannot_wait_after_original_claim()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{
            Mechanism, Observation, Outcome, Recorder, Request, Settlement, TraceQuery,
        };
        let (release, resume) = futures::channel::oneshot::channel();
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let traces = Stream::new(
            Arc::new(Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 0,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: Some(writes.clone()),
                resume_write: Some(std::sync::Mutex::new(Some(resume))),
            }),
            "trace",
        )?;
        let recorder = Arc::new(Recorder::new(traces.clone(), [14; 16]));
        let claims = Stream::new(Arc::new(MemoryStream::default()), "claims")?;
        let mut handoff = Box::pin(Recorder::handoff(
            Some(&recorder),
            Request {
                name: "dispatch.claim",
                mechanism: Mechanism::Commit,
                parent: None,
                guarantee: Some("original_dispatch_claim"),
            },
            claims.append(b"original".to_vec(), IdempotencyKey::unique()),
        ));
        let std::task::Poll::Ready(observed) = futures::poll!(&mut handoff) else {
            return Err("diagnostic finish suspended an accepted claim".into());
        };
        assert!(matches!(observed.result?, AppendOutcome::Committed(_)));
        let Settlement::Pending(recording) = observed.observation? else {
            return Err("dispatch handoff fabricated completion".into());
        };
        let mut terminal = traces.follow(1, Default::default()).await?;
        let pending = recording
            .into_owned()
            .finish_submitted(None, Outcome::Succeeded)?;
        let retry = pending.request().clone();
        drop(pending);
        release
            .send(())
            .map_err(|_| "terminal producer was cancelled")?;
        terminal.next().await.ok_or("terminal follow ended")??;
        assert!(matches!(
            traces.provider.append(retry).await?,
            AppendOutcome::Committed(_)
        ));
        assert_eq!(claims.tail().await?, 1);
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 3);
        let mut query = TraceQuery::new(traces.read(0, 2).await?, Default::default());
        query.next().await?;
        assert!(
            matches!(query.next().await?, Some(Observation::Completed { start, .. })
            if start.name == "dispatch.claim")
        );
        assert_eq!(query.active().len(), 0);
        assert_eq!(traces.tail().await?, 2);
        Ok(())
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn replay_observation_preserves_results_parentage_and_unretained_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::trace::{Mechanism, Observation, Recorder, Settlement, SpanId, TraceQuery};
        use std::error::Error as _;
        let provider = Arc::new(MemoryStream::default());
        let data = Stream::new(provider.clone(), "agents/worker/data")?;
        data.append(b"original payload".to_vec(), IdempotencyKey::unique())
            .await?;
        let traces = Stream::new(provider, "agents/worker/trace")?;
        let recorder = Arc::new(Recorder::new(traces.clone(), [3; 16]));
        let parent = SpanId {
            process: [1; 16],
            sequence: 7,
        };
        let mut replay = data.replay(0);
        let observed = replay
            .next_page_observed(Some(&recorder), Some(parent))
            .await;
        let rows = observed.result?.ok_or("original page missing")?;
        assert_eq!(rows[0].value.as_ref(), b"original payload");
        assert!(matches!(observed.observation?, Settlement::Finished(_)));
        let mut query = TraceQuery::new(traces.read(0, 8).await?, Default::default());
        let Some(Observation::Started { id: page_id, .. }) = query.next().await? else {
            return Err("replay parent admission missing".into());
        };
        query.next().await?;
        assert!(
            matches!(query.next().await?, Some(Observation::Completed { start, bytes: None, .. })
            if start.name == "stream.read_open" && start.parent == Some(page_id))
        );
        for expected in [Some(16), Some(0)] {
            query.next().await?;
            assert!(
                matches!(query.next().await?, Some(Observation::Completed { start, bytes, .. })
                if start.name == "stream.record_delivery" && start.parent == Some(page_id) && bytes == expected)
            );
        }
        assert!(matches!(query.next().await?, Some(Observation::Completed {
            start, bytes: Some(16), ..
        }) if start.parent == Some(parent) && start.mechanism == Mechanism::Io
            && start.guarantee.as_deref() == Some("gapless_committed_prefix")));
        let disabled = replay.next_page_observed(None, None).await;
        assert!(disabled.result?.is_none());
        assert!(matches!(disabled.observation?, Settlement::Disabled));
        assert_eq!(traces.tail().await?, 8);

        let mut incomplete = data.replay(1).until(2);
        let observed = incomplete
            .next_page_observed(Some(&recorder), Some(parent))
            .await;
        let error = observed
            .result
            .err()
            .ok_or("incomplete prefix was accepted")?;
        assert_eq!(error.category(), StreamError::Unavailable);
        let mut source = error.source();
        let mut found = false;
        while let Some(cause) = source {
            found |= matches!(
                cause.downcast_ref::<ReplayViolation>(),
                Some(ReplayViolation::IncompletePrefix { cursor: 1, end: 2 })
            );
            source = cause.source();
        }
        assert!(found, "original replay invariant cause was lost");
        let Settlement::Pending(recording) = observed.observation? else {
            return Err("unretained failure fabricated a terminal".into());
        };
        drop(recording);
        let mut query = TraceQuery::new(traces.read(8, 5).await?, Default::default());
        query.next().await?;
        assert_eq!(query.active().len(), 1);
        while query.next().await?.is_some() {}
        assert_eq!(query.active().len(), 1);
        assert_eq!(query.totals()[&Mechanism::Io].completed, 1);
        assert_eq!(query.totals()[&Mechanism::Wait].completed, 1);

        // A real producer-capacity refusal must not replace the successful
        // read, consume its payload or turn its cursor into a retry.
        let provider = Arc::new(MemoryStream::new(MemoryLimits {
            paths: Some(1),
            ..Default::default()
        }));
        let data = Stream::new(provider.clone(), "data")?;
        data.append(b"retained".to_vec(), IdempotencyKey::unique())
            .await?;
        let recorder = Arc::new(Recorder::new(Stream::new(provider, "trace")?, [4; 16]));
        let mut replay = data.replay(0);
        let observed = replay.next_page_observed(Some(&recorder), None).await;
        assert_eq!(
            observed.result?.ok_or("successful read was lost")?[0]
                .value
                .as_ref(),
            b"retained"
        );
        assert_eq!(replay.cursor(), 1);
        assert!(
            matches!(observed.observation, Err(error) if error.category() == StreamError::Capacity)
        );

        // Pause the page terminal after its child cursor admission has settled.
        // Dropping the observed future must not commit an undelivered cursor.
        let inner = MemoryStream::default();
        inner
            .append(AppendRequest {
                path: StreamPath::new("data")?,
                records: vec!["not skipped".into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = Arc::new(Gapped {
            inner,
            missing: u64::MAX,
            extra_records: 0,
            inspect_on_write: false,
            pause_at: 7,
            pause_write: Some(writes.clone()),
            resume_write: None,
        });
        let data = Stream::new(provider.clone(), "data")?;
        let traces = Stream::new(provider, "trace")?;
        let recorder = Arc::new(Recorder::new(traces.clone(), [5; 16]));
        let mut replay = data.replay(0);
        let mut cancelled = Box::pin(replay.next_page_observed(Some(&recorder), None));
        assert!(futures::poll!(&mut cancelled).is_pending());
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 8);
        drop(cancelled);
        assert_eq!(replay.cursor(), 0);
        assert_eq!(traces.tail().await?, 7);
        let retried = replay.next_page_observed(Some(&recorder), None).await;
        assert_eq!(
            retried.result?.ok_or("cancelled read skipped page")?[0]
                .value
                .as_ref(),
            b"not skipped"
        );
        assert!(matches!(retried.observation?, Settlement::Finished(_)));
        assert_eq!(replay.cursor(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn gaps_within_and_between_pages_fail_closed() {
        for (missing, cursor) in [(7, 0), (u64::from(REPLAY_PAGE), u64::from(REPLAY_PAGE))] {
            let inner = MemoryStream::new(MemoryLimits::default());
            let stream = filled(
                Gapped {
                    inner,
                    missing,
                    extra_records: 0,
                    inspect_on_write: false,
                    pause_at: 1,
                    pause_write: None,
                    resume_write: None,
                },
                2_100,
            )
            .await;
            let mut replay = stream.replay(0);
            assert_eq!(
                drain(&mut replay).await.map_err(|error| error.category()),
                Err(StreamError::Unavailable)
            );
            assert_eq!(
                replay.cursor(),
                cursor,
                "a refused page advanced its cursor"
            );
            assert_eq!(
                drain(&mut stream.replay(0).until(missing)).await.unwrap(),
                (0..missing).collect::<Vec<_>>()
            );
            assert_eq!(
                drain(&mut stream.replay(0).until(missing + 1))
                    .await
                    .map_err(|error| error.category()),
                Err(StreamError::Unavailable)
            );
        }
        let stream = filled(
            Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 1,
                inspect_on_write: false,
                pause_at: 1,
                pause_write: None,
                resume_write: None,
            },
            2_500,
        )
        .await;
        for end in [None, Some(7), Some(2_500)] {
            let mut replay = stream.replay(0);
            if let Some(end) = end {
                replay = replay.until(end);
            }
            assert_eq!(
                drain(&mut replay).await.map_err(|error| error.category()),
                Err(StreamError::Unavailable)
            );
            assert_eq!(replay.cursor(), 0, "an overlong page advanced its cursor");
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod write_tests {
    use super::*;

    #[tokio::test]
    async fn canonical_write_preserves_genuine_families_and_atomic_fork_tail()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = StreamPath::new("write/source")?;
        let destination = StreamPath::new("write/fork")?;
        let append = AppendRequest {
            path: source.clone(),
            records: vec![b"first".to_vec().into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        };
        let append_request = WriteRequest::Append(append.clone());
        let appended = provider.write(append_request.clone()).await?;
        let WriteOutcome::Append(AppendOutcome::Committed(receipt)) = &appended else {
            return Err(StreamError::InvalidResponse);
        };
        assert_eq!(
            provider.append(append.clone()).await?,
            AppendOutcome::Committed(receipt.clone())
        );
        let fork_request = WriteRequest::Fork(ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: None,
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        });
        let forked = provider.write(fork_request.clone()).await?;
        let WriteOutcome::Fork(forked_receipt) = &forked else {
            return Err(StreamError::InvalidResponse);
        };
        assert_eq!(forked_receipt.forked_at, 1);
        let mut commit = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![crate::CommitEffect::hard(
                vec![CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                }],
                vec![CommitMutation::Append {
                    path: source.clone(),
                    records: vec![b"second".to_vec().into()],
                }],
            )],
            idempotency_key: IdempotencyKey::unique(),
        };
        let commit_request = WriteRequest::Commit(commit.clone());
        let committed = provider.write(commit_request.clone()).await?;
        assert!(matches!(
            &committed,
            WriteOutcome::Commit(CommitOutcome::Committed(_))
        ));
        assert_eq!(provider.tail(source).await?, 2);
        assert_eq!(provider.tail(destination).await?, 1);
        for (request, expected) in [
            (append_request, appended),
            (fork_request, forked),
            (commit_request, committed),
        ] {
            assert_eq!(provider.write(request).await?, expected);
        }
        // Reusing a key for another family must not recover or re-encode the append.
        commit.idempotency_key = append.idempotency_key;
        assert_eq!(
            provider.write(WriteRequest::Commit(commit)).await,
            Err(StreamError::IdempotencyMismatch)
        );
        Ok(())
    }
}
