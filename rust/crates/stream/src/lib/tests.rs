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
        late_error: bool,
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
            let late_error = self.late_error;
            let records = self.inner.read(request).await?;
            Ok(records.map_cursor(|records| {
                Box::pin(
                    records
                        .chain(futures::stream::iter(extra).flatten())
                        .chain(futures::stream::iter(
                            late_error.then_some(Err(StreamError::Unavailable)),
                        ))
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
            late_error: false,
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
        assert_eq!(provider.tail(path.clone()).await?, 1);
        assert!(matches!(
            provider
                .native_latest_before(path.clone(), None, 1, Default::default(), meter.original())
                .await?,
            Err(StreamError::Unsupported)
        ));
        assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        assert_eq!(
            provider
                .latest_before(path, None, 1, Default::default())
                .await?
                .map(|record| record.sequence),
            Some(0),
            "portable point read must remain a real fallback countercontrol"
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_latest_before_keeps_original_cuts_and_budget()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering;
        let provider = MemoryStream::default();
        let path = StreamPath::new("native-original-point")?;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![
                    AppendRecord::latest("first", "key", false)?,
                    AppendRecord::latest("other", "other", false)?,
                    AppendRecord::latest("second", "key", false)?,
                ],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let before = 3;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![AppendRecord::latest("later", "key", false)?],
                if_tail: Some(before),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let key = Bytes::from_static(b"key");
        for (key, cut, expected) in [
            (Some(key.clone()), before, Some(2)),
            (Some(key), 1, Some(0)),
            (None, before, Some(2)),
            (Some(Bytes::from_static(b"missing")), before, None),
        ] {
            let meter = Arc::new(crate::test_meter::Meter::default());
            let original = provider
                .native_latest_before(
                    path.clone(),
                    key.clone(),
                    cut,
                    Default::default(),
                    meter.original(),
                )
                .await??;
            assert_eq!(
                original.value().as_ref().map(|record| record.sequence),
                expected,
                "native point selection borrowed a later cut"
            );
            assert_eq!(
                provider
                    .latest_before(path.clone(), key, cut, Default::default())
                    .await?
                    .map(|record| record.sequence),
                expected
            );
            drop(original);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }
        let meter = Arc::new(crate::test_meter::Meter::default());
        let absent = provider
            .native_latest_before(
                StreamPath::new("never-created")?,
                Some(Bytes::from(b"unique-zero-cut-key".to_vec())),
                0,
                Default::default(),
                meter.original(),
            )
            .await??;
        assert!(absent.value().is_none());
        assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
        drop(absent);
        let original_cut = RevisionCut::of(Revision {
            scope: RevisionScope::new(Bytes::from_static(b"original-nonempty-cut")),
            incarnation: 1,
            index: 0,
        });
        let copied = original_cut.native_clone(&meter.original())?;
        assert_eq!(copied.value(), &original_cut);
        assert!(meter.held.load(Ordering::SeqCst) > 0);
        drop(copied);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        // The operation allowance refuses with the meter's inline OOM error;
        // the separately supported diagnostic denial deliberately owns a box.
        meter
            .limit
            .store(meter.allocations.load(Ordering::SeqCst), Ordering::SeqCst);
        assert!(original_cut.native_clone(&meter.original()).is_err());
        // A unique ordinary key would otherwise lazily allocate its shared
        // header on clone before reaching the provider's native admission.
        let unique_key = Some(Bytes::from(b"unique-unshared-key".to_vec()));
        let mut refused_key = None;
        let physical_key = acyclic_test_support::measure(|| {
            use futures::FutureExt as _;
            refused_key = Some(
                provider
                    .native_latest_before(
                        path.clone(),
                        unique_key,
                        before,
                        Default::default(),
                        meter.original(),
                    )
                    .now_or_never(),
            );
        });
        assert!(matches!(refused_key, Some(Some(Err(_)))));
        assert_eq!(
            physical_key.count_total, 0,
            "native point read promoted a unique key before admission"
        );
        let mut refused = None;
        let physical = acyclic_test_support::measure(|| {
            use futures::FutureExt as _;
            refused = Some(
                provider
                    .native_latest_before(
                        path.clone(),
                        None,
                        before,
                        original_cut,
                        meter.original(),
                    )
                    .now_or_never(),
            );
        });
        assert!(
            matches!(refused, Some(Some(Err(_)))),
            "native point read bypassed its original allocation denial"
        );
        assert_eq!(
            physical.count_total, 0,
            "native point read copied its revision vector before admission"
        );
        meter.assert_refunded();
        meter.limit.store(u64::MAX, Ordering::SeqCst);
        meter.cancelled.store(true, Ordering::SeqCst);
        assert!(
            provider
                .native_latest_before(path, None, 0, Default::default(), meter.original(),)
                .await
                .is_err(),
            "cancelled native point read admitted absence"
        );
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn latest_before_requires_the_original_single_record_cursor_to_close() {
        for (extra_records, late_error, expected) in [
            (0, false, None),
            (1, false, Some(StreamError::InvalidResponse)),
            (0, true, Some(StreamError::Unavailable)),
        ] {
            let provider = Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records,
                late_error,
            };
            let path = StreamPath::new("original-point-close").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec!["first".into(), "later".into()],
                    if_tail: Some(0),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
                .unwrap();
            let result = provider
                .latest_before(path, None, 1, Default::default())
                .await;
            match expected {
                Some(expected) => assert_eq!(
                    result.err(),
                    Some(expected),
                    "point read ignored an extra record or a late cursor error"
                ),
                None => assert_eq!(result.unwrap().map(|record| record.sequence), Some(0)),
            }
        }
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
        assert_original_replay_failure(drain(&mut stream.replay(0).until(tail + 2)).await);
    }

    fn assert_original_replay_failure(result: Result<Vec<u64>, StreamError>) {
        let error = result.unwrap_err();
        assert_eq!(error.category(), StreamError::Unavailable);
        let mut cause = std::error::Error::source(&error);
        let mut original = false;
        while let Some(source) = cause {
            original |= source.downcast_ref::<ReplayViolation>().is_some();
            cause = source.source();
        }
        assert!(original, "replay lost the original prefix violation");
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
                    late_error: false,
                },
                2_100,
            )
            .await;
            let mut replay = stream.replay(0);
            assert_original_replay_failure(drain(&mut replay).await);
            assert_eq!(
                replay.cursor(),
                cursor,
                "a refused page advanced its cursor"
            );
            assert_eq!(
                drain(&mut stream.replay(0).until(missing)).await.unwrap(),
                (0..missing).collect::<Vec<_>>()
            );
            assert_original_replay_failure(drain(&mut stream.replay(0).until(missing + 1)).await);
        }
        let stream = filled(
            Gapped {
                inner: MemoryStream::default(),
                missing: u64::MAX,
                extra_records: 1,
                late_error: false,
            },
            2_500,
        )
        .await;
        for end in [None, Some(7), Some(2_500)] {
            let mut replay = stream.replay(0);
            if let Some(end) = end {
                replay = replay.until(end);
            }
            assert_original_replay_failure(drain(&mut replay).await);
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

#[cfg(all(test, feature = "serde"))]
mod indexed_trace_tests {
    use super::*;
    use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
    use std::sync::Arc;

    #[tokio::test]
    async fn indexed_lookup_preserves_original_record_cut_and_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = Arc::new(MemoryStream::default());
        let data = Stream::new(provider.clone(), "agents/worker/data")?;
        data.append(
            AppendRecord::latest(
                OwnedBytes::from_static(b"original"),
                OwnedBytes::from_static(b"selected"),
                false,
            )?,
            IdempotencyKey::unique(),
        )
        .await?;
        let original = provider
            .latest(
                data.path().clone(),
                OwnedBytes::from_static(b"selected"),
                Default::default(),
            )
            .await?;
        let original_head = provider.head(data.path().clone()).await?;
        let child_request = ChildrenPageRequest {
            tail_stream: None,
            parent: Some(data.path().clone()),
            after: None,
            hierarchy_version: None,
            limit: 1,
        };

        let trace = Stream::new(provider.clone(), "agents/worker/trace")?;
        let recorder = Arc::new(Recorder::new(trace.clone(), [51; 16]));
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
        for name in ["agents/worker/data/a", "agents/worker/data/b"] {
            Stream::new(provider.clone(), name)?
                .append(
                    AppendRecord::new(OwnedBytes::from_static(b"child"))?,
                    IdempotencyKey::unique(),
                )
                .await?;
        }
        let visibility_cut = RevisionCut::of(provider.wait_applied(&Default::default()).await?);
        let applied = root.within(provider.wait_applied(&visibility_cut)).await?;
        assert!(visibility_cut.proven_by(Some(&applied)));
        let original_children = provider.children_page(child_request.clone()).await?;
        assert_eq!(original_children.children.len(), 1);
        let mut continuation = child_request.clone();
        continuation.after = original_children.next_after.clone();
        continuation.hierarchy_version = Some(original_children.hierarchy_version);
        assert!(continuation.after.is_some());
        let next_children = provider.children_page(continuation.clone()).await?;
        assert_eq!(next_children.children.len(), 1);
        assert!(next_children.next_after.is_none());
        assert_eq!(root.within(data.head()).await?, original_head);
        assert_eq!(
            root.within(provider.head(data.path().clone())).await?,
            original_head
        );
        assert_eq!(
            root.within(provider.tail(data.path().clone())).await?,
            original_head.tail
        );
        let watermarks = root
            .within(provider.watermarks(data.path().clone()))
            .await?;
        assert_eq!(watermarks.accepted, original_head.tail);
        assert_eq!(watermarks.durable, original_head.durable);
        assert_eq!(
            root.within(data.children(None, 1)).await?,
            original_children
        );
        assert_eq!(
            root.within(provider.children_page(continuation)).await?,
            next_children
        );
        root.within(provider.children_page(ChildrenPageRequest {
            tail_stream: None,
            parent: Some(trace.path().clone()),
            after: None,
            hierarchy_version: None,
            limit: 1,
        }))
        .await?;
        root.within(provider.head(trace.path().clone())).await?;
        let (selected, condition) = root
            .within(provider.latest_fenced(
                data.path().clone(),
                Some(OwnedBytes::from_static(b"selected")),
                Default::default(),
            ))
            .await?;
        assert_eq!(selected, original);
        assert_eq!(
            condition,
            CommitCondition::Tail {
                path: data.path().clone(),
                expected: 1
            }
        );
        assert!(
            root.within(provider.latest(
                data.path().clone(),
                OwnedBytes::from_static(b"absent"),
                Default::default()
            ))
            .await?
            .is_none()
        );
        assert!(
            root.within(provider.latest_before(data.path().clone(), None, 0, Default::default()))
                .await?
                .is_none()
        );
        assert!(
            root.within(provider.latest(
                trace.path().clone(),
                OwnedBytes::from_static(b"absent"),
                Default::default()
            ))
            .await?
            .is_none()
        );
        let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await?,
            Default::default(),
        );
        let mut completed = 0;
        let mut fence = false;
        let mut applied_wait = false;
        let mut request = false;
        let mut indexed_bytes = Vec::new();
        let mut projections = std::collections::BTreeMap::new();
        while completed < 11 {
            let Some(observation) = query.next().await? else {
                return Err("trace ended before original request hops settled".into());
            };
            if let Observation::Completed {
                id,
                start,
                bytes,
                result,
                ..
            } = observation
            {
                completed += 1;
                assert_eq!(result, Outcome::Succeeded);
                if id == parent {
                    assert!(!request);
                    request = true;
                    continue;
                }
                assert_eq!(start.parent, Some(parent));
                if start.name == "stream.wait_applied" {
                    assert!(!applied_wait);
                    applied_wait = true;
                    assert_eq!(start.mechanism, Mechanism::Wait);
                    assert_eq!(
                        start.guarantee.as_deref(),
                        Some("original_revision_cut_visible_before_return")
                    );
                    assert_eq!(bytes, None);
                    continue;
                }
                assert_eq!(start.mechanism, Mechanism::ProviderCall);
                match start.name.as_str() {
                    "stream.latest_fence" => {
                        assert!(!fence);
                        fence = true;
                        assert_eq!(bytes, None);
                        assert_eq!(
                            start.guarantee.as_deref(),
                            Some("original_head_cut_for_optimistic_publication_fence")
                        );
                    }
                    "stream.latest_before" => {
                        assert_eq!(
                            start.guarantee.as_deref(),
                            Some("indexed_record_strictly_before_original_cut")
                        );
                        indexed_bytes.push(bytes);
                    }
                    "stream.head"
                    | "stream.tail"
                    | "stream.watermarks"
                    | "stream.children_page" => {
                        let guarantee = match start.name.as_str() {
                            "stream.children_page" => "bounded_direct_children_at_namespace_cut",
                            "stream.head" => "atomic_history_durability_origin",
                            "stream.tail" => "accepted_tail_without_durability_wait",
                            _ => "accepted_visibility_and_confirmed_durability_at_one_snapshot",
                        };
                        assert_eq!(start.guarantee.as_deref(), Some(guarantee));
                        assert_eq!(bytes, None);
                        *projections.entry(start.name).or_insert(0) += 1;
                    }
                    name => panic!("unexpected indexed request hop: {name}"),
                }
            }
        }
        assert!(fence && request && applied_wait);
        indexed_bytes.sort();
        assert_eq!(indexed_bytes, [Some(0), Some(8)]);
        assert_eq!(projections["stream.children_page"], 2);
        assert_eq!(projections["stream.head"], 2);
        assert_eq!(projections["stream.tail"], 1);
        assert_eq!(projections["stream.watermarks"], 1);
        assert_eq!(
            trace.tail().await?,
            22,
            "zero cuts and diagnostic reads must not emit provider work"
        );
        assert_eq!(
            provider
                .latest(
                    data.path().clone(),
                    OwnedBytes::from_static(b"selected"),
                    Default::default()
                )
                .await?,
            original
        );
        assert_eq!(
            trace.tail().await?,
            22,
            "the request owner must not leak into unrelated work"
        );
        Ok(())
    }
}
