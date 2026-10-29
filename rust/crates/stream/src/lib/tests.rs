#[cfg(test)]
mod error_code_tests {
    use super::StreamError;

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
            assert_eq!(StreamError::from_code(error.code()), Some(error));
        }
        assert_eq!(StreamError::from_code("stream_not_found"), None);
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
mod token_operation_tests {
    use super::TOKEN_OPERATIONS;

    #[test]
    fn inventory_is_ordered_and_unique() {
        assert_eq!(
            TOKEN_OPERATIONS,
            &[
                "list", "read", "follow", "append", "fork", "create", "commit",
            ]
        );
        let mut sorted = TOKEN_OPERATIONS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), TOKEN_OPERATIONS.len());
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use futures::StreamExt as _;

    /// Delegates to memory but drops one sequence from every read, as a
    struct Gapped {
        inner: MemoryStream,
        missing: u64,
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
            let missing = self.missing;
            let records = self.inner.read(request).await?;
            Ok(Box::pin(records.filter(move |record| {
                std::future::ready(!matches!(record, Ok(record) if record.sequence == missing))
            })))
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
    }

    #[tokio::test]
    async fn gaps_within_and_between_pages_fail_closed() {
        for missing in [7, u64::from(REPLAY_PAGE)] {
            let inner = MemoryStream::new(MemoryLimits::default());
            let stream = filled(Gapped { inner, missing }, 2_100).await;
            let mut replay = stream.replay(0);
            assert_eq!(drain(&mut replay).await, Err(StreamError::Unavailable));
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
            conditions: vec![CommitCondition::Tail {
                path: source.clone(),
                expected: 1,
            }],
            mutations: vec![CommitMutation::Append {
                path: source.clone(),
                records: vec![b"second".to_vec().into()],
            }],
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
