// Included in attestation::tests: use the same probe as the native vectors.
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn every_write_family_preserves_digest_admission_and_retry_evidence(
        payload in proptest::collection::vec(proptest::num::u8::ANY, 1..128),
        identity in proptest::collection::vec(proptest::num::u8::ANY, 1..32),
    ) {
        use super::StreamProvider as _;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        runtime.block_on(async {
            for hosted in [false, true] {
                let audience = crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation, id: "attestation-proof".into(),
                };
                let issuer = crate::core::AuthorityRoot::new("proof", [7; 32], audience.clone());
                let admission = if hosted {
                    super::TurnAdmission::Admitted {
                        audience,
                        scope: issuer.bootstrap_scope("caller", &crate::contract::Capabilities::default(), None, &mut None).expect("scope"),
                    }
                } else { super::TurnAdmission::LocalRoot };
                let attestor = std::sync::Arc::new(Counting::default());
                let probe = std::sync::Arc::new(Probe::default());
                let attested = super::Attested::new(probe.clone(), Some(attestor.clone()), admission.clone());
                let path = super::StreamPath::new("proof/source").expect("path");
                let append = super::AppendRequest {
                    path: path.clone(), records: vec![bytes::Bytes::copy_from_slice(&payload).into()],
                    if_tail: Some(0), idempotency_key: Some(super::IdempotencyKey::new(identity.clone()).expect("key")),
                };
                let fork = super::ForkRequest {
                    source: path.clone(), destination: super::StreamPath::new("proof/fork").expect("path"),
                    at_tail: Some(0), exclude: vec![], idempotency_key: append.idempotency_key.clone(),
                };
                let commit = super::CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![acyclic_stream::CommitCondition::Tail { path: path.clone(), expected: 0 }],
                    mutations: vec![acyclic_stream::CommitMutation::Append {
                        path, records: vec![bytes::Bytes::copy_from_slice(&payload).into()],
                    }],
                    idempotency_key: super::IdempotencyKey::new(identity.clone()).expect("key"),
                };
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(error, _)| error).expect("work");
                // Repeat identical sealed requests: exact retry evidence must
                // be identical for every API family, including native ingress.
                for _ in 0..2 {
                    assert!(matches!(attested.append(append.clone()).await, Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.fork(fork.clone()).await, Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.commit(commit.clone()).await, Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.commit(super::CommitRequest { deadline_unix_millis: Some(u64::MAX), ..commit.clone() }).await, Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.native_append(append.clone(), work.runtime_meter()).await.expect("native transport"), Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.native_commit(commit.clone(), None, work.runtime_meter()).await.expect("native transport"), Err(super::StreamError::AccessDenied)));
                    let guarded = attested.native_commit_guarded::<super::StreamError, _, _>(
                        commit.clone(), None, work.runtime_meter(), || std::future::ready(Ok(())))
                        .await.expect("native transport");
                    assert!(matches!(guarded, Err(super::StreamError::AccessDenied)));
                    assert!(matches!(attested.native_prepare_commit(commit.clone(), 256, work.runtime_meter())
                        .await.expect("native transport"), Err(super::StreamError::Unsupported)));
                }
                let reached = probe.reached.lock().expect("probe ledger");
                assert_eq!(reached.len(), 14, "each supported write family reaches its exact evidence binding");
                assert_eq!(reached[..7], reached[7..], "exact retries carry identical evidence");
                assert_eq!(attestor.0.load(std::sync::atomic::Ordering::SeqCst), 14);
                for observed in attestor.1.lock().expect("admission ledger").iter() {
                    match (&admission, observed) {
                        (super::TurnAdmission::LocalRoot, super::TurnAdmission::LocalRoot) => {}
                        (super::TurnAdmission::Admitted { audience: expected, scope },
                         super::TurnAdmission::Admitted { audience: actual, scope: actual_scope }) => {
                            assert_eq!(expected, actual);
                            assert_eq!(scope, actual_scope);
                        }
                        _ => panic!("write attestation changed the original admission"),
                    }
                }
            }
        });
    }
}
