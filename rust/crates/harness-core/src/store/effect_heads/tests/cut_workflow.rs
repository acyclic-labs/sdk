use super::*;
use crate::core::{ApplyResult, EffectGuarantee, Reducer, effect_root_tests};
use acyclic_stream::*;
use acyclic_test_support::differential;
use proptest::prelude::*;

#[path = "old_effect_heads.rs"]
mod old_effect_heads;

// Each differential run owns a fresh physical store and reducer. The observer
// sees the actual provider calls, including their order and complete payloads.
#[derive(Default)]
struct Observed(MemoryStream);

fn record(value: impl std::fmt::Debug) {
    differential::record(effect_root_tests::Observation::Storage(format!(
        "{value:?}"
    )));
}

fn normalized_path(path: &StreamPath) -> StreamPath {
    StreamPath::new(
        path.as_str()
            .replace("harness/v3/effect-heads/", "harness/v2/effect-heads/"),
    )
    .unwrap()
}

// Only the intentionally changed certificate bytes and namespace are normalized.
// Canonical events, tree nodes, tails, conditions and operation order stay exact.
fn normalized_request(mut request: CommitRequest) -> CommitRequest {
    if let [CommitMutation::Append { path, .. }] = request.mutations.as_slice()
        && path.as_str().contains("/effect-heads/")
        && path.as_str().contains("/nodes/")
    {
        // Immutable-node retry keys bind the versioned path. Check that binding
        // before translating it to the common namespace, rather than erasing it.
        let key = |path: &StreamPath| {
            crate::distributed::digest_key(
                blake3::hash(path.as_str().as_bytes()).as_bytes(),
                &mut None,
            )
            .unwrap()
            .0
        };
        assert_eq!(request.idempotency_key, key(path));
        request.idempotency_key = key(&normalized_path(path));
    }
    for condition in &mut request.conditions {
        match condition {
            CommitCondition::Absent { path }
            | CommitCondition::Tail { path, .. }
            | CommitCondition::Head { path, .. } => *path = normalized_path(path),
        }
    }
    for mutation in &mut request.mutations {
        let CommitMutation::Append { path, records } = mutation else {
            panic!("unexpected fork")
        };
        if path.as_str().contains("/effect-heads/") && path.as_str().ends_with("/cuts") {
            for bytes in records {
                let root = decode_certificate(path.as_str().contains("/v2/"), bytes).0;
                *bytes = crate::contract::canonical_json_bytes(&(root, "format-specific MAC"))
                    .unwrap()
                    .into();
            }
        }
        *path = normalized_path(path);
    }
    request
}

#[async_trait::async_trait]
impl StreamProvider for Observed {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        record(("inspect", &key));
        self.0.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> std::result::Result<u64, StreamError> {
        record(("tail", normalized_path(&path)));
        self.0.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        record(("bounds", normalized_path(&path)));
        self.0.bounds(path).await
    }
    async fn append(
        &self,
        request: AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        record(("append", &request));
        self.0.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        record(("fork", &request));
        self.0.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        record((
            "read",
            normalized_path(&request.path),
            request.from,
            request.limit,
        ));
        self.0.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        record(("follow", normalized_path(&path), from));
        self.0.follow(path, from).await
    }
    async fn children(
        &self,
        request: ChildrenRequest,
    ) -> std::result::Result<ChildStream, StreamError> {
        record(("children", &request));
        self.0.children(request).await
    }
    async fn commit(
        &self,
        request: CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        record(("commit", normalized_request(request.clone())));
        self.0.commit(request).await
    }
    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        // Commit identities intentionally bind different wire bytes. Atomic
        // membership is checked by both verifiers, including the split-commit probe.
        record("read_commit");
        self.0.read_commit(id).await
    }
}

fn decode_certificate(old: bool, bytes: &[u8]) -> (Option<Digest>, Digest) {
    if old {
        serde_json::from_slice(bytes).unwrap()
    } else {
        // Independent of the candidate codec, including the absent-root padding.
        assert_eq!(bytes.len(), 65);
        let root: Digest = bytes[1..33].try_into().unwrap();
        let root = match bytes[0] {
            0 => {
                assert_eq!(root, [0; 32]);
                None
            }
            1 => Some(root),
            _ => panic!("invalid certificate tag"),
        };
        (root, bytes[33..].try_into().unwrap())
    }
}

fn cut_path(old: bool) -> StreamPath {
    path(
        &authority(),
        if old {
            "harness/v2/effect-heads/"
        } else {
            HEADS
        },
        Some(&"cuts"),
        &mut None,
    )
    .unwrap()
    .0
}

async fn root_at(
    client: &Arc<Observed>,
    old: bool,
    verifier: &AuthorityVerifier,
    revision: u64,
    maximum: u64,
) -> Result<(Option<Digest>, u64)> {
    if old {
        old_effect_heads::Tree {
            client,
            authority: &authority(),
        }
        .root_at(verifier, revision, maximum, &mut None)
        .await
    } else {
        Tree {
            client,
            authority: &authority(),
        }
        .root_at(verifier, revision, maximum, &mut None)
        .await
    }
}

fn file(bytes: &[u8], media: &str) -> crate::conversation::FileRef {
    use crate::conversation::*;
    let provider = crate::resources::ProviderRef::new(
        "test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )
    .unwrap();
    let volume = VolumeRef::new(
        provider,
        "effects".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
        &mut None,
    )
    .unwrap();
    FileRef::new(
        volume,
        "request.json".into(),
        "1".into(),
        FileDescriptor::from_bytes(bytes, media, &mut None).unwrap(),
        "request.json".into(),
        &mut None,
    )
    .unwrap()
}

async fn workflow(
    client: Arc<Observed>,
    old: bool,
    seed: u8,
    reorder: bool,
    break_verification: bool,
) -> Vec<std::result::Result<Option<Digest>, String>> {
    let owner = authority();
    let verifier = issuer().verifier(&mut None).unwrap();
    let mut reducer = Reducer::new(owner.clone(), verifier.clone(), SchemaRegistry::new());
    let mut outcomes = Vec::new();
    for number in 1..=3 {
        let action = if number == 1 {
            Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            }
        } else {
            Action::PlanEffect {
                effect_id: EffectId::from_bytes([seed.wrapping_add(number); 16]),
                provider: "test.effects".into(),
                guarantee: EffectGuarantee::IdempotentRetry,
                effect_kind: "send".into(),
                request: file(
                    format!("{{\"seed\":{seed}}}").as_bytes(),
                    "application/json",
                ),
                result_schema: file(b"{}", "application/schema+json"),
            }
        };
        let command = Command {
            operation_id: OperationId::from_bytes([number; 16]),
            idempotency_key: crate::IdempotencyKey::new(format!("cut-{number}")).unwrap(),
            expected_revision: reducer.revision(),
            scope: issuer()
                .issue(
                    "owner",
                    &Capabilities::new([
                        "lifecycle:manage",
                        "effect:plan",
                        "effect:run",
                        "effect:provider:test.effects",
                    ]),
                    None,
                    None,
                    &mut None,
                )
                .unwrap(),
            causal_parent: None,
            action,
        };
        let ApplyResult::Applied { event } = reducer.plan(&command).unwrap() else {
            panic!("fresh command")
        };
        let publication = if old {
            old_effect_heads::publication(&client, &reducer, &event, &mut None)
                .await
                .unwrap()
        } else {
            publication(&client, &reducer, &event, &mut None)
                .await
                .unwrap()
        };
        let certificate = publication.indexes.last().unwrap();
        assert_eq!(certificate.0, cut_path(old));
        let (root, proof) = decode_certificate(old, &certificate.2);
        assert_eq!(root.is_some(), number != 1);
        let digest = *blake3::hash(&publication.bytes).as_bytes();
        let expected = if old {
            effect_root_tests::old_proof(&verifier, event.revision, digest, root, &mut None)
                .unwrap()
        } else {
            effect_root_tests::reference(&verifier, event.revision, digest, root)
        };
        assert_eq!(proof, expected);
        let mut unmetered = None;
        let (mut request, quota) = publication
            .request(
                &client,
                acyclic_stream::IdempotencyKey::new(format!("publish-{number}")).unwrap(),
                &mut unmetered,
            )
            .await
            .unwrap();
        drop(quota);
        if reorder && !old {
            request.mutations.reverse();
        }
        let CommitOutcome::Committed(envelope) = client.commit(request).await.unwrap() else {
            panic!("publication")
        };
        publication.verify(&envelope, &mut None).unwrap();
        reducer.apply_committed(event, &mut None).unwrap();
        let result = root_at(&client, old, &verifier, number.into(), 1_000_000).await;
        assert_eq!(result.as_ref().unwrap().0, root);
        outcomes.push(
            result
                .map(|value| value.0)
                .map_err(|error| error.to_string()),
        );
    }
    // Verify the earlier absence cut after effectful cuts have been committed.
    outcomes.push(
        root_at(&client, old, &verifier, 1, 1_000_000)
            .await
            .map(|value| value.0)
            .map_err(|error| error.to_string()),
    );
    let canonical = Stream::new(client.clone(), owner.stream_path(&mut None).unwrap())
        .unwrap()
        .read(0, 1)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .remove(0)
        .value;
    let certificate = Stream::new(client.clone(), cut_path(old).as_str())
        .unwrap()
        .read(0, 1)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .remove(0)
        .value;
    let (root, proof) = decode_certificate(old, &certificate);
    for fault in [
        "valid", "proof", "split", "anchor", "cut", "quota", "audience",
    ] {
        let copy = Arc::new(Observed::default());
        let proof = if fault == "proof" {
            [proof[0] ^ 1; 32]
        } else {
            proof
        };
        let bytes: OwnedBytes = if old {
            crate::contract::canonical_json_bytes(&(root, proof)).unwrap()
        } else {
            let mut bytes = vec![u8::from(root.is_some())];
            bytes.extend(root.unwrap_or([0; 32]));
            bytes.extend(proof);
            bytes
        }
        .into();
        let source = StreamPath::new(owner.stream_path(&mut None).unwrap()).unwrap();
        let mut mutations = Vec::new();
        if fault != "anchor" {
            mutations.push(CommitMutation::Append {
                path: source,
                records: vec![canonical.clone()],
            });
        }
        if fault != "cut" {
            mutations.push(CommitMutation::Append {
                path: cut_path(old),
                records: vec![bytes],
            });
        }
        let groups = if fault == "split" {
            mutations
                .into_iter()
                .map(|one| vec![one])
                .collect::<Vec<_>>()
        } else {
            vec![mutations]
        };
        for (index, mutations) in groups.into_iter().enumerate() {
            let conditions = mutations
                .iter()
                .map(|mutation| match mutation {
                    CommitMutation::Append { path, .. } => {
                        CommitCondition::Absent { path: path.clone() }
                    }
                    _ => unreachable!(),
                })
                .collect();
            copy.commit(CommitRequest {
                conditions,
                mutations,
                idempotency_key: acyclic_stream::IdempotencyKey::new(format!("copy-{index}"))
                    .unwrap(),
            })
            .await
            .unwrap();
        }
        let v = if fault == "audience" {
            AuthorityIssuer::new(
                "foreign",
                [91; 32],
                Authority {
                    kind: crate::core::AggregateKind::Task,
                    id: "foreign".into(),
                },
            )
            .verifier(&mut None)
            .unwrap()
        } else {
            verifier.clone()
        };
        let result = root_at(
            &copy,
            old,
            &v,
            1,
            if fault == "quota" { 0 } else { 1_000_000 },
        )
        .await
        .map(|value| value.0)
        .map_err(|error| error.to_string());
        outcomes.push(if break_verification && !old && fault == "proof" {
            Ok(None)
        } else {
            result
        });
    }
    outcomes
}

fn compare(seed: u8, reorder: bool, break_verification: bool) {
    let [(old, before), (new, after)] = differential::pair::<_, _, effect_root_tests::Observation>(
        || Arc::new(Observed::default()),
        |client, old| {
            futures::executor::block_on(workflow(client, old, seed, reorder, break_verification))
        },
    );
    assert_eq!(old, new);
    // Hash framing and digest-dependent work intentionally differ. Compare the
    // complete normalized storage trace, not a hash or operation-count summary.
    let storage = |events: Vec<effect_root_tests::Observation>| {
        events
            .into_iter()
            .filter_map(|event| match event {
                effect_root_tests::Observation::Storage(value) => Some(value),
                effect_root_tests::Observation::Hash(_) => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(storage(before), storage(after));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn old_and_new_cut_workflows_preserve_results_errors_and_storage_order(seed in any::<u8>()) {
        compare(seed, false, false);
    }
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn workflow_oracle_detects_storage_reordering() {
    compare(7, true, false);
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn workflow_oracle_detects_lost_authentication_error() {
    compare(7, false, true);
}
