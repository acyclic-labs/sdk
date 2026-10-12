use super::*;
use crate::{
    Capabilities, IdempotencyKey, OperationId,
    core::{
        Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, LifecycleState, Reducer,
        SchemaRegistry,
    },
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, OwnedBytes,
};
use futures::TryStreamExt as _;
use std::sync::Arc;

fn authority() -> Authority {
    Authority {
        kind: AggregateKind::Task,
        id: "effect-root-proof".into(),
    }
}

fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new("effect-root-proof", [91; 32], authority())
}

async fn publish_lifecycle(
    client: &std::sync::Arc<MemoryStream>,
    reducer: &mut Reducer,
    number: u8,
) -> Result<()> {
    let command = Command {
        operation_id: OperationId::from_bytes([number; 16]),
        idempotency_key: IdempotencyKey::new(format!("root-{number}"))?,
        expected_revision: reducer.revision(),
        scope: issuer()
            .issue(
                "owner",
                &Capabilities::new(["lifecycle:manage"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::TransitionLifecycle {
            to: if number.is_multiple_of(2) {
                LifecycleState::Waiting
            } else {
                LifecycleState::Active
            },
            reason: None,
        },
    };
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        return Err(Error::Invalid(
            "root fixture did not produce a new event".into(),
        ));
    };
    let mut indexed = operations::IndexedPublication::new(
        reducer.authority(),
        &event,
        crate::wire_codec::encode_event(reducer.authority(), &event, &mut None)?,
        &mut None,
    )?;
    publication_with_work(
        client,
        reducer.authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        &event,
        None,
        &mut indexed,
        &mut None,
    )
    .await?;
    let mut request_work = None;
    let (request, _request_hold) = indexed
        .request(
            client,
            acyclic_stream::IdempotencyKey::new(format!("root-{number}"))?,
            &mut request_work,
        )
        .await?;
    let CommitOutcome::Committed(envelope) = client.commit(request).await? else {
        return Err(Error::Invalid("root fixture publication conflicted".into()));
    };
    indexed.verify(&envelope, &mut None)?;
    reducer.apply_committed(event, &mut None)?;
    Ok(())
}

async fn copy_cut(
    canonical: &OwnedBytes,
    certificate: OwnedBytes,
    atomic: bool,
) -> Result<std::sync::Arc<MemoryStream>> {
    let client = Arc::new(MemoryStream::default());
    let canonical_path = StreamPath::new(authority().stream_path(&mut None)?)?;
    let proof_path = certificate_path_with_work(&authority(), &mut None).map(|(path, _)| path)?;
    if atomic {
        client
            .commit(CommitRequest {
                conditions: vec![
                    CommitCondition::Absent {
                        path: canonical_path.clone(),
                    },
                    CommitCondition::Absent {
                        path: proof_path.clone(),
                    },
                ],
                mutations: vec![
                    CommitMutation::Append {
                        path: canonical_path,
                        records: vec![canonical.clone()],
                    },
                    CommitMutation::Append {
                        path: proof_path,
                        records: vec![certificate],
                    },
                ],
                idempotency_key: acyclic_stream::IdempotencyKey::new("copied-effect-cut")?,
            })
            .await?;
    } else {
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), canonical_path.as_str())?
            .append(canonical.clone())
            .await?;
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), proof_path.as_str())?
            .append(certificate)
            .await?;
    }
    Ok(client)
}

#[tokio::test]
async fn original_cut_authenticates_absence_and_rejects_rebinding_and_non_atomic_copy() -> Result<()>
{
    let client = Arc::new(MemoryStream::default());
    let mut reducer = Reducer::new(
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    publish_lifecycle(&client, &mut reducer, 1).await?;
    let selected = select_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        EffectId::from_bytes([92; 16]),
        1,
        1_000_000,
        &mut None,
    )
    .await?;
    assert_eq!(selected.0, None);
    let exact = selected.1;
    publish_lifecycle(&client, &mut reducer, 2).await?;
    assert_eq!(
        select_with_work(
            &client,
            &authority(),
            &issuer().verifier(&mut None).expect("verifier copy"),
            EffectId::from_bytes([92; 16]),
            1,
            exact,
            &mut None
        )
        .await?
        .1,
        exact
    );
    assert!(matches!(
        select_with_work(
            &client,
            &authority(),
            &issuer().verifier(&mut None).expect("verifier copy"),
            EffectId::from_bytes([92; 16]),
            1,
            exact - 1,
            &mut None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    let canonical = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        authority().stream_path(&mut None)?,
    )?
    .read(0, 1)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let certificates = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        certificate_path_with_work(&authority(), &mut None)
            .map(|(path, _)| path)?
            .as_str(),
    )?
    .read(0, 1)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let valid = copy_cut(&canonical[0].value, certificates[0].value.clone(), true).await?;
    assert_eq!(
        Tree {
            client: &valid,
            authority: &authority()
        }
        .root_at(
            &issuer().verifier(&mut None).expect("verifier copy"),
            1,
            exact,
            &mut None
        )
        .await?
        .0,
        None
    );
    let non_atomic = copy_cut(&canonical[0].value, certificates[0].value.clone(), false).await?;
    assert!(matches!(
        Tree {
            client: &non_atomic,
            authority: &authority()
        }
        .root_at(
            &issuer().verifier(&mut None).expect("verifier copy"),
            1,
            exact,
            &mut None
        )
        .await,
        Err(Error::Storage(_))
    ));
    for mutation in 0..3 {
        let mut changed: Certificate = crate::contract::json_from_slice(&certificates[0].value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        match mutation {
            0 => changed.root = Some([93; 32]),
            1 => changed.revision = 2,
            _ => changed.canonical_digest = [94; 32],
        }
        let forged = copy_cut(
            &canonical[0].value,
            OwnedBytes::from(crate::contract::canonical_json_bytes(&changed)?),
            true,
        )
        .await?;
        assert!(matches!(
            Tree {
                client: &forged,
                authority: &authority()
            }
            .root_at(
                &issuer().verifier(&mut None).expect("verifier copy"),
                1,
                1_000_000,
                &mut None
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
    }
    let foreign = AuthorityIssuer::new(
        "effect-root-proof",
        [91; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "foreign-effect-root".into(),
        },
    )
    .verifier(&mut None)
    .expect("verifier copy");
    assert!(matches!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .root_at(&foreign, 1, exact, &mut None)
        .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn compressed_index_preserves_historical_heads_and_authenticated_absence() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut root = None;
    let mut cuts = Vec::new();
    for number in 1..=64_u64 {
        let effect = EffectId::from_bytes(uuid::Uuid::from_u128(u128::from(number)).into_bytes());
        let mut used = 0;
        let observed = Tree {
            client: &client,
            authority: &authority(),
        }
        .walk(effect, root, &mut used, 1_000_000, &mut None)
        .await?;
        assert!(observed.1.is_none_or(|(_, id, _)| id != effect));
        root = Some(
            Tree {
                client: &client,
                authority: &authority(),
            }
            .update(
                effect,
                Head {
                    revision: number,
                    position: 0,
                },
                observed,
                &mut None,
            )
            .await?,
        );
        cuts.push(root);
    }
    for (index, historical) in cuts.into_iter().enumerate() {
        let through = u64::try_from(index)
            .map_err(|_| Error::Invalid("effect root fixture cut overflows".into()))?
            + 1;
        for number in [1_u64, through, 65] {
            let effect =
                EffectId::from_bytes(uuid::Uuid::from_u128(u128::from(number)).into_bytes());
            let mut used = 0;
            let observed = Tree {
                client: &client,
                authority: &authority(),
            }
            .walk(effect, historical, &mut used, 1_000_000, &mut None)
            .await?;
            let head = observed
                .1
                .and_then(|(_, id, head)| (id == effect).then_some(head));
            assert_eq!(
                head,
                (number <= through).then_some(Head {
                    revision: number,
                    position: 0
                })
            );
        }
    }
    let effect = EffectId::from_bytes(uuid::Uuid::from_u128(1).into_bytes());
    let mut used = 0;
    let observed = Tree {
        client: &client,
        authority: &authority(),
    }
    .walk(effect, root, &mut used, 1_000_000, &mut None)
    .await?;
    assert!(matches!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .update(
            effect,
            Head {
                revision: 65,
                position: 2
            },
            observed,
            &mut None
        )
        .await,
        Err(Error::Storage(_))
    ));
    Ok(())
}

#[tokio::test]
async fn missing_authenticated_node_rejects_instead_of_selecting_older_state() -> Result<()> {
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let effect = EffectId::from_bytes([95; 16]);
    let root = Some(
        Tree {
            client: &client,
            authority: &authority(),
        }
        .stage_node(
            Node::Leaf {
                effect,
                head: Head {
                    revision: 3,
                    position: 2,
                },
            },
            &mut None,
        )
        .await?,
    );
    provider
        .history_read_fault
        .store(3, std::sync::atomic::Ordering::SeqCst);
    let mut used = 0;
    assert!(matches!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .walk(effect, root, &mut used, 1_000_000, &mut None)
        .await,
        Err(Error::Storage(_))
    ));
    let mut used = 0;
    assert_eq!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .walk(effect, root, &mut used, 1_000_000, &mut None)
        .await?
        .1
        .map(|(_, _, head)| head),
        Some(Head {
            revision: 3,
            position: 2
        })
    );
    Ok(())
}

#[tokio::test]
async fn seeded_archive_transitions_preserve_old_cuts_across_read_faults() -> Result<()> {
    use std::{collections::BTreeMap, sync::atomic::Ordering};
    for seed in 1..=8_u64 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut random = seed;
        let mut root = None;
        let mut heads = BTreeMap::<EffectId, Head>::new();
        let mut cuts = Vec::new();
        for revision in 1..=96_u64 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = random % 24;
            let effect = EffectId::from_bytes(
                *blake3::hash(&key.to_le_bytes())
                    .as_bytes()
                    .first_chunk::<16>()
                    .ok_or_else(|| Error::Invalid("fixture key".into()))?,
            );
            let previous = heads.get(&effect).copied();
            let head = Head {
                revision,
                position: previous.map_or(0, |head| head.position + 1),
            };
            let mut used = 0;
            let observed = Tree {
                client: &client,
                authority: &authority(),
            }
            .walk(effect, root, &mut used, 1_000_000, &mut None)
            .await?;
            root = Some(
                Tree {
                    client: &client,
                    authority: &authority(),
                }
                .update(effect, head, observed, &mut None)
                .await?,
            );
            heads.insert(effect, head);
            cuts.push((root, heads.clone()));
            let cut = usize::try_from(random % revision)
                .map_err(|_| Error::Invalid("fixture cut".into()))?;
            let (historical, expected) = &cuts[cut];
            provider.history_read_fault.store(3, Ordering::SeqCst);
            let mut failed_bytes = 0;
            assert!(
                Tree {
                    client: &client,
                    authority: &authority()
                }
                .walk(effect, *historical, &mut failed_bytes, 1_000_000, &mut None)
                .await
                .is_err()
            );
            let mut bytes = 0;
            let selected = Tree {
                client: &client,
                authority: &authority(),
            }
            .walk(effect, *historical, &mut bytes, 1_000_000, &mut None)
            .await?;
            assert_eq!(
                selected
                    .1
                    .and_then(|(_, id, head)| (id == effect).then_some(head)),
                expected.get(&effect).copied()
            );
            assert!(bytes > 0);
            let mut short = 0;
            assert!(
                Tree {
                    client: &client,
                    authority: &authority()
                }
                .walk(effect, *historical, &mut short, bytes - 1, &mut None)
                .await
                .is_err()
            );
            let absent = EffectId::from_bytes([255; 16]);
            let mut bytes = 0;
            let selected = Tree {
                client: &client,
                authority: &authority(),
            }
            .walk(absent, *historical, &mut bytes, 1_000_000, &mut None)
            .await?;
            assert!(selected.1.is_none_or(|(_, id, _)| id != absent));
        }
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn failed_walk_releases_path_reservation() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let effect = EffectId::from_bytes([1; 16]);
    let leaf = Tree {
        client: &client,
        authority: &authority(),
    }
    .stage_node(
        Node::Leaf {
            effect,
            head: Head {
                revision: 1,
                position: 0,
            },
        },
        &mut None,
    )
    .await?;
    let branch = Node::Branch {
        bit: 0,
        left: leaf,
        right: leaf,
    };
    let maximum =
        super::super::history::canonical_bytes_with_work(&branch, MAX_NODE_BYTES, &mut None)?.len()
            as u64;
    let root = Tree {
        client: &client,
        authority: &authority(),
    }
    .stage_node(branch, &mut None)
    .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(16_384, 64 * 1024 * 1024);
    let mut io = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget,
        &cancellation,
    )?);
    let before = io.remaining()?.peak_allocation_bytes;
    assert!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .walk(effect, Some(root), &mut 0, maximum, &mut Some(&mut io))
        .await
        .is_err()
    );
    assert_eq!(io.remaining()?.peak_allocation_bytes, before);
    Ok(())
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_decode_controls {
    use super::super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn budget() -> WorkBudget {
        WorkBudget {
            allocation_operations: 1024,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
                + 1024 * 1024,
            source_bytes_read: 1024 * 1024,
            items_examined: 1024 * 1024,
            bytes_copied: 1024 * 1024,
            ..WorkBudget::default()
        }
    }

    #[test]
    fn malformed_node_and_certificate_keep_original_serde_custody() {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
        );
        let node = decode_node_with_work(
            br#"{"kind":"leaf","effect":false,"head":{"revision":1,"position":0}}"#,
            &mut Some(&mut io),
        );
        let certificate = decode_certificate_with_work(br#"{"authority":false,"canonical_path":"harness/v2/task/x","revision":1,"canonical_digest":[],"root":null,"proof":[]}"#, &mut Some(&mut io));
        for error in [
            node.err().expect("invalid node"),
            certificate.err().expect("invalid certificate"),
        ] {
            match error {
                Error::NativeJson {
                    source,
                    custody: Some(custody),
                    ..
                } => {
                    assert!(custody.bytes() > 0);
                    assert!(work.shares_ledger(custody.work()));
                    assert!(source.is_data());
                }
                other => panic!("expected retained native serde cause: {other:?}"),
            }
        }
    }

    #[test]
    fn cancellation_preserves_original_callback_cause() {
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
        );
        cancellation.cancel();
        let error = decode_node_with_work(
            br#"{"kind":"branch","bit":0,"left":[],"right":[]}"#,
            &mut Some(&mut io),
        )
        .err()
        .expect("cancelled");
        assert!(matches!(error, Error::NativeWork { .. }));
    }

    #[test]
    fn original_quota_denies_node_before_serde_diagnostic_allocation() {
        let cancellation = CancellationToken::new();
        let mut limit = budget();
        limit.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
        let work = NativeMaterializationWork::new(limit, &cancellation).expect("original ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
        );
        let error = decode_node_with_work(
            br#"{"kind":"leaf","effect":false,"head":null}"#,
            &mut Some(&mut io),
        )
        .err()
        .expect("quota denial");
        assert!(matches!(error, Error::NativeWork { .. }));
    }
}
