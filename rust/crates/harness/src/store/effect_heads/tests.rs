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
    publication(client, reducer, &event, &mut indexed, &mut None).await?;
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
    let proof_path =
        path(&authority(), HEADS, Some((&"cuts", 4)), &mut None).map(|(path, _)| path)?;
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
    let selected = Tree {
        client: &client,
        authority: &authority(),
    }
    .root_at(
        &issuer().verifier(&mut None).expect("verifier copy"),
        1,
        1_000_000,
        &mut None,
    )
    .await?;
    assert_eq!(selected.0, None);
    let exact = selected.1;
    publish_lifecycle(&client, &mut reducer, 2).await?;
    assert_eq!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .root_at(
            &issuer().verifier(&mut None).expect("verifier copy"),
            1,
            exact,
            &mut None
        )
        .await?
        .1,
        exact
    );
    assert!(matches!(
        Tree {
            client: &client,
            authority: &authority()
        }
        .root_at(
            &issuer().verifier(&mut None).expect("verifier copy"),
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
        path(&authority(), HEADS, Some((&"cuts", 4)), &mut None)
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
    for mutation in 0..2 {
        let mut changed: (Option<Digest>, Digest) =
            crate::contract::json_from_slice(&certificates[0].value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        match mutation {
            0 => changed.0 = Some([93; 32]),
            _ => changed.1 = [94; 32],
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
async fn seeded_archive_transitions_preserve_old_cuts_across_read_faults() -> Result<()> {
    use std::{collections::BTreeMap, sync::atomic::Ordering};
    for seed in 1..=8_u64 {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut random = seed;
        let mut root = None;
        let mut heads = BTreeMap::<EffectId, (u64, u64)>::new();
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
            let head = (revision, previous.map_or(0, |head| head.1 + 1));
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
    .stage_node(Node::Leaf(effect, (1, 0)), &mut None)
    .await?;
    let branch = Node::Branch(0, leaf, leaf);
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
