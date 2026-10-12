use super::*;
use crate::{
    Capabilities, IdempotencyKey, OperationId,
    core::{
        Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, LifecycleState, Reducer,
        SchemaRegistry,
    },
};
use acyclic_stream::{OwnedBytes, CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream};
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
        scope: issuer().issue("owner", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof"),
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
     &mut None)?;
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
            acyclic_stream::IdempotencyKey::new(format!("root-{number}"))?, &mut request_work,
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
        client.commit(CommitRequest {
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
    let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None).expect("verifier copy"), SchemaRegistry::new());
    publish_lifecycle(&client, &mut reducer, 1).await?;
    let selected = select_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        EffectId::from_bytes([92; 16]),
        1,
        1_000_000, &mut None)
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
            exact, &mut None)
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
            exact - 1, &mut None)
        .await,
        Err(Error::Invalid(_))
    ));
    let canonical = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let certificates = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), certificate_path_with_work(&authority(), &mut None).map(|(path, _)| path)?.as_str())?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let valid = copy_cut(&canonical[0].value, certificates[0].value.clone(), true).await?;
    assert_eq!(
        root_at(&valid, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), 1, exact, &mut None)
            .await?
            .0,
        None
    );
    let non_atomic = copy_cut(&canonical[0].value, certificates[0].value.clone(), false).await?;
    assert!(matches!(
        root_at(&non_atomic, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), 1, exact, &mut None).await,
        Err(Error::Storage(_))
    ));
    for mutation in 0..4 {
        let mut changed: Certificate = crate::executor::decode_json(&certificates[0].value, None)?.0;
        match mutation {
            0 => changed.root = Some([93; 32]),
            1 => changed.revision = 2,
            2 => changed.canonical_digest = [94; 32],
            _ => changed.canonical_path = "harness/foreign/tasks/effect-root-proof".into(),
        }
        let forged = copy_cut(
            &canonical[0].value,
            OwnedBytes::from(crate::contract::canonical_json_bytes(&changed)?),
            true,
        )
        .await?;
        assert!(matches!(
            root_at(&forged, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), 1, 1_000_000, &mut None).await,
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
    .verifier(&mut None).expect("verifier copy");
    assert!(matches!(
        root_at(&client, &authority(), &foreign, 1, exact, &mut None).await,
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
        let observed = walk(&client, &authority(), effect, root, &mut used, 1_000_000, &mut None).await?;
        assert!(observed.leaf.is_none_or(|(_, id, _)| id != effect));
        root = Some(
            update(
                &client,
                &authority(),
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
            let observed = walk(
                &client,
                &authority(),
                effect,
                historical,
                &mut used,
                1_000_000,
                &mut None,
            )
            .await?;
            let head = observed
                .leaf
                .and_then(|(_, id, head)| (id == effect).then_some(head));
            assert_eq!(
                head,
                (number <= through).then_some(Head {
                    revision: number,
                    position: 0
                })
            );
            assert!(observed.path.len() <= 128);
        }
    }
    let effect = EffectId::from_bytes(uuid::Uuid::from_u128(1).into_bytes());
    let mut used = 0;
    let observed = walk(&client, &authority(), effect, root, &mut used, 1_000_000, &mut None).await?;
    assert!(matches!(
        update(
            &client,
            &authority(),
            effect,
            Head {
                revision: 65,
                position: 2
            },
            observed,
            &mut None,
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
        stage_node(
            &client,
            &authority(),
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
        walk(&client, &authority(), effect, root, &mut used, 1_000_000, &mut None).await,
        Err(Error::Storage(_))
    ));
    let mut used = 0;
    assert_eq!(
        walk(&client, &authority(), effect, root, &mut used, 1_000_000, &mut None)
            .await?
            .leaf
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
            random = random.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let key = random % 24;
            let effect = EffectId::from_bytes(*blake3::hash(&key.to_le_bytes()).as_bytes()
                .first_chunk::<16>().ok_or_else(|| Error::Invalid("fixture key".into()))?);
            let previous = heads.get(&effect).copied();
            let head = Head { revision, position: previous.map_or(0, |head| head.position + 1) };
            let mut used = 0;
            let observed = walk(&client, &authority(), effect, root, &mut used, 1_000_000, &mut None).await?;
            root = Some(update(&client, &authority(), effect, head, observed, &mut None).await?);
            heads.insert(effect, head);
            cuts.push((root, heads.clone()));
            let cut = usize::try_from(random % revision)
                .map_err(|_| Error::Invalid("fixture cut".into()))?;
            let (historical, expected) = &cuts[cut];
            provider.history_read_fault.store(3, Ordering::SeqCst);
            let mut failed_bytes = 0;
            assert!(walk(&client, &authority(), effect, *historical,
                &mut failed_bytes, 1_000_000, &mut None).await.is_err());
            let mut bytes = 0;
            let selected = walk(&client, &authority(), effect, *historical,
                &mut bytes, 1_000_000, &mut None).await?;
            assert_eq!(selected.leaf.and_then(|(_, id, head)|
                (id == effect).then_some(head)), expected.get(&effect).copied());
            assert!(selected.path.len() <= 128);
            assert!(bytes > 0);
            let mut short = 0;
            assert!(walk(&client, &authority(), effect, *historical,
                &mut short, bytes - 1, &mut None).await.is_err());
            let absent = EffectId::from_bytes([255; 16]);
            let mut bytes = 0;
            let selected = walk(&client, &authority(), absent, *historical,
                &mut bytes, 1_000_000, &mut None).await?;
            assert!(selected.leaf.is_none_or(|(_, id, _)| id != absent));
        }
    }
    Ok(())
}
