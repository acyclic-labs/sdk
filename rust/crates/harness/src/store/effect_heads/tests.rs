use super::*;
use crate::{
    Capabilities, IdempotencyKey, OperationId,
    core::{
        Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, LifecycleState, Reducer,
        SchemaRegistry,
    },
};
use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream};
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
    client: &StreamClient<MemoryStream>,
    reducer: &mut Reducer,
    number: u8,
) -> Result<()> {
    let command = Command {
        operation_id: OperationId::from_bytes([number; 16]),
        idempotency_key: IdempotencyKey::new(format!("root-{number}"))?,
        expected_revision: reducer.revision(),
        scope: issuer().root("owner", Capabilities::new(["lifecycle:manage"])),
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
        crate::wire_codec::encode_event(reducer.authority(), &event)?,
    )?;
    publication(
        client,
        reducer.authority(),
        &issuer().verifier(),
        &event,
        None,
        &mut indexed,
    )
    .await?;
    let request = indexed
        .request(
            client,
            acyclic_stream::IdempotencyKey::new(format!("root-{number}"))?,
        )
        .await?;
    let CommitOutcome::Committed(envelope) = client.commit(request).await? else {
        return Err(Error::Invalid("root fixture publication conflicted".into()));
    };
    indexed.verify(&envelope)?;
    reducer.apply_committed(event)?;
    Ok(())
}

async fn copy_cut(
    canonical: &Bytes,
    certificate: Bytes,
    atomic: bool,
) -> Result<StreamClient<MemoryStream>> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let canonical_path = StreamPath::new(authority().stream_path()?)?;
    let proof_path = certificate_path(&authority())?;
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
        client
            .stream(canonical_path.as_str())?
            .append(canonical.clone())
            .await?;
        client
            .stream(proof_path.as_str())?
            .append(certificate)
            .await?;
    }
    Ok(client)
}

#[tokio::test]
async fn original_cut_authenticates_absence_and_rejects_rebinding_and_non_atomic_copy() -> Result<()>
{
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let mut reducer = Reducer::new(authority(), issuer().verifier(), SchemaRegistry::new());
    publish_lifecycle(&client, &mut reducer, 1).await?;
    let selected = select(
        &client,
        &authority(),
        &issuer().verifier(),
        EffectId::from_bytes([92; 16]),
        1,
        1_000_000,
    )
    .await?;
    assert_eq!(selected.head, None);
    let exact = selected.consumed_bytes;
    publish_lifecycle(&client, &mut reducer, 2).await?;
    assert_eq!(
        select(
            &client,
            &authority(),
            &issuer().verifier(),
            EffectId::from_bytes([92; 16]),
            1,
            exact
        )
        .await?
        .consumed_bytes,
        exact
    );
    assert!(matches!(
        select(
            &client,
            &authority(),
            &issuer().verifier(),
            EffectId::from_bytes([92; 16]),
            1,
            exact - 1
        )
        .await,
        Err(Error::Invalid(_))
    ));
    let canonical = client
        .stream(authority().stream_path()?)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let certificates = client
        .stream(certificate_path(&authority())?.as_str())?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let valid = copy_cut(&canonical[0].value, certificates[0].value.clone(), true).await?;
    assert_eq!(
        root_at(&valid, &authority(), &issuer().verifier(), 1, exact)
            .await?
            .hash,
        None
    );
    let non_atomic = copy_cut(&canonical[0].value, certificates[0].value.clone(), false).await?;
    assert!(matches!(
        root_at(&non_atomic, &authority(), &issuer().verifier(), 1, exact).await,
        Err(Error::Storage(_))
    ));
    for mutation in 0..4 {
        let mut changed: Certificate = crate::executor::decode_json(&certificates[0].value)?;
        match mutation {
            0 => changed.root = Some([93; 32]),
            1 => changed.revision = 2,
            2 => changed.canonical_digest = [94; 32],
            _ => changed.canonical_path = "harness/foreign/tasks/effect-root-proof".into(),
        }
        let forged = copy_cut(
            &canonical[0].value,
            Bytes::from(crate::contract::canonical_json_bytes(&changed)?),
            true,
        )
        .await?;
        assert!(matches!(
            root_at(&forged, &authority(), &issuer().verifier(), 1, 1_000_000).await,
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
    .verifier();
    assert!(matches!(
        root_at(&client, &authority(), &foreign, 1, exact).await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn compressed_index_preserves_historical_heads_and_authenticated_absence() -> Result<()> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let mut root = None;
    let mut cuts = Vec::new();
    for number in 1..=64_u64 {
        let effect = EffectId::from_bytes(uuid::Uuid::from_u128(u128::from(number)).into_bytes());
        let mut used = 0;
        let observed = walk(&client, &authority(), effect, root, &mut used, 1_000_000).await?;
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
    let observed = walk(&client, &authority(), effect, root, &mut used, 1_000_000).await?;
    assert!(matches!(
        update(
            &client,
            &authority(),
            effect,
            Head {
                revision: 65,
                position: 2
            },
            observed
        )
        .await,
        Err(Error::Storage(_))
    ));
    Ok(())
}

#[tokio::test]
async fn missing_authenticated_node_rejects_instead_of_selecting_older_state() -> Result<()> {
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = StreamClient::new(provider.clone());
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
        )
        .await?,
    );
    provider
        .history_read_fault
        .store(3, std::sync::atomic::Ordering::SeqCst);
    let mut used = 0;
    assert!(matches!(
        walk(&client, &authority(), effect, root, &mut used, 1_000_000).await,
        Err(Error::Storage(_))
    ));
    let mut used = 0;
    assert_eq!(
        walk(&client, &authority(), effect, root, &mut used, 1_000_000)
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
