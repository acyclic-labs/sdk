use super::*;
use crate::{
    addressing::ActorReference,
    domain::{ActorLimits, CodeSha256},
    hosting::Host,
    local::{Clock, PreparedPublish, RuntimeLimits, Store, SystemClock},
};
use acyclic_stream::{IdempotencyKey, MemoryStream, StreamPath};
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A reusable embedded book must not expire original grants on a rejected
/// command. Admission uses actual committed inputs; only due grants may move.
#[tokio::test]
async fn reservation_rejection_preserves_due_grants_and_original_fence() -> TestResult {
    use crate::guest::{SubscriptionCuts, Trigger};
    use acyclic_stream::StreamProviderExt as _;

    let provider = Arc::new(MemoryStream::default());
    let actor = ActorReference::new(b"account", b"pools", b"atomic-step")?;
    let (store, _) = open(&provider, &actor, "atomic-pool").await?;
    let snapshot = store.load(&actor).await?;
    let state = snapshot.state().ok_or("missing state")?;
    let anchor = provider
        .head(snapshot.path().clone())
        .await?
        .last
        .ok_or("missing state anchor")?
        .commit_id;
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"owner"),
        Bytes::from_static(b"original-evidence"),
    )?;
    let mut rejected_allocations = [None; 2];
    let mut release_allocations = None;
    for population in [1u64, 1_000] {
        let mut original = Book::new(population + 3)?;
        for generation in 1..=population + 2 {
            let deadline_unix_ms = if generation <= 2 { 10 } else { 30 };
            original.active.entry(deadline_unix_ms).or_default().insert(
                generation,
                Grant {
                    pool: actor.encode().into(),
                    state_path: snapshot.path().clone(),
                    request_id: Bytes::copy_from_slice(&generation.to_be_bytes()),
                    generation,
                    units: 1,
                    owner: owner.clone(),
                    deadline_unix_ms,
                },
            );
        }
        original.used = population + 2;
        original.generation = u64::MAX;
        original.validate()?;
        let unchanged = encode(&original)?;
        let copying = acyclic_test_support::measure(|| {
            std::hint::black_box(original.clone());
        });
        for units in [0, 1] {
            let request_id = if units == 0 {
                Bytes::from_static(b"zero")
            } else {
                Bytes::from_static(b"overflow")
            };
            let request = Request::Reserve {
                request_id: request_id.clone(),
                units,
                owner: owner.clone(),
                deadline_unix_ms: 40,
            };
            // Bypass the client-only argument check, retaining a real original
            // input. Zero units must also be refused at the handler boundary.
            let mut operation = b"reserve\0".to_vec();
            operation.extend_from_slice(&request_id);
            let invocation = Invocation::new(
                actor.clone(),
                "inbox".into(),
                operation.into(),
                encode(&request)?,
            )?;
            let ticket = store.enqueue_invoke(&invocation).await?;
            let delivery = Delivery {
                actor: actor.clone(),
                epoch: state.epoch(),
                trigger: Trigger::Record {
                    subscription: "inbox".into(),
                    sequence: ticket.record().sequence,
                },
                now_unix_ms: 20,
                record: Some(ticket.record().clone()),
                subscriptions: SubscriptionCuts::new(
                    snapshot.path().clone(),
                    anchor,
                    state.subscriptions().clone(),
                ),
            };
            let mut book = original.clone();
            let rejected = acyclic_test_support::measure(|| {
                assert_eq!(book.step(&delivery).err(), Some(invalid()));
            });
            assert_eq!(encode(&book)?, unchanged);
            let count = &mut rejected_allocations[usize::try_from(units)?];
            if let Some(expected) = count {
                assert_eq!(rejected.count_total, *expected);
            } else {
                *count = Some(rejected.count_total);
            }
            if population == 1_000 {
                assert!(rejected.count_total < copying.count_total);
            }

            // The actual alarm transition is the eager-cleanup negative
            // control: performing it before refusal changes the original book.
            let alarm = Delivery {
                record: None,
                trigger: Trigger::Alarm {
                    generation: 1,
                    now_unix_ms: 20,
                },
                ..delivery.clone()
            };
            let mut eager = original.clone();
            let step = eager.step(&alarm)?;
            assert_eq!(
                step.alarm,
                AlarmChange::Set {
                    deadline_unix_ms: 30
                }
            );
            assert_eq!(eager.used(), population);
            assert_ne!(encode(&eager)?, unchanged);
            assert_eq!(eager.step(&delivery).err(), Some(invalid()));
            assert_ne!(encode(&eager)?, unchanged);

            // Releasing an exact live grant still progresses at an exhausted
            // generation fence, reclaiming only that grant and due grants.
            let mut valid = delivery;
            let invocation = Request::Release {
                grant: book
                    .active
                    .get(&30)
                    .and_then(|grants| grants.get(&3))
                    .ok_or("missing live grant")?
                    .clone(),
            }
            .invocation(actor.clone(), "inbox".into())?;
            let ticket = store.enqueue_invoke(&invocation).await?;
            valid.trigger = Trigger::Record {
                subscription: "inbox".into(),
                sequence: ticket.record().sequence,
            };
            valid.record = Some(ticket.record().clone());
            let allocations = acyclic_test_support::measure(|| {
                let step = book.step(&valid).unwrap();
                assert_eq!(
                    decode::<Decision>(&step.response.unwrap()).unwrap(),
                    Decision::Released
                );
            });
            if let Some(expected) = release_allocations {
                assert_eq!(allocations.count_total, expected);
            } else {
                release_allocations = Some(allocations.count_total);
            }
            assert_eq!(book.used(), population - 1);
            assert_eq!(book.generation, u64::MAX);
            assert_eq!(book.deadline(), (population > 1).then_some(30));
            book.validate()?;
        }
    }
    Ok(())
}

async fn open(
    provider: &Arc<MemoryStream>,
    actor: &ActorReference,
    root: &str,
) -> Result<(Store<MemoryStream>, Host<MemoryStream>), Box<dyn std::error::Error>> {
    open_at(
        provider,
        actor,
        root,
        StreamPath::new(format!("{root}/inbox"))?,
    )
    .await
}

async fn open_at(
    provider: &Arc<MemoryStream>,
    actor: &ActorReference,
    root: &str,
    input: StreamPath,
) -> Result<(Store<MemoryStream>, Host<MemoryStream>), Box<dyn std::error::Error>> {
    let store = Store::new(Arc::clone(provider), StreamPath::new(root)?);
    let host = publish(&store, actor, super::create(&store, actor, input, 2)?).await?;
    Ok((store, host))
}

async fn publish(
    store: &Store<MemoryStream>,
    actor: &ActorReference,
    effects: impl IntoIterator<Item = crate::guest::Effect>,
) -> Result<Host<MemoryStream>, Box<dyn std::error::Error>> {
    let mut create = PreparedPublish::foreign(IdempotencyKey::new(
        format!("create-{}", store.root()).into_bytes(),
    )?);
    for effect in effects {
        create.join(effect)?;
    }
    store.submit(create).await?;
    Ok(Host::new(
        Store::new(Arc::clone(store.provider()), store.root().clone()),
        actor.clone(),
        Arc::new(Package::new(
            CodeSha256::try_from(vec![0x62; 32])?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        )),
        Arc::new(SystemClock),
        RuntimeLimits::new(1, 1)?,
    ))
}

/// Identical actor/subscription identities in different roots can share input.
/// Only the original root may apply or answer each committed operation.
#[tokio::test]
async fn shared_input_invocations_execute_only_under_the_original_store_root() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let actor = ActorReference::new(b"account", b"pools", b"same-shared")?;
    let input = StreamPath::new("shared/pool-input")?;
    let (left, left_host) = open_at(&provider, &actor, "shared-left", input.clone()).await?;
    let right = Store::new(Arc::clone(&provider), StreamPath::new("shared-right")?);
    let right_host = publish(
        &right,
        &actor,
        right.create(
            &actor,
            BTreeMap::from([(
                "inbox".into(),
                crate::guest::Subscription {
                    path: input,
                    cursor: 0,
                },
            )]),
            Some(initial_checkpoint(2)?),
        )?,
    )
    .await?;
    let left_pool = Pool::new(&left, &actor, "inbox");
    let right_pool = Pool::new(&right, &actor, "inbox");
    let owner = OwnerEvidence::new(Bytes::from_static(b"owner"), Bytes::from_static(b"proof"))?;
    let deadline = SystemClock
        .now_unix_ms()
        .checked_add(60_000)
        .ok_or("clock overflow")?;
    let request = Bytes::from_static(b"same-operation");
    let mut first = Box::pin(left_pool.reserve(2, request.clone(), owner.clone(), deadline));
    assert!(futures::poll!(first.as_mut()).is_pending());
    left_host.wake().await?;
    let first = first.await?;
    right_host.wake().await?;
    assert_eq!(used(&left, &actor).await?, 2);
    assert_eq!(
        used(&right, &actor).await?,
        0,
        "a valid request addressed to another root must not reserve capacity"
    );
    assert_eq!(
        right
            .load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(2)
    );
    let mut second = Box::pin(right_pool.reserve(2, request.clone(), owner.clone(), deadline));
    assert!(futures::poll!(second.as_mut()).is_pending());
    right_host.wake().await?;
    let second = second.await?;
    left_host.wake().await?;
    assert_eq!(used(&left, &actor).await?, 2);
    assert_eq!(used(&right, &actor).await?, 2);
    assert_eq!(
        left.load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(3)
    );
    assert_eq!(
        left_pool
            .reserve(2, request.clone(), owner.clone(), deadline)
            .await?,
        first
    );
    assert_eq!(
        right_pool.reserve(2, request, owner, deadline).await?,
        second
    );
    Ok(())
}

async fn used(
    store: &Store<MemoryStream>,
    actor: &ActorReference,
) -> Result<u64, Box<dyn std::error::Error>> {
    let snapshot = store.load(actor).await?;
    let checkpoint = snapshot
        .state()
        .and_then(|state| state.checkpoint())
        .ok_or("missing reservation checkpoint")?;
    let book: Book = decode(&checkpoint.bytes)?;
    book.validate()?;
    Ok(book.used())
}

/// Both roots host the SAME actor and receive identical first reserve requests.
/// Keep the actual returned grants: only their original state paths differ.
#[tokio::test]
async fn release_is_bound_to_the_original_store_root_in_client_and_handler() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let actor = ActorReference::new(b"account", b"pools", b"same")?;
    let (left, left_host) = open(&provider, &actor, "left-pool").await?;
    let (right, right_host) = open(&provider, &actor, "right-pool").await?;
    let left_pool = Pool::new(&left, &actor, "inbox");
    let right_pool = Pool::new(&right, &actor, "inbox");
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"owner"),
        Bytes::from_static(b"original-evidence"),
    )?;
    let deadline = SystemClock
        .now_unix_ms()
        .checked_add(60_000)
        .ok_or("clock overflow")?;
    let mut first = Box::pin(left_pool.reserve(
        2,
        Bytes::from_static(b"same-request"),
        owner.clone(),
        deadline,
    ));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(left_host.wake().await?, Some(deadline));
    let first = first.await?;
    let mut second =
        Box::pin(right_pool.reserve(2, Bytes::from_static(b"same-request"), owner, deadline));
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(right_host.wake().await?, Some(deadline));
    let second = second.await?;
    let Decision::Granted(left_grant) = first.decision() else {
        return Err("missing left grant".into());
    };
    let Decision::Granted(right_grant) = second.decision() else {
        return Err("missing right grant".into());
    };
    assert_eq!(left_grant.generation(), 1);
    assert_eq!(right_grant.generation(), 1);
    assert_eq!(left_grant.pool(), right_grant.pool());
    assert_ne!(left_grant.state_path(), right_grant.state_path());
    assert!(matches!(
        right_pool.release(left_grant).await,
        Err(crate::local::Error::Conflict)
    ));
    // Bypass only the client preflight. The actual admitted Handler must still
    // refuse the original foreign-root grant and preserve both checkpoints.
    let foreign_release = Request::Release {
        grant: left_grant.clone(),
    }
    .invocation(actor.clone(), "inbox".into())?;
    let mut release = Box::pin(right.invoke(&foreign_release));
    assert!(futures::poll!(release.as_mut()).is_pending());
    assert_eq!(right_host.wake().await?, Some(deadline));
    let release = release.await?;
    assert_eq!(decode::<Decision>(release.response())?, Decision::Stale);
    assert_eq!(used(&left, &actor).await?, 2);
    assert_eq!(used(&right, &actor).await?, 2);
    let mut release = Box::pin(right_pool.release(right_grant));
    assert!(futures::poll!(release.as_mut()).is_pending());
    assert_eq!(right_host.wake().await?, None);
    assert_eq!(release.await?.decision(), &Decision::Released);
    assert_eq!(used(&right, &actor).await?, 0);
    assert_eq!(used(&left, &actor).await?, 2);
    Ok(())
}
