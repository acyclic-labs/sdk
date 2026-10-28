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
