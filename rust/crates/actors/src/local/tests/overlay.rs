//! Fork overlays: creation from a template, branches born paused with their
//! own escrow, explicit allocation, and forks at a pinned cut.
use super::*;
use crate::escrow::{Error as EscrowError, Escrow};

async fn step<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
    epoch: u64,
    sequence: u64,
    transition: Transition,
) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let cut = store.load(address).await?;
    store
        .publish(
            &cut,
            epoch,
            &record(sequence),
            None,
            transition,
            key(&format!("{}/step/{sequence}", address.id()))?,
        )
        .await?;
    Ok(store.load(address).await?)
}

async fn created<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
    escrow: Option<Escrow>,
) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let mut publication = PreparedPublish::foreign(key(&format!("{}/create", address.id()))?);
    publication.join(store.create(
        None,
        address,
        Creation {
            subscriptions: inbox(address)?,
            escrow,
            ..Creation::default()
        },
    )?)?;
    store.submit(publication).await?;
    let cut = store.load(address).await?;
    Ok(store
        .claim(&cut, 1, key(&format!("{}/place", address.id()))?)
        .await?)
}

async fn activations<P: StreamProvider + ?Sized>(
    provider: &P,
    root: &StreamPath,
) -> Result<u64, StreamError> {
    match provider.tail(root.clone()).await {
        Err(StreamError::NotFound) => Ok(0),
        tail => tail,
    }
}

fn branched(cut: &Snapshot, escrow: Escrow) -> Result<Branch, Box<dyn std::error::Error>> {
    Ok(Branch {
        subscriptions: cut
            .state()
            .ok_or("missing source state")?
            .subscriptions()
            .clone(),
        escrow,
    })
}

async fn resume<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let cut = store.load(address).await?;
    let epoch = cut.state().ok_or("missing branch")?.epoch() + 1;
    Ok(store
        .claim_with_pause(
            &cut,
            epoch,
            Some(false),
            key(&format!("{}/resume", address.id()))?,
        )
        .await?)
}

/// Creation forks a template's exact cut into a dormant actor: no wake, no
/// inherited alarm, metadata or journal; the initial checkpoint is carried.
#[tokio::test]
async fn creation_overlay_forks_a_template_into_a_dormant_actor() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let template = actor("template")?;
    let placed = created(&store, &template, None).await?;
    let mut binding = store.bind_state(placed, 1)?;
    binding.transaction(|state| {
        state.put("secret".into(), Bytes::from_static(b"template-only"));
        state.append_journal(0, vec![Bytes::from_static(b"template-journal")])
    })?;
    binding.flush(key("template/flush")?).await?;
    let cut = step(
        &store,
        &template,
        1,
        0,
        transition(
            1,
            AlarmChange::Set {
                deadline_unix_ms: 42,
            },
        ),
    )
    .await?;
    let armed = cut.state().ok_or("missing template")?.alarm().clone();
    assert_eq!(armed.pending(), Some(42));

    let child = actor("created-child")?;
    let woken = activations(&*provider, store.root()).await?;
    let initial = Checkpoint {
        codec: CodecIdentity {
            name: "counter".into(),
            version: "1".into(),
        },
        bytes: Bytes::from_static(&[9]),
    };
    let mut publication = PreparedPublish::foreign(key("child/create")?);
    publication.join(store.create(
        Some(&cut),
        &child,
        Creation {
            subscriptions: inbox(&child)?,
            checkpoint: Some(initial.clone()),
            escrow: Some(Escrow::new(Some(4))),
        },
    )?)?;
    store.submit(publication).await?;
    // Reads are registered, not ready: creation alone wakes nothing.
    assert_eq!(activations(&*provider, store.root()).await?, woken);

    let child_cut = store.load(&child).await?;
    let state = child_cut.state().ok_or("missing child")?;
    assert_eq!(state.epoch(), 0);
    assert!(!state.paused());
    assert_eq!(state.checkpoint(), Some(&initial));
    assert_eq!(state.alarm().pending(), None);
    assert!(state.alarm().generation() > armed.generation());
    assert_eq!(state.metadata().len(), 0);
    assert_eq!(state.journal_tail(), 0);
    assert_eq!(state.cursor("inbox"), Some(0));
    assert_eq!(
        child_cut.provenance().map(|origin| &origin.source),
        Some(cut.path())
    );
    let journal = StreamPath::new(format!("{}/journal", child_cut.path()))?;
    assert!(matches!(
        provider.head(journal).await,
        Err(StreamError::NotFound)
    ));
    // The creator's funding is usable in the new actor's own branch.
    let placed = store.claim(&child_cut, 1, key("child/place")?).await?;
    let mut binding = store.bind_state(placed, 1)?;
    binding.transaction(|state| state.debit_escrow(4))?;
    assert!(matches!(
        binding.transaction(|state| state.debit_escrow(1)),
        Err(Error::Escrow(EscrowError::Exhausted))
    ));
    Ok(())
}

/// The forker allocates explicitly in its own step: its debit and the
/// branch's funding commit together, conserve the total and never repeat.
/// An actor-blind copy (no overlay) of a funded state cannot spend at all.
#[tokio::test]
async fn branch_escrow_is_zero_unless_allocated_and_never_double_spent() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let forker = actor("forker")?;
    let target = actor("target")?;
    let forker_cut = created(&store, &forker, Some(Escrow::new(Some(5)))).await?;
    created(&store, &target, None).await?;
    let target_cut = step(&store, &target, 1, 0, transition(1, AlarmChange::Keep)).await?;

    // Escrow zero: an unfunded branch cannot spend even after resuming.
    let unfunded = actor("unfunded-branch")?;
    let mut publication = PreparedPublish::foreign(key("unfunded/fork")?);
    publication.join(store.branch(
        &target_cut,
        &unfunded,
        branched(&target_cut, Escrow::new(Some(0)))?,
    )?)?;
    store.submit(publication).await?;
    let born = store.load(&unfunded).await?;
    assert!(born.state().ok_or("missing branch")?.paused());
    let resumed = resume(&store, &unfunded).await?;
    let epoch = resumed.state().ok_or("missing branch")?.epoch();
    let mut binding = store.bind_state(resumed, epoch)?;
    assert!(matches!(
        binding.transaction(|state| state.debit_escrow(1)),
        Err(Error::Escrow(EscrowError::Exhausted))
    ));

    // Explicit allocation: one publication debits the forker and funds the branch.
    let funded = actor("funded-branch")?;
    let mut binding = store.bind_state(forker_cut, 1)?;
    let allocated = binding.transaction(|state| state.allocate_escrow(Some(3)))?;
    let mut update = transition(1, AlarmChange::Keep);
    update
        .effects
        .push(store.branch(&target_cut, &funded, branched(&target_cut, allocated)?)?);
    store
        .submit(binding.prepare_publish(&record(0), None, update, key("forker/allocate")?)?)
        .await?;
    let forker_cut = store.load(&forker).await?;
    let left = forker_cut
        .state()
        .and_then(State::escrow)
        .ok_or("missing forker escrow")?;
    assert_eq!(left.remaining(), Some(2));
    // A second allocation beyond the remainder refuses and stages nothing.
    let mut binding = store.bind_state(forker_cut, 1)?;
    assert!(matches!(
        binding.transaction(|state| state.allocate_escrow(Some(3))),
        Err(Error::Escrow(EscrowError::Exhausted))
    ));
    binding.transaction(|state| {
        assert_eq!(state.escrow()?.remaining(), Some(2));
        Ok(())
    })?;
    let resumed = resume(&store, &funded).await?;
    let epoch = resumed.state().ok_or("missing branch")?.epoch();
    let mut binding = store.bind_state(resumed, epoch)?;
    binding.transaction(|state| state.debit_escrow(3))?;
    assert!(matches!(
        binding.transaction(|state| state.debit_escrow(1)),
        Err(Error::Escrow(EscrowError::Exhausted))
    ));

    // Negative control: an actor-blind subtree fork copies the funded state
    // without an overlay. Its copy is foreign to the new branch, so the
    // forker's remaining 2 cannot be spent twice.
    let forker_cut = store.load(&forker).await?;
    let copy = actor("blind-copy")?;
    let copy_path = store.path(&copy)?;
    let commit = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Absent {
                path: copy_path.clone(),
            }],
            mutations: vec![CommitMutation::Fork {
                exclude: Vec::new(),
                source: forker_cut.path().clone(),
                destination: copy_path,
                at_tail: forker_cut.tail,
                records: Vec::new(),
            }],
            idempotency_key: key("blind/fork")?,
        })
        .await?;
    PreparedPublish::settle(&commit)?;
    let copied = store.load(&copy).await?;
    assert_eq!(
        copied
            .state()
            .and_then(State::escrow)
            .map(Escrow::remaining),
        Some(Some(2))
    );
    let placed = store.claim(&copied, 2, key("blind/place")?).await?;
    let mut binding = store.bind_state(placed, 2)?;
    assert!(matches!(
        binding.transaction(|state| state.debit_escrow(1)),
        Err(Error::Escrow(EscrowError::ForeignBranch))
    ));
    Ok(())
}

/// A branch never resumed is a pinned cut: a later fork from it sees the
/// source as it was, while a fork from a cut the source has left refuses
/// and changes nothing.
#[tokio::test]
async fn fork_at_a_pinned_cut_is_exact_and_stale_cuts_refuse() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let source = actor("pinned-source")?;
    created(&store, &source, None).await?;
    let first = step(&store, &source, 1, 0, transition(1, AlarmChange::Keep)).await?;
    let pin = actor("pin")?;
    let mut publication = PreparedPublish::foreign(key("pin/fork")?);
    publication.join(store.branch(&first, &pin, branched(&first, Escrow::new(Some(0)))?)?)?;
    store.submit(publication).await?;
    let later = step(&store, &source, 1, 1, transition(2, AlarmChange::Keep)).await?;

    // Negative control: the cut the source has left no longer forks.
    let stale = actor("stale-branch")?;
    let mut publication = PreparedPublish::foreign(key("stale/fork")?);
    publication.join(store.branch(&first, &stale, branched(&first, Escrow::new(Some(0)))?)?)?;
    assert!(matches!(
        store.submit(publication).await,
        Err(Error::Conflict)
    ));
    assert!(store.load(&stale).await?.state().is_none());

    let pinned = store.load(&pin).await?;
    let replay = actor("replay-branch")?;
    let mut publication = PreparedPublish::foreign(key("replay/fork")?);
    publication.join(store.branch(
        &pinned,
        &replay,
        branched(&pinned, Escrow::new(Some(0)))?,
    )?)?;
    store.submit(publication).await?;
    let replayed = store.load(&replay).await?;
    let checkpoint = |cut: &Snapshot| {
        cut.state()
            .and_then(State::checkpoint)
            .map(|checkpoint| checkpoint.bytes.clone())
    };
    assert_eq!(checkpoint(&replayed), checkpoint(&first));
    assert_ne!(checkpoint(&replayed), checkpoint(&later));
    assert_eq!(
        replayed.provenance().map(|origin| &origin.source),
        Some(pinned.path())
    );
    Ok(())
}
