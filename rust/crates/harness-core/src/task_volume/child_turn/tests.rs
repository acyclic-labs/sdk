use super::*;
use acyclic_stream::MemoryStream;

fn publication(expected: Generation) -> ChildTurnPublication<'static> {
    ChildTurnPublication {
        expected,
        conditions: Vec::new(),
        deadline: None,
        work: None,
    }
}
fn reference() -> Component {
    Component::new("worker").unwrap()
}
fn task() -> TaskId {
    TaskId::from_bytes([1; 16])
}
fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}
fn key(file: ChildFile) -> FileKey {
    FileKey::Child {
        reference: reference(),
        file,
    }
}
fn claimed(value: Option<ChildTurn>) -> ChildTurnClaim {
    let Some(ChildTurn::Claimed { claim, .. }) = value else {
        panic!("expected owned input")
    };
    claim
}

async fn histories<P: StreamProvider + 'static>(provider: Arc<P>) -> Result<()> {
    let path = StreamPath::new("task-volume/child-turn-history")?;
    let volume = TaskVolume::open(provider.clone(), path.clone()).await?;
    let observer = TaskVolume::open_existing(provider.clone(), path.clone()).await?;
    let cut = volume.head().await?;
    volume
        .append(
            cut,
            &FileValue::new(key(ChildFile::Request), b"first input".to_vec()),
        )
        .await?;
    let cut = volume.head().await?;
    let first = claimed(
        volume
            .claim_child_turn(&reference(), task(), operation(2), publication(cut))
            .await?,
    );
    assert_eq!(volume.head().await?.position, cut.position + 1);
    let observed = observer
        .observe_child(&reference(), ChildResultWindow::Latest, 1)
        .await?;
    assert_eq!(observed.generation, volume.head().await?);
    assert_eq!(observed.status_publication, Some(first.operation()));
    assert_eq!(observed.claim.as_ref(), Some(&first));
    assert_eq!(observed.delivered, 1);
    assert_eq!(observed.pending_inputs, 0);
    assert!(!observed.status.unwrap().settled);
    assert!(observed.results.is_empty());
    assert_eq!(observed.control, None);
    let head = volume.head().await?;
    volume
        .append(
            head,
            &FileValue::new(key(ChildFile::Control), Control::Resume.bytes().to_vec()),
        )
        .await?;
    let resumed = observer
        .observe_child(&reference(), ChildResultWindow::Latest, 1)
        .await?;
    assert_eq!(resumed.state, ChildState::Running);
    assert_eq!(
        resumed.control,
        Some(Control::Resume),
        "exact control was reconstructed from reduced state"
    );
    assert_eq!(resumed.generation, volume.head().await?);

    // A second caller at the same original cut cannot also own this input.
    assert!(
        volume
            .claim_child_turn(&reference(), task(), operation(3), publication(cut))
            .await
            .is_err()
    );
    drop(volume);
    let volume = TaskVolume::open_existing(provider.clone(), path.clone()).await?;
    let Some(ChildTurn::InFlight { claim: recovered }) = volume
        .claim_child_turn(&reference(), task(), operation(2), publication(cut))
        .await?
    else {
        panic!("replay must not grant execution twice")
    };
    assert_eq!(recovered, first);
    assert_eq!(first.turn, 1);
    let cut = volume.head().await?;
    volume
        .complete_child_turn(&first, Outcome::Cancelled, publication(cut))
        .await?;
    assert_eq!(volume.head().await?.position, cut.position + 1);
    drop(volume);
    let volume = TaskVolume::open_existing(provider.clone(), path.clone()).await?;
    let Some(ChildTurn::Settled { claim, result }) = volume
        .claim_child_turn(&reference(), task(), operation(2), publication(cut))
        .await?
    else {
        panic!("settled replay")
    };
    assert_eq!(claim, first);
    assert!(matches!(result.outcome, Outcome::Cancelled));
    let head = volume.head().await?;
    assert_eq!(
        volume
            .complete_child_turn(&first, Outcome::Cancelled, publication(cut))
            .await?,
        head
    );
    assert!(
        volume
            .complete_child_turn(
                &first,
                Outcome::Failed {
                    message: "changed".into()
                },
                publication(head)
            )
            .await
            .is_err()
    );
    let cut = volume
        .append(
            head,
            &FileValue::new(
                key(ChildFile::Inbox(Component::new("next").unwrap())),
                b"second input".to_vec(),
            ),
        )
        .await?;
    let second = claimed(
        volume
            .claim_child_turn(&reference(), task(), operation(3), publication(cut))
            .await?,
    );
    assert_eq!(second.turn, 2);
    let observed = volume
        .observe_child(&reference(), ChildResultWindow::Latest, 1)
        .await?;
    assert_eq!(observed.status_publication, Some(second.operation()));
    assert_eq!(observed.claim.as_ref(), Some(&second));
    assert_eq!(observed.delivered, 2);
    assert_eq!(observed.pending_inputs, 0);
    assert!(!observed.status.unwrap().settled);
    assert_eq!(observed.results[0].publication, Some(first.operation()));
    // Replaying an old outcome cannot settle or acknowledge the next claimant.
    let head = volume.head().await?;
    volume
        .complete_child_turn(&first, Outcome::Cancelled, publication(head))
        .await?;
    let status = volume
        .child_lifecycle(Some(&reference()))
        .await?
        .files
        .into_iter()
        .find(|file| file.key == key(ChildFile::Status))
        .unwrap();
    assert!(!ChildStatus::parse(&status.bytes)?.settled);
    let mut stale = second.clone();
    stale.operation = first.operation;
    assert!(
        volume
            .complete_child_turn(&stale, Outcome::Cancelled, publication(head))
            .await
            .is_err()
    );
    let cut = volume
        .append(
            head,
            &FileValue::new(key(ChildFile::Control), Control::Archive.bytes().to_vec()),
        )
        .await?;
    volume
        .complete_child_turn(&second, Outcome::Cancelled, publication(cut))
        .await?;
    let status = volume
        .child_lifecycle(Some(&reference()))
        .await?
        .files
        .into_iter()
        .find(|file| file.key == key(ChildFile::Status))
        .unwrap();
    assert_eq!(
        ChildStatus::parse(&status.bytes)?.state,
        ChildState::Archived
    );
    let outcomes = volume
        .observe_child(&reference(), ChildResultWindow::After(0), 1)
        .await?
        .results;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].key, key(ChildFile::Result(1)));
    let outcomes = volume
        .observe_child(&reference(), ChildResultWindow::After(1), 1)
        .await?
        .results;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].key, key(ChildFile::Result(2)));
    assert!(
        volume
            .observe_child(&reference(), ChildResultWindow::After(2), 1)
            .await?
            .results
            .is_empty()
    );
    assert!(
        volume
            .observe_child(&reference(), ChildResultWindow::After(u64::MAX), 1)
            .await?
            .results
            .is_empty()
    );
    let observed = volume
        .observe_child(&reference(), ChildResultWindow::Latest, 1)
        .await?;
    assert_eq!(observed.state, ChildState::Archived);
    assert_eq!(observed.control, Some(Control::Archive));
    let head = volume.head().await?;
    assert!(
        volume
            .append(
                head,
                &FileValue::new(key(ChildFile::Control), Control::Resume.bytes().to_vec())
            )
            .await
            .is_err()
    );
    let still_archived = observer
        .observe_child(&reference(), ChildResultWindow::Latest, 1)
        .await?;
    assert_eq!(still_archived.state, ChildState::Archived);
    assert_eq!(still_archived.control, Some(Control::Archive));
    assert_eq!(still_archived.generation, head);

    assert_eq!(observed.status_publication, Some(second.operation()));
    assert_eq!(observed.claim.as_ref(), Some(&second));
    assert!(observed.status.unwrap().settled);
    assert_eq!(observed.results.len(), 1);
    assert_eq!(ChildTurnResult::parse(&observed.results[0].bytes)?.turn, 2);
    assert_eq!(observed.results[0].publication, Some(second.operation()));
    Ok(())
}

#[tokio::test]
async fn memory_claim_completion_and_reopen_share_one_atomic_history() -> Result<()> {
    histories(Arc::new(MemoryStream::default())).await
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn durable_claim_completion_and_reopen_share_one_atomic_history() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    histories(Arc::new(
        acyclic_stream::LocalStream::open(
            directory.path(),
            acyclic_stream::LocalStreamLimits::default(),
        )
        .await
        .unwrap(),
    ))
    .await
}

#[tokio::test]
async fn concurrent_claimants_have_exactly_one_winner() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("task-volume/concurrent-claims")?;
    let first = TaskVolume::open(provider.clone(), path.clone()).await?;
    let cut = first.head().await?;
    let cut = first
        .append(
            cut,
            &FileValue::new(key(ChildFile::Request), b"one".to_vec()),
        )
        .await?;
    let second = TaskVolume::open_existing(provider, path).await?;
    let reference = reference();
    let (a, b) = futures::join!(
        first.claim_child_turn(&reference, task(), operation(10), publication(cut)),
        second.claim_child_turn(&reference, task(), operation(11), publication(cut))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    Ok(())
}

#[tokio::test]
async fn failed_owner_fence_cannot_publish_either_half_of_a_claim() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("task-volume/claim-owner-fence")?;
    let volume = TaskVolume::open(provider, path.clone()).await?;
    let cut = volume.head().await?;
    let cut = volume
        .append(
            cut,
            &FileValue::new(key(ChildFile::Request), b"one".to_vec()),
        )
        .await?;
    let mut publish = publication(cut);
    publish
        .conditions
        .push(CommitCondition::Tail { path, expected: 0 });
    assert!(
        volume
            .claim_child_turn(&reference(), task(), operation(12), publish)
            .await
            .is_err()
    );
    assert_eq!(volume.head().await?, cut);
    assert!(
        volume
            .child_lifecycle(Some(&reference()))
            .await?
            .files
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "claim and delivery must be one publication")]
async fn negative_control_split_claim_exposes_an_unacknowledged_execution() {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("task-volume/split-claim-control").unwrap();
    let volume = TaskVolume::open(provider.clone(), path.clone())
        .await
        .unwrap();
    let cut = volume.head().await.unwrap();
    let cut = volume
        .append(
            cut,
            &FileValue::new(key(ChildFile::Request), b"one".to_vec()),
        )
        .await
        .unwrap();
    // Restore the former native-host cut: Status is durable before Delivered.
    let started = FileValue::new(
        key(ChildFile::Status),
        serde_json::to_vec(&ChildStatus {
            task: task(),
            state: ChildState::Running,
            settled: false,
            inputs: 1,
        })
        .unwrap(),
    );
    volume.append(cut, &started).await.unwrap();
    drop(volume);
    let reopened = TaskVolume::open_existing(provider, path).await.unwrap();
    let files = reopened
        .child_lifecycle(Some(&reference()))
        .await
        .unwrap()
        .files;
    let status = files.iter().any(|file| file.key == key(ChildFile::Status));
    let delivered = files
        .iter()
        .any(|file| file.key == key(ChildFile::Delivered));
    assert_eq!(
        status, delivered,
        "claim and delivery must be one publication"
    );
}

#[tokio::test]
async fn one_record_replay_and_follow_cannot_split_a_turn_transition() -> Result<()> {
    use futures::{FutureExt as _, StreamExt as _};
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("task-volume/atomic-turn-observations")?;
    let writer = TaskVolume::open(provider.clone(), path.clone()).await?;
    let cut = writer.head().await?;
    let cut = writer
        .append(
            cut,
            &FileValue::new(key(ChildFile::Request), b"one".to_vec()),
        )
        .await?;
    let reader = TaskVolume::open_existing(provider.clone(), path).await?;
    let query = Query::Child(reference());
    let before = reader.cut(&query).await?;
    let claim = claimed(
        writer
            .claim_child_turn(&reference(), task(), operation(20), publication(cut))
            .await?,
    );
    let (_, complete) = reader.refresh_bounded::<true>(1).await?;
    assert!(complete, "one replay record contains the entire claim");
    {
        let index = reader.index.lock().await;
        let status = index.files.get(&key(ChildFile::Status)).unwrap();
        let delivered = index.files.get(&key(ChildFile::Delivered)).unwrap();
        assert_eq!(status.position, delivered.position);
        assert_eq!(status.file.publication, Some(claim.operation));
    }
    let mut changes = reader.follow(query.clone(), before).await?;
    let change = changes
        .next()
        .now_or_never()
        .expect("accepted claim is immediately readable")
        .unwrap()?;
    assert_eq!(change.file.iter().chain(&change.files).count(), 2);
    let snapshot = reader.snapshot_at(&query, change.cursor.clone(), 3).await?;
    assert!(
        snapshot
            .files
            .iter()
            .any(|f| f.key == key(ChildFile::Status))
    );
    assert!(
        snapshot
            .files
            .iter()
            .any(|f| f.key == key(ChildFile::Delivered))
    );
    let cut = reader.head().await?;
    writer
        .complete_child_turn(&claim, Outcome::Cancelled, publication(cut))
        .await?;
    assert!(reader.refresh_bounded::<true>(1).await?.1);
    let change = changes
        .next()
        .now_or_never()
        .expect("accepted completion is immediately readable")
        .unwrap()?;
    assert_eq!(change.file.iter().chain(&change.files).count(), 2);
    let snapshot = reader.snapshot_at(&query, change.cursor, 4).await?;
    let status = snapshot
        .files
        .iter()
        .find(|f| f.key == key(ChildFile::Status))
        .unwrap();
    assert!(ChildStatus::parse(&status.bytes)?.settled);
    assert!(
        snapshot
            .files
            .iter()
            .any(|f| f.key == key(ChildFile::Result(1)))
    );
    Ok(())
}
