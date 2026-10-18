//! All schedules of one reservation/publication race. Once either attempt
//! conflicts, the other publication has committed, so the remaining retry is
//! sequential. This keeps the model small without a scheduling/coverage bound.
use super::super::*;

async fn commit(
    provider: &Arc<acyclic_stream::MemoryStream>,
    prepared: JournalFile,
    id: u8,
) -> bool {
    let mut work = None;
    let key = acyclic_stream::IdempotencyKey::new(vec![id]).unwrap();
    let mut request = crate::distributed::commit_request(key, 0, &mut work);
    crate::distributed::add_condition(&mut request, prepared.condition).unwrap();
    crate::distributed::add_record(&mut request, prepared.path, prepared.record).unwrap();
    matches!(
        crate::distributed::commit_journal(
            request,
            0,
            provider,
            crate::OperationId::from_bytes([id; 16])
        )
        .await
        .unwrap()
        .0,
        CommitOutcome::Committed(_)
    )
}
async fn prepare(
    volume: &TaskVolume<acyclic_stream::MemoryStream>,
    named: FileKey,
    id: u8,
    unchecked: bool,
) -> Option<JournalFile> {
    let result = if unchecked {
        volume
            .prepare_journal_file(named, Channel::Text, &[id], None)
            .await
    } else {
        volume
            .prepare_named_chunk(named, Channel::Text, &[id], None)
            .await
    };
    match result {
        Err(crate::Error::TaskFiles(error)) if matches!(*error, Error::SequenceConflict(_)) => None,
        result => result.unwrap(),
    }
}
/// Model real index locks, the sequence recheck and finalized provider commits.
pub fn verify_chunk_sequence_publication(unchecked: bool) {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let volume = Arc::new(
        loom::future::block_on(TaskVolume::open(
            provider.clone(),
            StreamPath::new("sequence-model").unwrap(),
        ))
        .unwrap(),
    );
    let task = Component::new("task").unwrap();
    let named = |id| {
        loom::future::block_on(volume.next_chunk_key(
            task.clone(),
            crate::OperationId::from_bytes([id; 16]),
            0,
            None,
        ))
        .unwrap()
    };
    let peer = loom::future::block_on(prepare(&volume, named(255), 255, false)).unwrap();
    let reserved = named(1);
    let writer = provider.clone();
    let peer = loom::thread::spawn(move || loom::future::block_on(commit(&writer, peer, 255)));
    let writer = provider.clone();
    let selected = volume.clone();
    let consumer = loom::thread::spawn(move || {
        loom::future::block_on(async {
            match prepare(&selected, reserved, 1, unchecked).await {
                None => false,
                Some(prepared) => commit(&writer, prepared, 1).await,
            }
        })
    });
    let outcomes = [(255, peer.join().unwrap()), (1, consumer.join().unwrap())];
    for (id, committed) in outcomes {
        if !committed {
            let next = named(id);
            let prepared = loom::future::block_on(prepare(&volume, next, id, false)).unwrap();
            assert!(loom::future::block_on(commit(&provider, prepared, id)));
        }
    }
    let snapshot = loom::future::block_on(volume.snapshot(&Query::Root, 4)).unwrap();
    let sequences: Vec<_> = snapshot
        .files
        .iter()
        .map(|file| match file.key {
            FileKey::Task {
                file: TaskFile::Chunk { sequence, .. },
                ..
            } => sequence,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(
        sequences,
        [0, 1],
        "task chunk publication repeated its reserved sequence"
    );
}
