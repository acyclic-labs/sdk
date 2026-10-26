use super::*;
use crate::{
    task_volume::{FileKey, FileValue, Observation, TaskVolume},
};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamPath};
use futures::FutureExt as _;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

type Attempt = (Query, Option<Cursor>, usize, usize);

#[derive(Clone)]
struct Source {
    volume: TaskVolume<MemoryStream>,
    reset: bool,
    close: Arc<AtomicBool>,
    invalid: Arc<AtomicBool>,
    pause: Arc<AtomicBool>,
    attempts: Arc<Mutex<Vec<Attempt>>>,
}
impl FileQuerySource for Source {
    fn retain(&self) -> Arc<dyn FileQuerySource> {
        Arc::new(self.clone())
    }
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        files: usize,
        bytes: usize,
    ) -> BoxProviderFuture<'_, crate::task_volume::Result<Observation>> {
        Box::pin(async move {
            self.attempts
                .lock()
                .map_err(|_| crate::task_volume::Error::InvalidPath)?
                .push((query.clone(), cursor.clone(), files, bytes));
            if cursor.is_some() && self.pause.swap(false, Ordering::SeqCst) {
                return futures::future::pending().await;
            }
            let first = cursor.is_none();
            let mut observed = self
                .volume
                .observe_files(query, if self.reset { None } else { cursor }, files, bytes)
                .await?;
            observed.reset = self.reset && !first;
            if !first && self.invalid.swap(false, Ordering::SeqCst) {
                observed
                    .snapshot
                    .as_mut()
                    .ok_or(crate::task_volume::Error::ResetRequired)?
                    .cursor
                    .accepted = 0;
            }
            if first || self.close.load(Ordering::SeqCst) {
                observed.changes = Box::pin(futures::stream::empty());
            }
            Ok(observed)
        })
    }
}
async fn fixture(reset: bool, pause: bool) -> crate::task_volume::Result<Source> {
    Ok(Source {
        volume: TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("ui-reader-resume")
                .map_err(|_| crate::task_volume::Error::InvalidPath)?,
        )
        .await?,
        reset,
        close: Arc::default(),
        invalid: Arc::default(),
        pause: Arc::new(AtomicBool::new(pause)),
        attempts: Arc::default(),
    })
}
async fn write(source: &Source, key: &str, bytes: &[u8]) -> crate::task_volume::Result<()> {
    source
        .volume
        .append(
            source.volume.head().await?,
            &FileValue::new(FileKey::parse(key)?, bytes.to_vec()),
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn documents_resume_exact_cuts_after_cancel_and_reset_atomically() {
    for reset in [false, true] {
        let source = fixture(reset, true).await.unwrap();
        let mut views = crate::test_support::empty_client_views();
        let mut documents = super::super::DocumentFiles::observe(
            &source,
            &mut views,
            &[(Query::File(FileKey::Goal), 1)],
            4096,
        )
        .await
        .unwrap();
        let cut = views
            .selections()
            .find(|(_, files)| *files.query() == Query::File(FileKey::Goal))
            .unwrap()
            .1
            .cursor()
            .clone();
        write(&source, "GOAL.md", b"resumed goal").await.unwrap();
        assert!(documents.read_change().now_or_never().is_none());
        assert_eq!(
            views
                .selections()
                .find(|(_, files)| *files.query() == Query::File(FileKey::Goal))
                .unwrap()
                .1
                .cursor(),
            &cut
        );
        let start = std::time::Instant::now();
        assert!(documents.next_change(&mut views).await.unwrap());
        assert!(
            views
                .selections()
                .any(|(_, files)| files.files().values().any(|file| file
                    .segments
                    .iter()
                    .any(|bytes| bytes.as_ref() == b"resumed goal")))
        );
        for (query, cursor, files, bytes) in source.attempts.lock().unwrap().iter().skip(1) {
            assert_eq!(
                (query, cursor, *files, *bytes),
                (&Query::File(FileKey::Goal), &Some(cut.clone()), 1, 4096)
            );
        }
        eprintln!(
            "CLIENT_DOCUMENT_RESUME os={} reset={reset} elapsed_us={}",
            std::env::consts::OS,
            start.elapsed().as_micros()
        );
    }
}
#[tokio::test]
async fn negative_control_exiting_the_document_reader_loses_a_publication() {
    let source = fixture(false, false).await.unwrap();
    let (mut follow, _) = FileFollow::open(&source, Query::File(FileKey::Goal), None, 1, 4096)
        .await
        .unwrap();
    write(&source, "GOAL.md", b"new").await.unwrap();
    assert!(follow.changes.next().await.is_none());
    assert!(
        matches!(follow.read().await.unwrap(), FileUpdate::Change(change) if change.file.is_some())
    );
}

#[tokio::test]
async fn closed_without_progress_returns_an_error_without_a_reconnect_loop() {
    let source = fixture(false, false).await.unwrap();
    source.close.store(true, Ordering::SeqCst);
    let (mut follow, _) = FileFollow::open(&source, Query::File(FileKey::Goal), None, 1, 4096)
        .await
        .unwrap();
    let cut = follow.cursor.clone();
    assert!(follow.read().await.is_err());
    assert!(follow.read().await.is_err());
    assert_eq!(follow.cursor, cut);
    assert_eq!(source.attempts.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn invalid_document_reset_preserves_the_fold_and_selected_delivery_survives_a_retry() {
    let source = fixture(true, false).await.unwrap();
    write(&source, "GOAL.md", b"old").await.unwrap();
    let mut views = crate::test_support::empty_client_views();
    let mut documents = super::super::DocumentFiles::observe(
        &source,
        &mut views,
        &[(Query::File(FileKey::Goal), 1)],
        4096,
    )
    .await
    .unwrap();
    let before = views.clone();
    write(&source, "GOAL.md", b"new").await.unwrap();
    source.invalid.store(true, Ordering::SeqCst);
    assert!(documents.read_change().await.is_err());
    assert_eq!(views, before);
    drop(documents.read_change().await.unwrap());
    assert_eq!(views, before);
    assert!(documents.next_change(&mut views).await.unwrap());
    assert_ne!(views, before);
    assert_eq!(
        source.attempts.lock().unwrap().len(),
        3,
        "retry uses the retained delivery, not another observe"
    );
}

#[tokio::test]
async fn document_deliveries_cannot_commit_another_readers_cursor() {
    let source = fixture(false, false).await.unwrap();
    let mut first_views = crate::test_support::empty_client_views();
    let mut second_views = crate::test_support::empty_client_views();
    let queries = [(Query::File(FileKey::Goal), 1)];
    let mut first = super::super::DocumentFiles::observe(&source, &mut first_views, &queries, 4096)
        .await
        .unwrap();
    let mut second =
        super::super::DocumentFiles::observe(&source, &mut second_views, &queries, 4096)
            .await
            .unwrap();
    let before = second_views.clone();
    write(&source, "GOAL.md", b"owned delivery").await.unwrap();
    let foreign = first.read_change().await.unwrap();
    assert!(second.apply_change(foreign, &mut second_views).is_err());
    assert_eq!(second_views, before);
    assert!(first.next_change(&mut first_views).await.unwrap());
    assert!(second.next_change(&mut second_views).await.unwrap());
    assert_eq!(first_views, second_views);
}
