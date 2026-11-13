use super::*;
use crate::filesystem::execution_journal::{inline_tests, test_support};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Hold the first original publication, then use the real filesystem journal.
/// The reader must drain without either completing or opening a second writer.
struct HeldJournal {
    inner: inline_tests::Journal,
    release: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
    entered: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    groups: AtomicUsize,
    active: AtomicUsize,
}

struct VisiblePublisher {
    inner: Arc<dyn crate::conversation::ContentPublisher>,
    stages: AtomicUsize,
}

impl crate::conversation::ContentPublisher for VisiblePublisher {
    fn volume(&self) -> &crate::conversation::VolumeRef {
        self.inner.volume()
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        self.stages.fetch_add(1, Ordering::SeqCst);
        self.inner
            .stage_once(operation, path, bytes, media, display, access)
    }
}

impl ExecutionJournal for HeldJournal {
    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<ExecutionAttempt<'a>> {
        self.inner.acquire_execution(operation, io)
    }

    fn cache_identity(&self) -> Option<ExecutionJournalCacheId> {
        self.inner.cache_identity()
    }
    fn inline_model_event_bytes(&self) -> u64 {
        self.inner.inline_model_event_bytes()
    }
    fn append_prepared_models<'a>(
        &'a self,
        operation: OperationId,
        entries: &'a [PreparedModelObservation<'a>],
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<u64>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
            self.groups.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = self.entered.lock().unwrap().take() {
                entered.send(()).expect("publication observer");
            }
            let release = self.release.lock().unwrap().take();
            if let Some(release) = release {
                release
                    .await
                    .map_err(|_| Error::Storage("publication source closed".into()))?;
            }
            let result = self
                .inner
                .append_prepared_models(operation, entries, io)?
                .await;
            assert_eq!(self.active.fetch_sub(1, Ordering::SeqCst), 1);
            result
        })
    }
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.inner.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.append(operation, key, event, io)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media, io)
    }
    fn load<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.inner.load(file, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.inner.interaction_outcome(id, io)
    }
}

fn work(seen: &crate::obs::capture::Seen, field: &str) -> u64 {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|(_, name, _)| *name == field)
        .map(|(_, _, value)| value.parse::<u64>().expect("original work counter"))
        .sum()
}

#[tokio::test]
async fn provider_drains_during_publication_and_groups_without_payload_reloads() -> Result<()> {
    let mut samples = Vec::new();
    for count in [16, 128] {
        let (journal, _issuer) = test_support::journal().await?;
        let operation = OperationId::new();
        inline_tests::initialize(&journal, operation).await?;
        let prepared = inline_tests::request()?;
        let model = &prepared.request().model;
        let publisher = Arc::new(VisiblePublisher {
            inner: test_support::live_publisher(&journal)?,
            stages: AtomicUsize::new(0),
        });
        let (release, held) = futures::channel::oneshot::channel();
        let (entered, entry) = futures::channel::oneshot::channel();
        let journal = HeldJournal {
            inner: journal,
            release: Mutex::new(Some(held)),
            entered: Mutex::new(Some(entered)),
            groups: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        };
        let executor = StockExecutor::new(
            model.clone(),
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_live_reply(LiveReply::new(publisher.clone(), "live".into()));
        let execution = StockExecution::ordinary(&executor);
        let mut events: Vec<_> = (0..count)
            .map(|_| ModelEvent::Reasoning {
                delta: "thought".into(),
            })
            .collect();
        // A restart publishes an empty live-answer marker as well as text.
        // It must obey the same durable-publication boundary as every delta.
        events.insert(0, ModelEvent::Restarted);
        // Also exercise staged content: a fresh file must not be reloaded.
        events.push(ModelEvent::Content {
            delta: "x".repeat(acyclic_stream::MAX_RECORD_BYTES),
        });
        events.push(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        });
        let consumed = AtomicUsize::new(0);
        let source = futures::stream::iter(events.iter().cloned().map(|event| {
            consumed.fetch_add(1, Ordering::SeqCst);
            Ok(event)
        }));
        let (seen, capture) = crate::obs::capture::install();
        let mut admission = ModelEventAdmission::default();
        #[cfg(feature = "research")]
        let mut phase = DispatchPhase::new();
        let mut result = Box::pin(execution.record_model_events(
            &journal,
            ObservedCall {
                operation_id: operation,
                dispatch: crate::model::ModelDispatch {
                    operation_id: operation,
                    step: 0,
                    request_digest: prepared.request_digest(),
                },
                model,
                sampling: prepared.request().sampling,
                max_output_tokens: prepared.request().max_output_tokens,
                purpose: ModelPurpose::Response,
                clock: super::super::model_usage::CallClock::unknown(),
            },
            Vec::new(),
            &mut admission,
            (Either::Right(source), true),
            #[cfg(feature = "research")]
            &mut phase,
        ));
        tokio::select! {
            result = &mut result => panic!("completion passed a held publication: {result:?}"),
            entered = entry => entered.expect("original publication entry"),
        }
        assert!(result.as_mut().now_or_never().is_none());
        assert_eq!(consumed.load(Ordering::SeqCst), count + 3);
        assert_eq!(publisher.stages.load(Ordering::SeqCst), 0);
        assert_eq!(journal.groups.load(Ordering::SeqCst), 1);
        assert_eq!(journal.active.load(Ordering::SeqCst), 1);
        release.send(()).expect("original pending publication");
        assert_eq!(result.await?, events);
        assert_eq!(journal.groups.load(Ordering::SeqCst), 2);
        assert_eq!(journal.active.load(Ordering::SeqCst), 0);
        assert!(
            publisher.stages.load(Ordering::SeqCst) > 0,
            "live publication counter is inactive"
        );
        assert_eq!(work(&seen, "journal_publications"), 2);
        assert_eq!(work(&seen, "journal_appends"), (count + 3) as u64);
        assert_eq!(
            work(&seen, "payload_stages"),
            (count + 4) as u64,
            "private events plus terminal usage"
        );
        assert_eq!(work(&seen, "payload_loads"), 0);
        samples.push((count as u64, work(&seen, "journal_publications")));
        let records = journal.replay(operation, 2, 256, None)?.await?;
        assert_eq!(records.len(), events.len());
        let mut replayed = Vec::new();
        for record in records {
            replayed.push(
                super::super::model_observation::load(&journal, &record.event, None)
                    .await?
                    .expect("original model event"),
            );
        }
        assert_eq!(replayed, events);
        assert!(
            work(&seen, "payload_loads") >= events.len() as u64,
            "replay must exercise the original load counter that was zero on publication"
        );
        drop(capture);
    }
    acyclic_test_support::scale::assert_scales(
        acyclic_test_support::scale::Bound::Constant,
        samples[0],
        samples[1],
    );
    Ok(())
}
