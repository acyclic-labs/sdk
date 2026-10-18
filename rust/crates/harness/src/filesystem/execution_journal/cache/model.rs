//! Model the real append -> refresh -> provider lock nesting. The probe adds
//! no lock: it detects overlapping production critical sections, including a
//! shortened guard lifetime. The provider uses the same loom sync shim.
use super::*;
use loom::sync::atomic::{AtomicUsize, Ordering};

/// Construct lazy async operations off the coroutine's small stack. Their
/// polling, and therefore every lock acquisition, remains inside Loom.
#[inline(never)]
pub(crate) fn model_future<'a, F: std::future::Future + Send + 'a>(
    make: impl FnOnce() -> F + Send,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + Send + 'a>> {
    std::thread::scope(|scope| scope.spawn(|| Box::pin(make())).join().unwrap())
}

#[derive(Default)]
pub(crate) struct AppendProbe {
    pub release_early: bool,
    active: AtomicUsize,
}
pub(crate) struct AppendScope<'a>(&'a AppendProbe);
impl AppendProbe {
    pub fn enter(&self) -> AppendScope<'_> {
        assert_eq!(
            self.active.fetch_add(1, Ordering::SeqCst),
            0,
            "production journal append/refresh guard was released early"
        );
        loom::thread::yield_now();
        AppendScope(self)
    }
}
impl Drop for AppendScope<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Runs production append validation, suffix replay, retry checking and provider
/// commits. The second append must refresh the first commit under the same
/// journal lock; nested provider locks are instrumented by acyclic_loom.
pub fn verify_cached_summary_publication(release_early: bool) {
    use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};
    const _: fn(
        crate::sync::Mutex<VerifiedSummary>,
    ) -> acyclic_stream::query_sync::Mutex<VerifiedSummary> = |lock| lock;
    let journal = loom::future::block_on(async {
        let provider =
            ProviderRef::new("model".into(), "filesystem".into(), "2".into(), &mut None).unwrap();
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None).unwrap();
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "model".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
        .unwrap();
        let issuer = crate::core::AuthorityIssuer::new(
            "model",
            [2; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: crate::ConversationId::from_bytes([3; 16]).to_string(),
            },
        );
        let scope = issuer
            .issue(
                "owner",
                &crate::Capabilities::new([
                    volume.capability(VolumeOperation::Read, &mut None).unwrap(),
                    volume
                        .capability(VolumeOperation::Write, &mut None)
                        .unwrap(),
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .unwrap();
        let mut journal = FilesystemExecutionJournal::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            host,
            &SchemaRegistry::new(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None).unwrap(),
                scope: &scope,
                maximum_bytes: 4096,
                io: None,
            },
        )
        .unwrap();
        journal.append_probe.release_early = release_early;
        Arc::new(journal)
    });
    let operation = OperationId::from_bytes([1; 16]);
    let mut workers = Vec::new();
    for _ in 0..2 {
        let journal = journal.clone();
        workers.push(loom::thread::spawn(move || {
            let event = ExecutionEvent::Started {
                request_digest: [1; 32],
            };
            // Construct the boxed future on an ordinary OS stack: the large
            // temporary exceeds Loom's coroutine stack. Construction never
            // polls it or accesses a modeled lock. All polling, including
            // append/refresh and nested provider locking, stays on Loom.
            let future =
                model_future(|| journal.append_validated(operation, None, "retry", &event, None));
            loom::future::block_on(future).unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    {
        let cache = loom::future::block_on(journal.verified.lock());
        assert_eq!(cache.summary.tail, 1);
        assert!(cache.summary.fence.is_some());
    }
    let future = model_future(|| journal.replay_page_fenced(operation, 0, 8, true, None, None));
    assert_eq!(loom::future::block_on(future).unwrap().0.len(), 1);
}
