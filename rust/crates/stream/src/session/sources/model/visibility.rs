//! Publication visibility through the real deduplicated wake and source poll.
use super::*;

pub(super) fn verify(release_only: bool) {
    loom::model(move || {
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        let value = Arc::new(AtomicU64::new(0));
        let notified = Arc::new(AtomicU64::new(0));
        let reading = value.clone();
        let notification = notified.clone();
        let mut opened = false;
        let source = futures::stream::poll_fn(move |_| {
            if !opened {
                opened = true;
                return Poll::Ready(Some((identity, Ok(Event::Opened(None)))));
            }
            // This relaxed observation identifies a coalesced publication. It
            // provides no acquire edge: visibility must come from Wake::recheck.
            let coalesced = notification.load(Ordering::Relaxed) == 1;
            let current = reading.load(Ordering::Relaxed);
            assert!(
                !coalesced || current == 7,
                "coalesced wake lost its publication"
            );
            if current == 0 {
                return Poll::Pending;
            }
            Poll::Ready(Some((
                identity,
                Ok(Event::Record(crate::Record {
                    sequence: 0,
                    value: vec![current as u8].into(),
                    commit_id: crate::CommitId::from_bytes([7; 32]),
                    committed_at_micros: 0,
                })),
            )))
        })
        .boxed();
        let mut sources = Sources::new();
        sources.insert(identity, source, Some(ALL));
        Arc::get_mut(&mut sources.entries.get_mut(&1).unwrap().wake)
            .unwrap()
            .release_only = release_only;
        let root = Arc::new(Root(AtomicU64::new(0)));
        let waker = futures::task::waker(root);
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            sources.poll((true, true, true), &mut cx),
            Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
        ));
        // Drain the original hint but leave queued==1, exactly the boundary
        // before recheck. The publisher's second wake can join that hint.
        let wake = sources.entries[&1].wake.clone();
        Wake::wake_by_ref(&wake);
        let hint = sources.signal.try_next().unwrap().unwrap();
        sources.wake(hint);
        let writer = loom::thread::spawn(move || {
            value.store(7, Ordering::Relaxed);
            let coalesced = wake.notify();
            notified.store(if coalesced { 1 } else { 2 }, Ordering::Relaxed);
        });
        // Do not join before this poll: join would supply the acquire edge
        // whose absence the release-only negative control must expose.
        let _ = sources.poll((true, true, true), &mut cx);
        writer.join().unwrap();
        // This model checks publication visibility inside the provider poll.
        // Repeated-drain liveness is checked independently by the recheck model.
    });
}
