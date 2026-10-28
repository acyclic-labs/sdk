//! Retained admission histories at the production channel and source queues.
use super::*;
use futures::FutureExt;

fn channel(latest: bool, pending: usize, credited: bool) -> Channel {
    Channel {
        binding: None,
        generation: 1,
        latest,
        opaque_cursor: false,
        revalidate: None,
        progress: None,
        pending: (0..pending)
            .map(|_| Pending { frame: Default::default(), key: None })
            .collect(),
        indexed: None,
        credit: wire::SessionWindow { frames: u32::from(credited), bytes: u64::from(credited) },
        sent: Default::default(),
        applied: Default::default(),
        ending: None,
    }
}

fn record() -> crate::Record {
    crate::Record {
        headers: Default::default(),
        sequence: 0,
        value: vec![7].into(),
        commit_id: crate::CommitId::from_bytes([1; 32]),
        committed_at_micros: 0,
    }
}

fn source(identity: wire::SessionChannel, records: Arc<signal::Signal<crate::Record>>) -> Source {
    futures::stream::once(async move { (identity, Ok(Event::Opened(None))) })
        .chain(futures::stream::once(async move {
            (identity, Ok(Event::Record(records.next().await.unwrap())))
        }))
        .boxed()
}

fn poll(
    sources: &mut Sources,
    room: (bool, bool, bool),
    cx: &mut Context<'_>,
) -> Poll<Result<Option<SourceItem>, StreamError>> {
    let mut control = Sources::new();
    sources.next_with_control(&mut control, room).poll_unpin(cx)
}

/// The reserved data slot must remain available to an independent credited source.
pub fn verify_source_saturation(global_gate: bool) {
    for latest in [false, true] {
        loom::model(move || {
            let mut sources = Sources::new();
            let blocked = channel(latest, if latest { 3 } else { 1 }, false);
            let mut healthy = channel(false, 0, false);
            let slow_records = Arc::new(signal::Signal::new());
            let records = Arc::new(signal::Signal::new());
            let root = Arc::new(Root(AtomicU64::new(0)));
            let wake = futures::task::waker(root.clone());
            let mut cx = Context::from_waker(&wake);
            for (id, channel, records) in [
                (1, &blocked, slow_records.clone()),
                (2, &healthy, records.clone()),
            ] {
                let identity = wire::SessionChannel { channel_id: id, generation: 1 };
                sources.insert(identity, source(identity, records), channel.source_mode());
                assert!(matches!(poll(&mut sources, (false, false, false), &mut cx),
                    Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
                ));
            }
            slow_records.send(record()).unwrap();
            // Two blocked records leave exactly the reserved third slot.
            let room = policy::room(3, 1000, 2, 200, 100, true);
            assert!(poll(&mut sources, room, &mut cx).is_pending());
            healthy.credit = wire::SessionWindow { frames: 1, bytes: 1 };
            sources.mode(2, healthy.source_mode());
            let writer = loom::thread::spawn(move || records.send(record()).unwrap());
            root.0.store(0, Ordering::SeqCst);
            let room = if global_gate { (room.0, room.0, room.2) } else { room };
            let first = poll(&mut sources, room, &mut cx);
            writer.join().unwrap();
            let delivered = if first.is_pending() {
                if !global_gate {
                    assert!(root.0.load(Ordering::SeqCst) > 0);
                }
                poll(&mut sources, room, &mut cx)
            } else { first };
            assert!(matches!(delivered,
                Poll::Ready(Ok(Some((identity, Ok(Event::Record(record))))))
                    if identity.channel_id == 2 && record.sequence == 0 && record.value.as_ref() == [7]),
                "the reserved slot must enable the healthy channel");
            assert_eq!(slow_records.try_next().unwrap().unwrap().value.as_ref(), [7]);
        });
    }
}

/// Disabling an already parked provider must preserve its next unpublished record.
pub fn verify_source_disabled(keep_polling: bool) {
    loom::model(move || {
        let identity = wire::SessionChannel { channel_id: 1, generation: 1 };
        let mut channel = channel(false, 0, true);
        let records = Arc::new(signal::Signal::new());
        let mut sources = Sources::new();
        sources.insert(identity, source(identity, records.clone()), channel.source_mode());
        let wake = futures::task::noop_waker();
        let mut cx = Context::from_waker(&wake);
        assert!(matches!(poll(&mut sources, (true, true, true), &mut cx),
            Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
                ));
        assert!(poll(&mut sources, (true, true, true), &mut cx).is_pending());
        let published = records.clone();
        let writer = loom::thread::spawn(move || published.send(record()).unwrap());
        channel.credit.frames = 0;
        if !keep_polling {
            sources.mode(1, channel.source_mode());
        }
        writer.join().unwrap();
        assert!(poll(&mut sources, (true, true, true), &mut cx).is_pending(),
            "a disabled source must not consume a record");
        // Re-admit the same provider and consume its original queued record.
        channel.credit.frames = 1;
        sources.mode(1, channel.source_mode());
        assert!(matches!(poll(&mut sources, (true, true, true), &mut cx),
            Poll::Ready(Ok(Some((_, Ok(Event::Record(record)))))) if record.sequence == 0 && record.value.as_ref() == [7]));
    });
}

/// Generated capacity cases traverse the real queue admission, not a copied policy.
pub fn verify_reserved_capacity(frames: usize, largest: usize) {
    loom::model(move || {
        let room = policy::room(frames, 4 * largest, frames - 1, 0, largest, true);
        let full = policy::room(frames, 4 * largest, frames, 0, largest, true);
        for (channel, room, expected) in [
            (channel(true, 0, false), room, false),
            (channel(false, 0, true), room, true),
            (channel(false, 0, true), full, false),
        ] {
            let identity = wire::SessionChannel { channel_id: 1, generation: 1 };
            let mut sources = Sources::new();
            let records = Arc::new(signal::Signal::new());
            records.send(record()).unwrap();
            sources.insert(identity, source(identity, records), channel.source_mode());
            let wake = futures::task::noop_waker();
            let mut cx = Context::from_waker(&wake);
            assert!(matches!(poll(&mut sources, room, &mut cx),
                Poll::Ready(Ok(Some((_, Ok(Event::Opened(_))))))
                ));
            assert_eq!(matches!(poll(&mut sources, room, &mut cx),
                Poll::Ready(Ok(Some((_, Ok(Event::Record(_))))))), expected);
        }
    });
}
