//! Provider publication racing the real runnable-source scheduler parking.
use super::*;

/// Explores provider publication racing source and session waiter registration.
pub fn verify_source_wakes(omit_signal: bool) {
    loom::model(move || {
        struct Root(AtomicU64);
        impl ArcWake for Root {
            fn wake_by_ref(root: &Arc<Self>) {
                root.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        let published = Arc::new(gate::Gate::new());
        let waiting = published.clone();
        let source = futures::stream::once(async move { (identity, Ok(Event::Opened(None))) })
            .chain(futures::stream::once(async move {
                waiting.changed(false).await.unwrap();
                (identity, Ok(Event::Opened(None)))
            }))
            .boxed();
        let mut sources = Sources::new();
        sources.insert(identity, source, Some(ALL));
        Arc::get_mut(&mut sources.entries.get_mut(&1).unwrap().wake)
            .unwrap()
            .omit_signal = omit_signal;
        let root = Arc::new(Root(AtomicU64::new(0)));
        let waker = futures::task::waker(root.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            sources.poll((true, true, true), &mut cx),
            Poll::Ready(Ok(Some(_)))
        ));
        let writer = loom::thread::spawn(move || published.set(true).unwrap());
        let first = sources.poll((true, true, true), &mut cx);
        let result = if first.is_pending() {
            // Consume the cooperative yield before checking the parked wakeup.
            root.0.store(0, Ordering::SeqCst);
            sources.poll((true, true, true), &mut cx)
        } else {
            first
        };
        writer.join().unwrap();
        if result.is_pending() {
            assert!(
                root.0.load(Ordering::SeqCst) > 0,
                "provider source lost its publication wake"
            );
            assert!(matches!(
                sources.poll((true, true, true), &mut cx),
                Poll::Ready(Ok(Some(_)))
            ));
        } else {
            assert!(matches!(result, Poll::Ready(Ok(Some(_)))));
        }
        sources.remove(1);
        assert!(sources.is_empty());
    });
}
