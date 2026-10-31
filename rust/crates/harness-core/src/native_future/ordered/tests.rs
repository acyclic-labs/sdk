use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Gate {
    completed: AtomicBool,
    notify_during_poll: AtomicBool,
    polls: AtomicUsize,
    wake: Mutex<Option<Waker>>,
}

struct Borrowed<'a> {
    gate: &'a Gate,
    value: usize,
    dropped: &'a AtomicUsize,
    original: Option<(&'a NativeMaterializationWork, u64)>,
}

impl Future for Borrowed<'_> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<usize> {
        self.gate.polls.fetch_add(1, Ordering::SeqCst);
        *lock(&self.gate.wake) = Some(context.waker().clone());
        if self.gate.notify_during_poll.swap(false, Ordering::SeqCst) {
            self.gate.completed.store(true, Ordering::SeqCst);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        if self.gate.completed.load(Ordering::SeqCst) {
            Poll::Ready(self.value)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Borrowed<'_> {
    fn drop(&mut self) {
        if let Some((work, baseline)) = self.original {
            assert!(work.live_allocation_bytes() > baseline);
        }
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ParentWake(AtomicUsize);

impl Wake for ParentWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn notified_order_reuses_slots_and_retains_custody_until_the_last_waker() -> Result<()> {
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 32 * 1024 * 1024),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = original.live_allocation_bytes();
        let work = native.then_some(&original);
        let gates: [Gate; 4] = std::array::from_fn(|_| Gate::default());
        let dropped = AtomicUsize::new(0);
        let build = |value| Borrowed {
            gate: &gates[value - 1],
            value,
            dropped: &dropped,
            original: work.map(|work| (work, baseline)),
        };
        let parent = Arc::new(ParentWake::default());
        let waker = Waker::from(Arc::clone(&parent));
        let mut context = Context::from_waker(&waker);
        let mut queue = NativeOrdered::new(work)?;
        let first = queue.push_back(build(1))?;
        let second = queue.push_back(build(2))?;
        let third = queue.push_back(build(3))?;
        assert!(queue.poll_next(&mut context).is_pending());
        assert!(
            gates
                .iter()
                .take(3)
                .all(|gate| gate.polls.load(Ordering::SeqCst) == 1)
        );

        gates[1].completed.store(true, Ordering::SeqCst);
        let retained = lock(&gates[1].wake)
            .as_ref()
            .expect("second was polled")
            .clone();
        retained.wake_by_ref();
        assert!(parent.0.load(Ordering::SeqCst) > 0);
        assert!(queue.poll_next(&mut context).is_pending());
        assert_eq!(gates[1].polls.load(Ordering::SeqCst), 2);
        assert_eq!(gates[0].polls.load(Ordering::SeqCst), 1);
        assert_eq!(gates[2].polls.load(Ordering::SeqCst), 1);

        // Lost-wake negative: changing producer state without its notification
        // cannot cause an unnotified future to be polled or make progress.
        gates[0].completed.store(true, Ordering::SeqCst);
        assert!(queue.poll_next(&mut context).is_pending());
        assert_eq!(gates[0].polls.load(Ordering::SeqCst), 1);
        lock(&gates[0].wake)
            .as_ref()
            .expect("first was polled")
            .wake_by_ref();
        assert!(matches!(
            queue.poll_next(&mut context),
            Poll::Ready(Ok(Some(1)))
        ));
        assert!(matches!(
            queue.poll_next(&mut context),
            Poll::Ready(Ok(Some(2)))
        ));

        let fourth = queue.push_back(build(4))?;
        assert_eq!(fourth.index, second.index);
        assert_ne!(fourth.generation, second.generation);
        let before = lock(&queue.ready.queue).length;
        retained.wake_by_ref();
        assert_eq!(lock(&queue.ready.queue).length, before);
        assert!(!queue.cancel(&second)?);
        assert!(queue.cancel(&third)?);
        assert!(queue.cancel(&fourth)?);
        assert!(!queue.cancel(&fourth)?);
        assert!(queue.is_empty());
        assert_eq!(lock(&queue.ready.queue).length, 0);

        let capacity = queue.slots.capacity();
        for _ in 0..128 {
            let key = queue.push_back(build(4))?;
            assert!(queue.cancel(&key)?);
        }
        assert_eq!(queue.slots.capacity(), capacity);
        assert_eq!(lock(&queue.ready.queue).length, 0);
        // A producer notifying from inside poll must remain queued when that
        // poll returns Pending; no population recheck repairs a lost event.
        gates[3].notify_during_poll.store(true, Ordering::SeqCst);
        let during_poll = queue.push_back(build(4))?;
        let before = parent.0.load(Ordering::SeqCst);
        assert!(queue.poll_next(&mut context).is_pending());
        assert!(parent.0.load(Ordering::SeqCst) > before);
        assert!(matches!(
            queue.poll_next(&mut context),
            Poll::Ready(Ok(Some(4)))
        ));
        drop(during_poll);
        drop((first, second, third, fourth));
        for gate in &gates {
            drop(lock(&gate.wake).take());
        }
        drop(queue);
        assert_eq!(dropped.load(Ordering::SeqCst), 133);
        if native {
            let retained_bytes = acyclic_native_runtime::arc_allocation_bytes::<ChildWake>() as u64
                + acyclic_native_runtime::arc_allocation_bytes::<Ready>() as u64;
            assert_eq!(original.live_allocation_bytes(), baseline + retained_bytes);
        }
        // The queue no longer exists; this wake is inert and still funded.
        retained.wake_by_ref();
        drop(retained);
        assert_eq!(original.live_allocation_bytes(), baseline);

        if native {
            gates[0].completed.store(false, Ordering::SeqCst);
            let mut queue = NativeOrdered::new(work)?;
            queue.push_back(build(1))?;
            assert!(queue.poll_next(&mut context).is_pending());
            let before = parent.0.load(Ordering::SeqCst);
            cancellation.cancel();
            assert!(parent.0.load(Ordering::SeqCst) > before);
            assert!(matches!(
                queue.poll_next(&mut context),
                Poll::Ready(Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    ..
                }))
            ));
            drop(queue);
            drop(lock(&gates[0].wake).take());
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
