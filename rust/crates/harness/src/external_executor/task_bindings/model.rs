use std::cell::Cell;

loom::thread_local! {static UNCACHED: Cell<bool> = Cell::new(false);}
pub(super) fn uncached() -> bool {
    UNCACHED.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use loom::{
        future::block_on,
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };

    fn overlap(broken: bool) {
        let bindings = Arc::new(TaskBindings::<usize>::default());
        let active = Arc::new(AtomicUsize::new(0));
        let task = TaskId::from_bytes([1; 16]);
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let bindings = bindings.clone();
                let active = active.clone();
                thread::spawn(move || {
                    UNCACHED.with(|mode| mode.set(broken));
                    let mut bound = block_on(bindings.lock(task));
                    let overlapped = active.fetch_add(1, Ordering::SeqCst);
                    thread::yield_now();
                    *bound += 1;
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(bound);
                    overlapped
                })
            })
            .collect();
        let overlaps: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        let bound = block_on(bindings.lock(task));
        let value = *bound;
        drop(bound);
        assert!(
            overlaps.into_iter().all(|count| count == 0),
            "task binding admitted overlapping commands"
        );
        assert_eq!(value, 2, "task binding lost its cached provider");
    }

    #[test]
    fn same_task_calls_share_one_retained_binding() {
        loom::model(|| overlap(false));
    }

    #[test]
    #[should_panic(expected = "task binding")]
    fn negative_control_detects_uncached_task_gates() {
        loom::model(|| overlap(true));
    }

    #[test]
    fn a_waiting_task_never_holds_the_index_needed_by_another_task() {
        loom::model(|| {
            let bindings = Arc::new(TaskBindings::<usize>::default());
            let first = TaskId::from_bytes([1; 16]);
            let second = TaskId::from_bytes([2; 16]);
            let held = block_on(bindings.lock(first));
            let waiter = bindings.clone();
            let waiter = thread::spawn(move || drop(block_on(waiter.lock(first))));
            let other = bindings.clone();
            let other = thread::spawn(move || *block_on(other.lock(second)) += 1);
            // Backend progress must complete while the first gate remains held,
            // including schedules where its second caller already entered.
            other.join().unwrap();
            drop(held);
            waiter.join().unwrap();
            assert_eq!(*block_on(bindings.lock(second)), 1);
        });
    }
}
