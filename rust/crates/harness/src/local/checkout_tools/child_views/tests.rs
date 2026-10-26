use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const CHILDREN: u64 = 10_000;

// Each child is constructed, used and settled in turn. Returns the views and
// index entries left behind, and the most ever live at once.
fn cycle(settle: bool) -> (usize, usize, usize) {
    let views = StateArc::new(ChildViews::<u64>::default());
    let (live, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
    for child in 0..CHILDREN {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&child.to_le_bytes());
        let task = TaskId::from_bytes(bytes);
        let view = futures::executor::block_on(ChildViews::acquire(
            &views,
            task,
            async || {
                peak.fetch_max(live.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                Ok(child)
            },
            async |_| Ok(()),
        ))
        .unwrap();
        assert_eq!(*view, child);
        drop(view);
        if settle {
            futures::executor::block_on(ChildViews::settle(&views, task, async |_| {
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        }
    }
    let entries = views.lock().entries.len();
    (views.live(), entries, peak.into_inner())
}

#[test]
fn ten_thousand_settled_children_leave_no_view_or_index_entry() {
    assert_eq!(cycle(true), (0, 0, 1));
}

#[test]
fn negative_control_children_that_never_settle_keep_every_view() {
    let children = usize::try_from(CHILDREN).unwrap();
    assert_eq!(cycle(false), (children, children, children));
}

// A child that settles during a call keeps its view until that call ends,
// then retires at the next settlement.
#[test]
fn a_view_in_use_retires_after_its_last_call() {
    let views = StateArc::new(ChildViews::<u8>::default());
    let (first, second) = (TaskId::from_bytes([1; 16]), TaskId::from_bytes([2; 16]));
    let retired = AtomicUsize::new(0);
    let retire = async |_: &u8| {
        retired.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let busy = futures::executor::block_on(ChildViews::acquire(
        &views,
        first,
        async || Ok(1),
        async |_| Ok(()),
    ))
    .unwrap();
    futures::executor::block_on(ChildViews::settle(&views, first, retire)).unwrap();
    assert_eq!((views.live(), retired.load(Ordering::SeqCst)), (1, 0));
    drop(busy);
    futures::executor::block_on(ChildViews::settle(&views, second, retire)).unwrap();
    assert_eq!((views.live(), retired.load(Ordering::SeqCst)), (0, 1));
}

fn refuse() -> Result<()> {
    Err(Error::Conflict("busy".into()))
}

fn settled_view(views: &StateArc<ChildViews<u8>>, task: TaskId) {
    let view = ChildViews::acquire(views, task, async || Ok(1), async |_| Ok(()));
    drop(futures::executor::block_on(view).unwrap());
    let refused = ChildViews::settle(views, task, async |_| refuse());
    assert!(futures::executor::block_on(refused).is_err());
}

// A view whose retirement failed is never leased again. The child's next
// construction first retries its retirement, and fails while that fails.
#[test]
fn a_refused_retirement_withdraws_the_view_until_its_retry() {
    let views = StateArc::new(ChildViews::<u8>::default());
    let task = TaskId::from_bytes([1; 16]);
    settled_view(&views, task);
    assert_eq!(views.live(), 1);
    assert!(ChildViews::get(&views, task).is_none());
    let constructed = AtomicUsize::new(0);
    // Copy, so both constructions below can take it.
    let construct = || async {
        constructed.fetch_add(1, Ordering::SeqCst);
        Ok(2)
    };
    let refused = ChildViews::acquire(&views, task, construct, async |_| refuse());
    assert!(futures::executor::block_on(refused).is_err());
    assert_eq!((views.live(), constructed.load(Ordering::SeqCst)), (1, 0));
    let retried = ChildViews::acquire(&views, task, construct, async |view| {
        assert_eq!(*view, 1);
        Ok(())
    });
    let view = futures::executor::block_on(retried).unwrap();
    assert_eq!((*view, constructed.load(Ordering::SeqCst)), (2, 1));
    drop(view);
    futures::executor::block_on(ChildViews::settle(&views, task, async |_| Ok(()))).unwrap();
    assert_eq!(views.live(), 0);
}

// Any later settlement retries a refused retirement.
#[test]
fn a_refused_retirement_is_retried_at_the_next_settlement() {
    let views = StateArc::new(ChildViews::<u8>::default());
    let (first, second) = (TaskId::from_bytes([1; 16]), TaskId::from_bytes([2; 16]));
    settled_view(&views, first);
    let retired = AtomicUsize::new(0);
    futures::executor::block_on(ChildViews::settle(&views, second, async |_| {
        retired.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }))
    .unwrap();
    assert_eq!((views.live(), retired.load(Ordering::SeqCst)), (0, 1));
    assert_eq!(views.lock().entries.len(), 0);
}
