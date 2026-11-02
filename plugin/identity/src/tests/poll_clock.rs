//! Advance each registered production timer on its original operation path.
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static ADVANCING: Arc<tokio::sync::Mutex<()>> = Arc::default();
}

/// Resume the runtime even if its advancing operation is cancelled.
struct Resume;

impl Drop for Resume {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}

/// Advance only after the original Sleep has registered its actual waker.
pub(crate) async fn observe(delay: tokio::time::Sleep) {
    use std::future::Future as _;
    use std::task::Poll;
    let advancing = ENABLED.with(Cell::get).then(|| ADVANCING.with(Arc::clone));
    tokio::pin!(delay);
    if let Some(advancing) = advancing {
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(delay.as_mut().poll(cx).is_pending())).await;
        if pending {
            let owner = advancing.lock().await;
            if delay.deadline() > tokio::time::Instant::now() {
                tokio::time::pause();
                let resume = Resume;
                let remaining = delay
                    .deadline()
                    .saturating_duration_since(tokio::time::Instant::now());
                // Cross the original timer wheel's rounded millisecond tick.
                tokio::time::advance(remaining.saturating_add(Duration::from_millis(1))).await;
                drop(resume);
            }
            drop(owner);
        }
    }
    delay.await;
}

pub(crate) struct Clock {
    _owner: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Clock {
    pub(crate) fn start() -> Self {
        assert!(matches!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        ));
        ENABLED.with(|enabled| {
            assert!(!enabled.replace(true), "one test clock per runtime thread");
        });
        Self {
            _owner: std::marker::PhantomData,
        }
    }
}

impl Drop for Clock {
    fn drop(&mut self) {
        ENABLED.with(|enabled| enabled.set(false));
    }
}

#[tokio::test]
async fn cancelled_advancement_releases_queued_and_late_timer_observers() {
    use std::future::Future as _;
    use std::task::Poll;
    let _clock = Clock::start();
    // These original timers model the configured device approval interval.
    let interval = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    let late = tokio::time::sleep(interval);
    let queued_deadline = started + interval * 2;
    let queued = observe(tokio::time::sleep_until(queued_deadline));
    tokio::pin!(queued);
    {
        let advancing = observe(tokio::time::sleep(interval));
        tokio::pin!(advancing);
        let result = std::future::poll_fn(|cx| Poll::Ready(advancing.as_mut().poll(cx))).await;
        assert!(result.is_pending(), "advance retains the observing future");
        assert!(
            started.elapsed() >= interval,
            "registered deadline advanced"
        );
        let result = std::future::poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx))).await;
        assert!(
            result.is_pending(),
            "second observer queues behind advancement"
        );
        // Expire the queued observer while the first still owns the clock cut.
        tokio::time::advance(
            queued_deadline.saturating_duration_since(tokio::time::Instant::now())
                + Duration::from_millis(1),
        )
        .await;
    }
    // A canceled advancement must already have resumed time: pause would panic.
    tokio::time::pause();
    let resume = Resume;
    let settled = tokio::time::Instant::now();
    let result = std::future::poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx))).await;
    assert!(
        ADVANCING.with(|advancing| advancing.try_lock().is_ok()),
        "cancelled owner releases the clock and queued observer retires its guard"
    );
    if result.is_pending() {
        queued.await;
    }
    observe(late).await;
    assert_eq!(
        tokio::time::Instant::now(),
        settled,
        "expired observers add no clock jump"
    );
    drop(resume);
}
