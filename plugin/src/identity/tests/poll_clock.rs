//! Advance registered production polling timers while real HTTP I/O progresses.
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Sleeps = Arc<Mutex<VecDeque<Duration>>>;
thread_local! {
    static SLEEPS: RefCell<Option<Sleeps>> = const { RefCell::new(None) };
}

fn sleeping(duration: Duration) {
    SLEEPS.with_borrow(|sleeps| {
        if let Some(sleeps) = sleeps {
            sleeps.lock().expect("clock waits").push_back(duration);
        }
    });
}

/// Register a timer only once its actual Sleep has first returned Pending.
pub(in crate::identity) async fn observe(delay: tokio::time::Sleep, duration: Duration) {
    use std::future::Future as _;
    use std::task::Poll;
    tokio::pin!(delay);
    let mut registered = false;
    std::future::poll_fn(|cx| match delay.as_mut().poll(cx) {
        Poll::Ready(()) => Poll::Ready(()),
        Poll::Pending => {
            if !registered {
                sleeping(duration);
                registered = true;
            }
            Poll::Pending
        }
    })
    .await;
}

pub(in crate::identity) struct Clock {
    task: tokio::task::JoinHandle<()>,
}

impl Clock {
    pub(in crate::identity) fn start() -> Self {
        let sleeps: Sleeps = Arc::default();
        SLEEPS.with_borrow_mut(|current| {
            assert!(current.is_none(), "one polling clock per test runtime");
            *current = Some(Arc::clone(&sleeps));
        });
        tokio::time::pause();
        let task = tokio::spawn(async move {
            let mut observed_wall = Instant::now();
            loop {
                // Real socket and persistence deadlines must still expire even
                // if a peer stalls or the awaiting task stays runnable.
                let now = Instant::now();
                let elapsed = now.duration_since(observed_wall);
                if elapsed >= Duration::from_millis(1) {
                    observed_wall = now;
                    tokio::time::advance(elapsed).await;
                }
                let duration = sleeps.lock().expect("clock waits").pop_front();
                if let Some(duration) = duration {
                    // This timer returned Pending before registration. Advance
                    // its actual requested delay, preserving polling intervals.
                    // Tokio rounds timer deadlines up to the next millisecond.
                    // Cross that timer-wheel tick without waiting out the polling delay.
                    tokio::time::advance(duration.saturating_add(Duration::from_millis(1))).await;
                }
                // Remain runnable so Tokio cannot auto-advance unrelated HTTP
                // timeouts while the loopback server waits for real socket I/O.
                tokio::task::yield_now().await;
            }
        });
        Self { task }
    }
}

impl Drop for Clock {
    fn drop(&mut self) {
        self.task.abort();
        SLEEPS.with_borrow_mut(|current| {
            current.take();
        });
        tokio::time::resume();
    }
}

#[tokio::test]
async fn stalled_io_timeout_expires_while_poll_clock_is_active() {
    use std::future::Future as _;
    use std::task::Poll;
    let _clock = Clock::start();
    let deadline = tokio::time::timeout(Duration::from_millis(20), std::future::pending::<()>());
    tokio::pin!(deadline);
    let wall = Instant::now();
    let expired = std::future::poll_fn(|cx| {
        if let Poll::Ready(result) = deadline.as_mut().poll(cx) {
            assert!(result.is_err());
            return Poll::Ready(true);
        }
        // Independent real time makes the old frozen-clock negative control
        // terminate promptly even though its Tokio timeout never expires.
        if wall.elapsed() >= Duration::from_millis(200) {
            return Poll::Ready(false);
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
    assert!(expired, "real I/O deadline was frozen by the polling clock");
}
