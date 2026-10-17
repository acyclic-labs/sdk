//! The controlled completion must retain the real capacity wait and handoff.
use super::*;
use crate::{AdmissionState, NativeWorkers, notify_capacity_released, run_blocking_io};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context as PollContext, Wake},
};

struct Woken(AtomicBool);
impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn saturated_queue(bypass: bool) -> io::Result<()> {
    let (sender, receiver) = mpsc::sync_channel(1);
    let workers = NativeWorkers {
        sender,
        admission: Arc::new(Mutex::new(AdmissionState::new())),
    };
    let filler = run_blocking_io(|| (), None)?.pending.take().unwrap();
    assert!(workers.sender.try_send(filler).is_ok());
    workers.admission.lock().unwrap().queued_jobs = 1;

    let effects = Arc::new(AtomicUsize::new(0));
    let executed = effects.clone();
    let mut task = run_blocking_io(
        move || {
            executed.fetch_add(1, Ordering::SeqCst);
            std::thread::current().id()
        },
        None,
    )?;
    let job = task.pending.take().unwrap();
    let mut completion: Pin<Box<dyn Future<Output = io::Result<()>> + '_>> = if bypass {
        // The former controlled path: executes before queue acceptance.
        Box::pin(async move {
            if let Some(waker) = job.run() {
                waker.wake();
            }
            Ok(())
        })
    } else {
        Box::pin(queued(job, &workers))
    };
    let flag = Arc::new(Woken(AtomicBool::new(false)));
    let waker = Waker::from(flag.clone());
    let mut cx = PollContext::from_waker(&waker);
    assert!(
        completion.as_mut().poll(&mut cx).is_pending(),
        "capacity wait was bypassed"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(workers.admission.lock().unwrap().waiter_count, 1);

    let filler = receiver.recv().unwrap();
    notify_capacity_released(&workers.admission);
    drop(filler);
    assert!(flag.0.swap(false, Ordering::SeqCst), "capacity wake lost");
    assert!(completion.as_mut().poll(&mut cx).is_pending());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(workers.admission.lock().unwrap().waiter_count, 0);
    assert_eq!(workers.admission.lock().unwrap().queued_jobs, 1);

    let admission = workers.admission.clone();
    std::thread::spawn(move || {
        let accepted = receiver.recv().unwrap();
        notify_capacity_released(&admission);
        if let Some(waker) = accepted.run() {
            waker.wake();
        }
    })
    .join()
    .unwrap();
    assert!(flag.0.load(Ordering::SeqCst), "terminal wake lost");
    assert!(matches!(
        completion.as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop(completion);
    let worker = match Pin::new(&mut task).poll(&mut cx) {
        Poll::Ready(result) => result?,
        Poll::Pending => panic!("the worker has already published its terminal"),
    };
    assert_ne!(worker, std::thread::current().id());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn controlled_native_completion_waits_for_capacity_and_worker_handoff() -> io::Result<()> {
    saturated_queue(false)
}

#[test]
#[should_panic(expected = "capacity wait was bypassed")]
fn bypassed_capacity_wait_is_rejected_by_the_real_queue_check() {
    saturated_queue(true).unwrap();
}

#[tokio::test]
async fn scoped_completion_uses_the_real_host_worker() -> io::Result<()> {
    crate::initialize_native_io_services()?;
    let (schedule, mut events) = Schedule::new();
    let task = run_blocking_io(|| std::thread::current().name().map(String::from), None)?;
    let mut completion = std::pin::pin!(schedule.scope(task));
    let mut boundaries = Vec::new();
    let worker = loop {
        tokio::select! {
            result = &mut completion => break result?,
            boundary = events.recv() => {
                let boundary = boundary.unwrap();
                boundaries.push(boundary.name);
                boundary.release();
            }
        }
    };
    schedule.drained().await;
    assert_eq!(boundaries, ["native:submitted", "native:completed"]);
    assert!(worker.unwrap().starts_with("acyclic-host-io-"));
    Ok(())
}

#[test]
fn inline_replay_io_preserves_gates_without_a_tokio_blocking_pool() -> io::Result<()> {
    let threads = Arc::new(AtomicUsize::new(0));
    let observed = threads.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .on_thread_start(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .build()?;
    runtime.block_on(async {
        let _inline = crate::InlineBlocking::enter();
        let driver = std::thread::current().id();
        let worker = spawn_blocking(|| std::thread::current().name().map(String::from))
            .await
            .unwrap();
        assert!(worker.unwrap().starts_with("acyclic-host-io-"));
        let (schedule, mut events) = Schedule::new();
        let mut controlled = Box::pin(schedule.scope(async {
            spawn_blocking(|| std::thread::current().id())
                .await
                .unwrap()
        }));
        let mut boundaries = Vec::new();
        loop {
            tokio::select! {
                actual = &mut controlled => { assert_eq!(actual, driver); break; },
                boundary = events.recv() => {
                    let boundary = boundary.unwrap();
                    boundaries.push(boundary.name);
                    boundary.release();
                }
            }
        }
        schedule.drained().await;
        assert_eq!(boundaries, ["blocking:submitted", "blocking:completed"]);
    });
    let check = || {
        assert_eq!(
            threads.load(Ordering::SeqCst),
            0,
            "private blocking pool created"
        )
    };
    check();
    // The former untracked branch starts a real private worker. The same
    // thread-count assertion must reject it, not just compare modeled counts.
    runtime.block_on(async { tokio::task::spawn_blocking(|| ()).await.unwrap() });
    assert!(std::panic::catch_unwind(check).is_err());
    Ok(())
}
