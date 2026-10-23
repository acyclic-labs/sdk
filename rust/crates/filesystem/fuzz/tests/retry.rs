use acyclic_fs::CancellationToken;
use std::cell::Cell;
use std::future::{Future, ready};
use std::task::{Context, Poll, Waker};

#[path = "../../src/access/retry.rs"]
mod protocol;

fn run<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("bounded observation must complete"),
    }
}

fn shutdown_model(broken: bool, cancel_caller: bool) {
    loom::model(move || {
        let caller = CancellationToken::new();
        let mount = CancellationToken::new();
        let cancelled = if cancel_caller {
            caller.clone()
        } else {
            mount.clone()
        };
        let (send, receive) = loom::sync::mpsc::channel();
        let mut shutdown = Some(loom::thread::spawn(move || {
            receive.recv().unwrap();
            cancelled.cancel();
        }));
        let reads = Cell::new(0);
        let observe = || {
            let count = reads.get() + 1;
            reads.set(count);
            if count == 2 {
                send.send(()).unwrap();
                shutdown.take().unwrap().join().unwrap();
            }
            ready(Ok::<_, ()>((count > 2).then_some(())))
        };
        let result = if broken {
            run(protocol::observe_policy_unfenced(&caller, &mount, observe))
        } else {
            run(protocol::observe_policy(&caller, &mount, observe))
        };
        let expected = if cancel_caller {
            matches!(result, Err(protocol::RetryError::CallerCancelled))
        } else {
            matches!(result, Err(protocol::RetryError::MountCancelled))
        };
        assert!(
            expected && reads.get() == 2,
            "policy retry continued after shutdown"
        );
        assert_eq!(caller.is_cancelled(), cancel_caller);
        assert_eq!(mount.is_cancelled(), !cancel_caller);
    });
}

#[test]
fn repeated_invalid_observations_stop_on_either_independent_cancellation() {
    shutdown_model(false, false);
    shutdown_model(false, true);
}

#[test]
#[should_panic(expected = "policy retry continued after shutdown")]
fn missing_mount_cancellation_negative_control() {
    shutdown_model(true, false);
}
