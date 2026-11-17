use super::*;
use loom::{
    sync::{Arc, Mutex},
    thread,
};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Namespace {
    staging: [Option<(usize, bool)>; 2],
    project: Option<usize>,
}

fn concurrent_first_opens(shared_staging: bool) {
    // The model mutex stands for one atomic filesystem namespace operation.
    // It is a leaf: release it before preparation, callbacks, or a yield.
    let namespace = Arc::new(Mutex::new(Namespace::default()));
    let workers: Vec<_> = (0..2)
        .map(|attempt| {
            let namespace = namespace.clone();
            thread::spawn(move || {
                let slot = if shared_staging { 0 } else { attempt };
                let prepare = async {
                    namespace.lock().unwrap().staging[slot] = Some((attempt, false));
                    thread::yield_now();
                    let mut names = namespace.lock().unwrap();
                    let owned = names.staging[slot].as_mut().expect("staging lost");
                    assert_eq!(owned.0, attempt, "staging lost");
                    owned.1 = true;
                    Ok(())
                };
                let publish = || {
                    let mut names = namespace.lock().unwrap();
                    if names.project.is_some() {
                        return Err(std::io::ErrorKind::AlreadyExists.into());
                    }
                    assert_eq!(names.staging[slot], Some((attempt, true)), "staging lost");
                    names.staging[slot] = None;
                    names.project = Some(attempt);
                    Ok(())
                };
                let discard = || {
                    let mut names = namespace.lock().unwrap();
                    assert_eq!(names.staging[slot], Some((attempt, true)), "staging lost");
                    names.staging[slot] = None;
                    Ok(())
                };
                // Poll the production initialization future. Its only modeled
                // waits are the filesystem callbacks and explicit scheduling cut.
                match std::pin::pin!(initialize(prepare, publish, discard))
                    .poll(&mut Context::from_waker(Waker::noop()))
                {
                    Poll::Ready(result) => result.unwrap(),
                    Poll::Pending => panic!("initialization unexpectedly suspended"),
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let names = namespace.lock().unwrap();
    assert!(names.project.is_some());
    assert_eq!(names.staging, [None, None]);
}

#[test]
fn initialization_keeps_attempt_custody_until_atomic_publication() {
    loom::model(|| concurrent_first_opens(false));
    let failure = std::panic::catch_unwind(|| loom::model(|| concurrent_first_opens(true)))
        .expect_err("shared-staging negative control was not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("staging lost"),
        "unexpected negative control: {message}"
    );
}
