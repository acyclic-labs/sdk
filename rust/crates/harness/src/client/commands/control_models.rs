use super::*;
use acyclic_stream::{
    MemoryStream, StreamPath,
    verification::{block_on, check},
};
use std::{
    future::Future as _,
    sync::Arc,
    task::{Context, Poll},
};

fn resume_wakeup(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("control-model")?,
        ))?);
        let (waker, woken) = super::super::query_model::wake_flag();
        let task = TaskId::from_bytes([1; 16]);
        block_on(CommandControl::change(
            &volume,
            Some(task),
            &[task],
            Some(true),
            1024,
        ))?;
        if broken {
            // Deliberately broken: separate the old paused snapshot from a
            // newly opened follow. A resume between them is lost.
            let snapshot = block_on(volume.snapshot(&Query::File(CommandControl::key()?), 1))?;
            block_on(CommandControl::change(
                &volume,
                Some(task),
                &[task],
                Some(false),
                1024,
            ))?;
            let observed = block_on(volume.observe(Query::File(CommandControl::key()?), None, 1))?;
            let old = CommandControl::from_bytes(&snapshot.files[0].bytes, 1024)?;
            let mut changes = observed.changes;
            let mut next = Box::pin(changes.next());
            let wake = next.as_mut().poll(&mut Context::from_waker(&waker));
            assert!(
                !old.paused(&[task]) || !matches!(wake, Poll::Pending),
                "resume wakeup lost"
            );
        } else {
            let publisher = loom::thread::spawn({
                let volume = volume.clone();
                move || {
                    block_on(CommandControl::change(
                        &volume,
                        Some(task),
                        &[task],
                        Some(false),
                        1024,
                    ))
                }
            });
            let lineage = [task];
            let mut wait = Box::pin(CommandControl::wait(&volume, &lineage, 1024));
            let first = wait.as_mut().poll(&mut Context::from_waker(&waker));
            publisher
                .join()
                .map_err(|_| "resume publisher panicked")??;
            if first.is_pending() {
                assert!(
                    woken.load(loom::sync::atomic::Ordering::SeqCst),
                    "follow did not wake its waiter"
                );
                assert!(
                    matches!(
                        wait.as_mut().poll(&mut Context::from_waker(&waker)),
                        Poll::Ready(Ok(()))
                    ),
                    "resume wakeup lost"
                );
            }
        }
        Ok(())
    });
}
#[test]
fn resume_publication_cannot_cross_the_control_snapshot_follow_cut() {
    resume_wakeup(false);
}
#[test]
#[should_panic(expected = "resume wakeup lost")]
fn negative_control_detects_subscribing_after_the_pause_snapshot() {
    resume_wakeup(true);
}
