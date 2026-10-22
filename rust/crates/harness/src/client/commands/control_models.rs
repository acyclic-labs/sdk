use super::super::model::check;
use super::*;
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};
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
            let wake = next
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()));
            assert!(
                !old.paused(&[task]) || !matches!(wake, Poll::Pending),
                "resume wakeup lost"
            );
        } else {
            // One resume can precede the initial poll or follow the parked
            // subscription. Both orders use the production snapshot/follow.
            for early in [false, true] {
                block_on(CommandControl::change(
                    &volume,
                    Some(task),
                    &[task],
                    Some(true),
                    1024,
                ))?;
                let lineage = [task];
                let mut waiting = Box::pin(CommandControl::wait(&volume, &lineage, 1024));
                if !early {
                    assert!(
                        waiting
                            .as_mut()
                            .poll(&mut Context::from_waker(std::task::Waker::noop()))
                            .is_pending()
                    );
                }
                block_on(CommandControl::change(
                    &volume,
                    Some(task),
                    &[task],
                    Some(false),
                    1024,
                ))?;
                block_on(waiting)?;
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

fn worker_keeps_resume_input_live(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("worker-control-model")?,
        ))?);
        let task = TaskId::from_bytes([1; 16]);
        // The same scheduling function called by run_command_client decides
        // whether the input owner waits or launches the owner effect. Model
        // both legal orders of the executor's first poll relative to input.
        for early in [false, true] {
            let stop = CommandResolution::Submit {
                request: CommandRequest::Pause {
                    scope: CommandTaskScope::Subtree,
                },
            };
            block_on(super::super::schedule_command_effect(
                &stop,
                Box::pin(CommandControl::change(
                    &volume,
                    Some(task),
                    &[task],
                    Some(true),
                    1024,
                )),
                || panic!("control input was detached"),
            ))
            .expect("stop must publish before reading the next input")?;
            let lineage = [task];
            let effect = Box::pin(CommandControl::wait(&volume, &lineage, 1024));
            let inline = Box::pin(CommandControl::wait(&volume, &lineage, 1024));
            let mut worker = None;
            let spawn = || {
                worker = Some(Box::pin(effect));
            };
            let shell = CommandResolution::Tool {
                tool: CommandTool::Bash,
                input: serde_json::json!({"command":"echo ready"}),
            };
            let mut input = Box::pin(async {
                if broken {
                    super::super::scheduling::schedule(false, inline, spawn).await
                } else {
                    super::super::schedule_command_effect(&shell, inline, spawn).await
                }
            });
            assert!(
                input
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_ready(),
                "resume input blocked by paused owner"
            );
            drop(input);
            let mut worker = worker.expect("owner effect must be launched");
            if !early {
                assert!(
                    worker
                        .as_mut()
                        .poll(&mut Context::from_waker(std::task::Waker::noop()))
                        .is_pending()
                );
            }
            // This is the next input event; an inline await above prevents it.
            let resume = CommandResolution::Submit {
                request: CommandRequest::Resume {
                    scope: CommandTaskScope::Subtree,
                },
            };
            block_on(super::super::schedule_command_effect(
                &resume,
                Box::pin(CommandControl::change(
                    &volume,
                    Some(task),
                    &lineage,
                    Some(false),
                    1024,
                )),
                || panic!("control input was detached"),
            ))
            .expect("resume input must publish inline")?;
            block_on(worker)?;
        }
        Ok(())
    });
}
#[test]
fn owner_worker_keeps_resume_input_live_through_real_control_files() {
    worker_keeps_resume_input_live(false);
}
#[test]
#[should_panic(expected = "resume input blocked by paused owner")]
fn negative_control_detects_awaiting_a_paused_owner_before_input() {
    worker_keeps_resume_input_live(true);
}

fn control_claim_race(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("control-claim-model")?,
        ))?);
        let task = TaskId::from_bytes([1; 16]);
        block_on(CommandControl::change(
            &volume,
            Some(task),
            &[task],
            Some(true),
            1024,
        ))?;
        let claim = FileValue::new(
            FileKey::parse(".ui/session-worker/execution-claim.json")?,
            b"{}".to_vec(),
        );
        // The competing CAS linearizes before the control snapshot, between
        // snapshot and CAS, or after CAS. Exhaust all three orders; the stream
        // and task-volume models separately prove atomic publication itself.
        for order in 0..3 {
            let publish_claim = || async {
                let cut = volume.snapshot(&Query::Root, 2).await?.generation;
                volume.append(cut, &claim).await
            };
            if order == 0 {
                block_on(publish_claim())?;
            }
            let snapshot = block_on(volume.snapshot(&Query::File(CommandControl::key()?), 1))?;
            if order == 1 {
                let cut = block_on(volume.snapshot(&Query::Root, 2))?.generation;
                let competing = FileValue::new(claim.key.clone(), b"{\"claimed\":true}".to_vec());
                block_on(volume.append(cut, &competing))?;
            }
            let success = if broken {
                let mut control = CommandControl::from_bytes(&snapshot.files[0].bytes, 1024)?;
                control.set(Some(task), snapshot.generation.position, false);
                block_on(volume.append(
                    snapshot.generation,
                    &FileValue::new(CommandControl::key()?, serde_json::to_vec(&control)?),
                ))
                .is_ok()
            } else {
                block_on(CommandControl::publish_change(
                    &volume,
                    Some(task),
                    &[task],
                    Some(false),
                    1024,
                    snapshot,
                ))
                .is_ok()
            };
            assert!(success, "resume publication lost to competing claim");
            if order == 2 {
                block_on(publish_claim())?;
            }
            let saved = block_on(volume.snapshot(&Query::File(CommandControl::key()?), 1))?;
            assert!(!CommandControl::from_bytes(&saved.files[0].bytes, 1024)?.paused(&[task]));
        }
        Ok(())
    });
}
#[test]
fn control_publications_survive_concurrent_owner_claims() {
    control_claim_race(false);
}
#[test]
#[should_panic(expected = "resume publication lost to competing claim")]
fn negative_control_detects_a_lost_resume_on_claim_conflict() {
    control_claim_race(true);
}

fn eof_keeps_a_parked_effect(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("eof-model")?,
        ))?);
        let task = TaskId::from_bytes([1; 16]);
        block_on(CommandControl::change(
            &volume,
            Some(task),
            &[task],
            Some(true),
            1024,
        ))?;
        let lineage = [task];
        let mut worker = Box::pin(CommandControl::wait(&volume, &lineage, 1024));
        assert!(
            worker
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        let finished = broken || super::super::command_loop_finished(false, true, true);
        assert!(!finished, "EOF cancelled a parked command");
        block_on(CommandControl::change(
            &volume,
            Some(task),
            &lineage,
            Some(false),
            1024,
        ))?;
        block_on(worker)?;
        assert!(super::super::command_loop_finished(false, true, false));
        assert!(super::super::command_loop_finished(true, false, true));
        Ok(())
    });
}
#[test]
fn eof_drains_a_parked_owner_while_file_resume_remains_live() {
    eof_keeps_a_parked_effect(false);
}
#[test]
#[should_panic(expected = "EOF cancelled a parked command")]
fn negative_control_detects_exiting_at_eof_before_the_owner_finishes() {
    eof_keeps_a_parked_effect(true);
}
