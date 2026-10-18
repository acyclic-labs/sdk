use super::*;
use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

fn fence(broken: bool, assert_property: bool) {
    let durable = Arc::new(AtomicBool::new(false));
    let signal = acyclic_fs::CancellationToken::new();
    let flush = signal.clone();
    let written = durable.clone();
    let prepare = async move {
        // The flusher becomes runnable only after the complete staged prefix.
        Ok(thread::spawn(move || {
            written.store(true, Ordering::Release);
            flush.cancel();
        }))
    };
    let commit = async move {
        signal.cancelled().await;
        Ok(())
    };
    if broken {
        crate::task_volume::model_checking::set_fault(
            crate::task_volume::model_checking::Fault::SkipChildInputFence,
        );
    }
    let writer = loom::future::block_on(Box::pin(fenced_inputs(prepare, commit))).unwrap();
    let fenced = durable.load(Ordering::Acquire);
    writer.join().unwrap();
    if assert_property {
        assert!(fenced, "child admission escaped its staged input fence");
    }
}

#[test]
fn child_preparation_waits_for_the_actual_input_fence() {
    loom::model(|| fence(false, true));
    let failure = std::panic::catch_unwind(|| loom::model(|| fence(true, true)))
        .expect_err("missing input fence was not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("child admission escaped its staged input fence"),
        "unexpected negative control: {message}"
    );
    loom::model(|| fence(true, false));
}
