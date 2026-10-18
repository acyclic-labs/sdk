use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use acyclic_stream::{MemoryStream, StreamPath};

fn input_ack(broken: bool, assert_property: bool) {
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None).unwrap(),
        "child-input-model".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
        &mut None,
    )
    .unwrap();
    let provider: Arc<dyn StreamProvider> = Arc::new(MemoryStream::default());
    let path = StreamPath::new("child-input-ack").unwrap();
    let files = loom::future::block_on(Box::pin(crate::task_volume::model_checking::files(
        provider.clone(),
        path.clone(),
        volume,
    )));
    let reference = Component::new("worker").unwrap();
    let mut values = BTreeMap::new();
    let mut generation = loom::future::block_on(Box::pin(files.inner.head())).unwrap();
    for (file, bytes) in [
        (ChildFile::Request, b"work".as_slice()),
        (
            ChildFile::Inbox(Component::new("next").unwrap()),
            b"next".as_slice(),
        ),
    ] {
        generation = loom::future::block_on(Box::pin(files.inner.append(
            generation,
            &FileValue::new(
                FileKey::Child {
                    reference: reference.clone(),
                    file,
                },
                bytes.to_vec(),
            ),
        )))
        .unwrap();
    }
    let status = ChildStatus {
        task: TaskId::from_bytes([2; 16]),
        state: ChildState::Running,
        settled: false,
        inputs: 1,
    };
    loom::future::block_on(Box::pin(super::super::publish(
        &files,
        &reference,
        status,
        &mut values,
        &mut generation,
    )))
    .unwrap();
    let result = |turn| {
        FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Result(turn),
            },
            serde_json::to_vec(&crate::task_volume::ChildTurnResult {
                task: status.task,
                turn,
                outcome: crate::Outcome::Cancelled,
            })
            .unwrap(),
        )
    };
    generation =
        loom::future::block_on(Box::pin(files.inner.append(generation, &result(1)))).unwrap();
    if broken {
        crate::task_volume::model_checking::set_fault(
            crate::task_volume::model_checking::Fault::SkipChildInputAck,
        );
    }
    let admitted = loom::future::block_on(Box::pin(publish(
        &files,
        &reference,
        ChildStatus {
            inputs: 2,
            ..status
        },
        &mut values,
        &mut generation,
    )))
    .unwrap();
    let answer = if admitted {
        Some(loom::future::block_on(Box::pin(
            files.inner.append(generation, &result(2)),
        )))
    } else {
        None
    };
    if assert_property {
        assert!(
            admitted && answer.is_some_and(|result| result.is_ok()),
            "live child answer escaped its input acknowledgement"
        );
    }
}

#[test]
fn live_input_ack_precedes_its_result_publication() {
    loom::model(|| input_ack(false, true));
    let failure = std::panic::catch_unwind(|| loom::model(|| input_ack(true, true)))
        .expect_err("missing input acknowledgement was not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("live child answer escaped its input acknowledgement"),
        "unexpected control failure: {message}"
    );
    loom::model(|| input_ack(true, false));
}
