use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use acyclic_stream::{MemoryStream, StreamPath};

fn accepted_control(broken: bool, check: bool) {
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None).unwrap(),
        "child-refresh-model".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
        &mut None,
    )
    .unwrap();
    let provider: Arc<dyn StreamProvider> = Arc::new(MemoryStream::default());
    let files = Arc::new(loom::future::block_on(Box::pin(
        crate::task_volume::model_checking::files(
            provider,
            StreamPath::new("child-refresh").unwrap(),
            volume,
        ),
    )));
    let reference = Component::new("worker").unwrap();
    let generation = loom::future::block_on(Box::pin(files.inner.head())).unwrap();
    loom::future::block_on(Box::pin(files.inner.append(
        generation,
        &FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Request,
            },
            b"work".to_vec(),
        ),
    )))
    .unwrap();
    let snapshot =
        loom::future::block_on(Box::pin(files.inner.child_lifecycle(Some(&reference)))).unwrap();
    let mut generation = snapshot.generation;
    let mut cursor = Follow::new(snapshot.cursor);
    let mut values = BTreeMap::new();
    // Admission awaits external work while another actor accepts a control.
    // This uses the real cancellation wakeup shim and file-query/provider locks.
    // Lock order: task-volume index is released before provider publication.
    let admitted = acyclic_fs::CancellationToken::new();
    let actor = loom::thread::spawn({
        let files = files.clone();
        let reference = reference.clone();
        let admitted = admitted.clone();
        move || {
            loom::future::block_on(Box::pin(files.inner.append(
                generation,
                &FileValue::new(
                    FileKey::Child {
                        reference,
                        file: ChildFile::Control,
                    },
                    Control::Pause.bytes().to_vec(),
                ),
            )))
            .unwrap();
            admitted.cancel();
        }
    });
    loom::future::block_on(Box::pin(admitted.cancelled()));
    if !broken {
        loom::future::block_on(Box::pin(refresh(
            files.as_ref(),
            &reference,
            &mut cursor,
            &mut values,
            &mut generation,
        )))
        .unwrap();
    }
    let state = desired(&reference, &values).unwrap();
    let published = loom::future::block_on(Box::pin(super::super::publish(
        files.as_ref(),
        &reference,
        ChildStatus {
            task: TaskId::from_bytes([4; 16]),
            state,
            settled: true,
            inputs: 1,
        },
        &mut values,
        &mut generation,
    )))
    .unwrap();
    actor.join().unwrap();
    if check {
        assert!(
            published && state == ChildState::Paused,
            "admission returned to a stale metadata publication cut"
        );
    }
}

#[test]
fn admission_refresh_observes_the_accepted_control_before_publication() {
    loom::model(|| accepted_control(false, true));
    let failure = std::panic::catch_unwind(|| loom::model(|| accepted_control(true, true)))
        .expect_err("missing refresh escaped the metadata cut assertion");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("admission returned to a stale metadata publication cut"),
        "{message}"
    );
    loom::model(|| accepted_control(true, false));
}
