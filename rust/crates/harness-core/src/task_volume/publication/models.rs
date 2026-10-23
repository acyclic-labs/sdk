//! Two single-file writers, using the production retry loop and modeled provider.
use super::*;
use acyclic_stream::MemoryStream;

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn scenario(broken: bool) {
    let volume = Arc::new(
        loom::future::block_on(Box::pin(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("publication-model").unwrap(),
        )))
        .unwrap(),
    );
    let goal = Write::new(FileKey::Goal, b"goal".to_vec());
    let output = Write::new(
        FileKey::Task {
            task: Component::new("task").unwrap(),
            file: TaskFile::Tool {
                turn: 0,
                call: Component::new("bash").unwrap(),
                file: ToolFile::Stdout(0),
            },
        },
        b"stdout".to_vec(),
    );
    let generation = loom::future::block_on(Box::pin(volume.head())).unwrap();
    // Prepare the client's immutable record before the hand-off. The model
    // explores its actual atomic provider append against the entire output
    // retry path, without modeling unrelated client index reconstruction.
    let (record, _) = segment::encode(&goal, None).unwrap();
    let provider = volume.provider.clone();
    let path = volume.path.clone();
    let peer = loom::thread::spawn(move || {
        loom::future::block_on(Box::pin(provider.append(AppendRequest {
            path,
            records: vec![record.into()],
            if_tail: None,
            idempotency_key: None,
        })))
        .unwrap();
    });
    if broken {
        model_checking::set_fault(model_checking::Fault::StopAfterGenerationConflict);
    }
    let result = loom::future::block_on(Box::pin(publish(&volume, generation, &output, || {
        std::future::ready(Ok::<_, Error>((Vec::new(), None)))
    })));
    assert!(
        result.is_ok(),
        "output stopped on unrelated generation conflict"
    );
    peer.join().unwrap();
    let snapshot = loom::future::block_on(Box::pin(volume.snapshot(&Query::Root, 8))).unwrap();
    assert!(snapshot.files.contains(&goal));
    assert!(snapshot.files.contains(&output));
}

#[test]
fn unrelated_writers_preserve_both_files_with_negative_control() {
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("missing generation retry was accepted");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("output stopped on unrelated generation conflict"),
        "{text}"
    );
}

#[test]
fn a_retry_revalidates_the_owner_before_publishing() {
    fn scenario(broken: bool) {
        let volume = loom::future::block_on(Box::pin(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("owner-retry-model").unwrap(),
        )))
        .unwrap();
        let goal = Write::new(FileKey::Goal, b"client".to_vec());
        let output = Write::new(
            FileKey::Task {
                task: Component::new("task").unwrap(),
                file: TaskFile::Tool {
                    turn: 0,
                    call: Component::new("bash").unwrap(),
                    file: ToolFile::Stdout(0),
                },
            },
            b"stdout".to_vec(),
        );
        if broken {
            model_checking::set_fault(model_checking::Fault::StopAfterGenerationConflict);
        }
        let mut calls = 0;
        let result = loom::future::block_on(Box::pin(publish(
            &volume,
            loom::future::block_on(Box::pin(volume.head())).unwrap(),
            &output,
            || {
                calls += 1;
                if calls == 1 {
                    let head = loom::future::block_on(Box::pin(volume.head())).unwrap();
                    loom::future::block_on(Box::pin(volume.append(head, &goal))).unwrap();
                    std::future::ready(Ok((Vec::new(), None)))
                } else {
                    std::future::ready(Err(crate::Error::Unauthorized("owner lost".into())))
                }
            },
        )));
        assert_eq!(calls, 2, "retry did not revalidate owner");
        assert!(matches!(result, Err(crate::Error::Unauthorized(_))));
        let snapshot = loom::future::block_on(Box::pin(volume.snapshot(&Query::Root, 8))).unwrap();
        assert_eq!(snapshot.files, [goal]);
    }
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("missing retry revalidation was accepted");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains("retry did not revalidate owner"), "{text}");
}
