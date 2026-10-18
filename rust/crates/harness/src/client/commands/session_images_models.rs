use super::image_test_support::{Reader, image};
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use std::{
    future::Future as _,
    task::{Context, Waker},
};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn draft_race(broken: bool) {
    loom::model(move || {
        let (volume, mut submitter, mut editor) = loom::future::block_on(Box::pin(async {
            let seed = Arc::new(MemoryStream::default());
            let path = StreamPath::new("image-draft-model").unwrap();
            let volume = Arc::new(
                files::TaskVolume::open(seed.clone(), path.clone())
                    .await
                    .unwrap(),
            );
            let session = uuid::Uuid::from_bytes([1; 16]);
            let mut submitter = CommandSession::open(volume.clone(), session, 65_536, 32)
                .await
                .unwrap();
            submitter.write_state(CommandFileRoot::Config, "image-input.json", &json!({
                "default":"test/model", "models":{"test/model": crate::model::ImageInput::default()}
            })).await.unwrap();
            submitter
                .write_state(
                    CommandFileRoot::Session,
                    "attachments.json",
                    &json!([image(1)]),
                )
                .await
                .unwrap();
            // Streams models its own commit/follow implementation. Bound this
            // composition to its atomic provider contract, retaining the real
            // encoded files, volume locks, client fold and publication paths.
            let provider =
                Arc::new(files::model_checking::Journal::snapshot(&*seed, path.clone()).await);
            let volume = Arc::new(
                files::TaskVolume::open_existing(provider, path)
                    .await
                    .unwrap(),
            );
            let submitter = CommandSession::open(volume.clone(), session, 65_536, 32)
                .await
                .unwrap();
            let editor = CommandSession::open(volume.clone(), session, 65_536, 32)
                .await
                .unwrap();
            (volume, submitter, editor)
        }));
        // Both clients have resolved the same immutable cut before either IO
        // starts. Explore the real commits, cache acknowledgements and follows;
        // serializing pure preparation removes no publication ordering.
        files::model_checking::pause_next_publication();
        let mut update = Box::pin(async move {
            editor
                .write_unchanged_state(
                    CommandFileRoot::Session,
                    "attachments.json",
                    &json!([image(2)]),
                )
                .await
        });
        assert!(
            update
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if broken {
            files::model_checking::set_fault(files::model_checking::Fault::OmitDraftConsumption);
        }
        files::model_checking::pause_next_publication();
        let mut submission = Box::pin(submitter.submit("describe", &Reader));
        assert!(
            submission
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let peer = loom::thread::spawn(move || loom::future::block_on(update));
        let submitted = loom::future::block_on(submission);
        let edited = peer.join().unwrap();
        loom::future::block_on(Box::pin(async {
            let mut restored = CommandSession::open(volume, submitter.session, 65_536, 32)
                .await
                .unwrap();
            let request = restored.take_request();
            let draft = crate::client::host_images::draft_images(&restored).unwrap();
            assert_eq!(request.is_some(), submitted.is_ok());
            if let Some(request) = request {
                let resolution: CommandResolution = serde_json::from_slice(&request.bytes).unwrap();
                let CommandResolution::Prompt { mentions, .. } = resolution else {
                    panic!("prompt missing")
                };
                assert_eq!(mentions.len(), 1);
                assert_eq!(mentions[0].file, image(1));
                assert!(
                    draft.is_empty(),
                    "accepted request did not consume its draft"
                );
                assert!(
                    matches!(edited, Err(Error::Conflict(_))),
                    "peer update crossed consumption"
                );
            } else {
                assert!(matches!(submitted, Err(Error::Conflict(_))));
                assert!(edited.is_ok());
                assert_eq!(
                    draft,
                    vec![image(2)],
                    "rejected submission lost the peer draft"
                );
            }
        }));
    });
}

#[test]
fn submission_and_draft_update_share_one_exact_cut() {
    draft_race(false);
}

#[test]
fn missing_draft_consumption_is_detected_negative_control() {
    let panic = std::panic::catch_unwind(|| draft_race(true))
        .expect_err("omitting consumption must violate the production oracle");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("did not consume its draft"), "{message}");
}
