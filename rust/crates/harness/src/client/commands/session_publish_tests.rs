use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use std::cell::Cell;

thread_local! {
    static EXPECTED: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static OBSERVATIONS: Cell<usize> = const { Cell::new(0) };
    static COPY_PAYLOAD: Cell<bool> = const { Cell::new(false) };
}

// This mutation and allocation-identity oracle run at the production writer's
// owned-file handoff, before any required wire encoding. Retaining the original
// allocation in the test makes address reuse impossible.
pub(super) fn handoff(file: files::FileValue) -> files::FileValue {
    let file = if COPY_PAYLOAD.with(Cell::get) {
        files::FileValue {
            bytes: file.bytes.to_vec().into(),
            ..file
        }
    } else {
        file
    };
    if let Some(expected) = EXPECTED.with(Cell::get) {
        assert_eq!(
            (file.bytes.as_ptr() as usize, file.bytes.len()),
            expected,
            "publication payload allocation changed"
        );
        OBSERVATIONS.with(|count| count.set(count.get() + 1));
    }
    file
}

async fn publish_owned(body: Vec<u8>) {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("owned-command-publication").unwrap();
    let volume = Arc::new(
        files::TaskVolume::open(provider.clone(), path.clone())
            .await
            .unwrap(),
    );
    let mut session =
        CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 16_384, 32)
            .await
            .unwrap();
    let key = ui("config", "owned.json").unwrap();
    let file = files::FileValue::new(key.clone(), body);
    let original = file.bytes.clone();
    EXPECTED.with(|slot| slot.set(Some((original.as_ptr() as usize, original.len()))));
    OBSERVATIONS.with(|count| count.set(0));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            EXPECTED.with(|slot| slot.set(None));
        }
    }
    let _reset = Reset;
    session
        .publish_rebased::<false, true>(file, false)
        .await
        .unwrap();
    assert_eq!(OBSERVATIONS.with(Cell::get), 1);
    assert_eq!(session.files[&key].bytes, original);
    let reopened = files::TaskVolume::open(provider, path).await.unwrap();
    assert_eq!(
        reopened
            .snapshot(&files::Query::File(key), 1)
            .await
            .unwrap()
            .files[0]
            .bytes,
        original
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn publication_moves_the_original_payload_allocation(body in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..4096)) {
        futures::executor::block_on(publish_owned(body));
    }
}

#[test]
fn copying_the_publication_payload_is_detected_negative_control() {
    COPY_PAYLOAD.with(|flag| flag.set(true));
    let caught =
        std::panic::catch_unwind(|| futures::executor::block_on(publish_owned(vec![7; 128])));
    COPY_PAYLOAD.with(|flag| flag.set(false));
    let panic = caught.expect_err("copying the production payload must fail the allocation oracle");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("publication payload allocation changed"),
        "{message}"
    );
}
