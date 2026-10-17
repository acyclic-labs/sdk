use super::*;
use crate::task_volume::{ChildTurnResult, FileQuerySource, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

async fn volume(prompt: usize, answer: usize) -> TaskVolume<MemoryStream> {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("listing-property").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::from_bytes([1; 16]);
    let reference = Component::new("worker").unwrap();
    let status = ChildStatus {
        task,
        state: ChildState::Running,
        inputs: 1,
        settled: true,
    };
    let result = ChildTurnResult {
        task,
        turn: 1,
        outcome: crate::Outcome::Failed {
            message: "a".repeat(answer),
        },
    };
    for (file, bytes) in [
        (ChildFile::Request, vec![b'p'; prompt]),
        (ChildFile::Status, serde_json::to_vec(&status).unwrap()),
        (ChildFile::Result(1), serde_json::to_vec(&result).unwrap()),
    ] {
        let file = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file,
            },
            bytes,
        );
        volume
            .append(volume.head().await.unwrap(), &file)
            .await
            .unwrap();
    }
    volume
}

fn unchanged_listing(result: Result<String>, expected: &str, check: bool) {
    if check {
        assert_eq!(
            result.unwrap(),
            expected,
            "file bodies changed a fitting listing"
        );
        assert!(expected.len() <= 4096);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn growing_bodies_cannot_break_a_fitting_listing(
        prompt in 1usize..8192, answer in 0usize..8192,
    ) {
        futures::executor::block_on(async {
            let small = volume(1, 0).await;
            let grown = volume(prompt, answer).await;
            for query in [Query::Children, Query::Child(Component::new("worker").unwrap())] {
                let expected = listing(&small, &query, 3).await.unwrap();
                unchanged_listing(listing(&grown, &query, 3).await, &expected, true);
                prop_assert!(grown.list_files(&query, 2).await.is_err(), "file count bound changed");
            }
            Ok(())
        })?;
    }
}

#[test]
fn body_bounded_listing_negative_control_requires_its_property_assertion() {
    futures::executor::block_on(async {
        let files = volume(3072, 3072).await;
        for query in [
            Query::Children,
            Query::Child(Component::new("worker").unwrap()),
        ] {
            let expected = listing(&files, &query, 3).await.unwrap();
            unchanged_listing(Ok(expected.clone()), &expected, true);
            // The old listing first projected every body through render_bytes.
            let old = || async {
                files
                    .observe_files(query.clone(), None, 3, 4096)
                    .await
                    .map(|observation| {
                        observation
                            .snapshot
                            .unwrap()
                            .files
                            .iter()
                            .map(|file| format!(".volumes/{}\n", file.key.path()))
                            .collect()
                    })
                    .map_err(storage)
            };
            let result = old().await;
            assert!(
                result.is_err(),
                "negative control no longer exceeds the body bound"
            );
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unchanged_listing(result, &expected, true)
            }))
            .expect_err("body-dependent listing escaped the invariant");
            unchanged_listing(old().await, &expected, false);
        }
    });
}
