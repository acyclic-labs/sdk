use super::*;
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};
use std::sync::Arc;

// The actual MemoryStream and TaskVolume use the cfg-selected synchronization
// shims. No replacement volume, snapshot, CAS or model-only file map is used.
fn capture_and_publish<const ATOMIC: bool, const FENCE: bool>(racing_capture: bool) {
    loom::model(move || {
        let provider = Arc::new(MemoryStream::default());
        let path = StreamPath::new("summary-selection").unwrap();
        let writer = Arc::new(block_on(TaskVolume::open(provider.clone(), path.clone())).unwrap());
        let task = TaskId::from_bytes([1; 16]);
        let head = commands::compact_key(task).unwrap();
        let query = Query::Turns(Component::new(task.to_string()).unwrap());
        let initial_cut = block_on(writer.cut(&query)).unwrap();
        let a_key = checkpoint_key(task, 1, OperationId::from_bytes([1; 16])).unwrap();
        let b_key = checkpoint_key(task, 2, OperationId::from_bytes([2; 16])).unwrap();
        let initial = SummarySelection {
            continuation: None,
            checkpoint: a_key.clone(),
            selected_at: 0,
            through: None,
        };
        let generation = block_on(writer.head()).unwrap();
        let generation = block_on(writer.append(
            generation,
            &FileValue::new(head.clone(), serde_json::to_vec(&initial).unwrap()),
        ))
        .unwrap();
        let rewind = SummarySelection {
            through: Some(initial_cut.clone()),
            ..initial.clone()
        };
        // Independent real clients share the stream, not disposable caches.
        // This bounds the topology without bounding schedules or permutations.
        let volume = Arc::new(block_on(TaskVolume::open_existing(provider, path)).unwrap());
        let publish_rewind = {
            let volume = writer.clone();
            let head = head.clone();
            let rewind = rewind.clone();
            move || {
                block_on(volume.append(
                    generation,
                    &FileValue::new(head, serde_json::to_vec(&rewind).unwrap()),
                ))
                .unwrap()
            }
        };
        // The sequential case isolates equality of the complete selection:
        // even an unchanged checkpoint and old user-supplied fence differ.
        let mut writer = None;
        let before = if racing_capture {
            writer = Some(loom::thread::spawn(publish_rewind));
            None
        } else {
            let captured = block_on(capture_selection(&volume, task, 4096)).unwrap();
            publish_rewind();
            Some(captured)
        };
        let (mut cut, selected, identity) =
            before.unwrap_or_else(|| block_on(capture_selection(&volume, task, 4096)).unwrap());
        if !ATOMIC {
            // Former production route: selection, then an independent live cut.
            cut.cursor = block_on(volume.cut(&query)).unwrap();
        }
        if let Some(writer) = writer {
            writer.join().unwrap();
        }
        if racing_capture {
            let observed_rewind = cut.cursor.position > generation.position;
            assert_eq!(
                selected.as_ref() == Some(&rewind),
                observed_rewind,
                "capture mixed selection and history generations"
            );
        }
        let mut checkpoint = crate::context::summary_source::tests::checkpoint(initial_cut);
        checkpoint.boundary.captured_at = cut.cursor.position;
        checkpoint.boundary.previous = identity;
        if let Some(frozen) = selected.as_ref().and_then(|value| value.through.as_ref()) {
            checkpoint.boundary.through = frozen.clone();
            checkpoint.boundary.frozen = true;
        }
        let accepted =
            block_on(select::<_, FENCE>(&volume, task, &checkpoint, &b_key, 4096)).unwrap();
        let (_, current, _) = block_on(capture_selection(&volume, task, 4096)).unwrap();
        let expected = selected.as_ref() == Some(&rewind);
        assert_eq!(
            accepted, expected,
            "publication ignored the complete selection identity"
        );
        if !expected {
            assert_eq!(current, Some(rewind));
        }
    });
}
#[test]
fn capture_and_publication_share_the_real_volume_observation() {
    capture_and_publish::<true, true>(true);
}
#[test]
fn publication_checks_the_complete_selection_identity() {
    capture_and_publish::<true, true>(false);
}
#[test]
#[should_panic(expected = "capture mixed selection and history generations")]
fn negative_control_detects_separate_selection_and_history_reads() {
    capture_and_publish::<false, true>(true);
}
#[test]
#[should_panic(expected = "publication ignored the complete selection identity")]
fn negative_control_detects_an_omitted_selection_fence() {
    capture_and_publish::<true, false>(false);
}
