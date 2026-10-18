use super::*;
use crate::task_volume::{Generation, Query, SCHEMA, query_id};
use proptest::prelude::*;

fn snapshot(payloads: Vec<Vec<u8>>) -> HeadSnapshot {
    let generation = Generation {
        volume: [1; 32],
        epoch: Default::default(),
        position: payloads.len() as u64 + 2,
        commit: [2; 32],
    };
    HeadSnapshot {
        files: payloads
            .into_iter()
            .enumerate()
            .map(|(ordinal, bytes)| HeadFile {
                key: FileKey::Task {
                    task: Component::new(format!("task{}", ordinal % 2)).unwrap(),
                    file: TaskFile::Chunk {
                        execution: crate::OperationId::from_bytes([ordinal as u8; 16]),
                        turn: 0,
                        sequence: ordinal as u64,
                    },
                },
                publication: None,
                channel: Some(Channel::Text),
                length: bytes.len() as u64,
                segments: vec![bytes.into()],
                through_sequence: None,
            })
            .collect(),
        cursor: Cursor {
            volume: generation.volume,
            epoch: generation.epoch,
            schema: SCHEMA,
            query: query_id(&Query::Root).unwrap(),
            position: generation.position,
            accepted: generation.position,
            durable: generation.position,
            commit: generation.commit,
        },
        generation,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn snapshot_and_follow_indexes_have_equal_observable_order(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 1..16),
    ) {
        let reopened = FileViews::ordinary(snapshot(payloads), QueryViewLimits {
            files: 32, segments: 32, bytes: 1024,
        }).unwrap();
        let mut followed = reopened.clone();
        followed.task_streams.clear();
        for (ordinal, key) in followed.stream_order.iter().enumerate() {
            let position = (ordinal as u64 + 2, 0);
            *followed.stream_positions.get_mut(key).unwrap() = position;
            let FileKey::Task { task, .. } = key else { unreachable!() };
            followed.task_streams.insert((task.clone(), position.0, position.1), key.clone());
            let changed = followed.stream_updates[key];
            followed.task_updates.get_mut(&(task.clone(), changed.0, changed.1)).unwrap().1 = position;
        }
        // Restoring derived equality rejects two equivalent production layouts.
        prop_assert_ne!(&followed.stream_positions, &reopened.stream_positions);
        prop_assert_eq!(&followed, &reopened);
        for task in ["task0", "task1"] {
            let task = Component::new(task).unwrap();
            let keys = |view: &FileViews| view.text_files(&task, None).map(|file| file.key.clone()).collect::<Vec<_>>();
            prop_assert_eq!(keys(&followed), keys(&reopened));
        }
        // Ignoring derived indexes entirely would miss a broken resume anchor.
        followed.stream_positions.values_mut().next().unwrap().0 += 1;
        prop_assert_ne!(followed, reopened);
    }
}
